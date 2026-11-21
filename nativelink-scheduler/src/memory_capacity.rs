// Copyright 2026 The NativeLink Authors. All rights reserved.
//
// Licensed under the Functional Source License, Version 1.1, Apache 2.0 Future License (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    See LICENSE file for details
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Correlated physical capacity observation and controller-owned memory fitting.

use std::fmt::Write;
use std::sync::Arc;

use nativelink_config::schedulers::{PropertyType, ResourceCapacity, ResourceEscalationSpec};
use nativelink_error::{Code, Error, make_err};
use nativelink_util::action_messages::{ActionInfo, MemoryReservation};
use nativelink_util::operation_state_manager::QueuedMemoryFit;
use nativelink_util::platform_properties::{PlatformProperties, PlatformPropertyValue};
use tokio::sync::watch;

use crate::platform_property_manager::PlatformPropertyManager;
use crate::worker_scheduler::ResourceCapacitySnapshot;

#[derive(Clone, Copy)]
pub(crate) enum CapacityProperty<'a> {
    Minimum(u64),
    Exact(&'a str),
    Other,
}

impl<'a> From<&'a PlatformPropertyValue> for CapacityProperty<'a> {
    fn from(value: &'a PlatformPropertyValue) -> Self {
        match value {
            PlatformPropertyValue::Minimum(value) => Self::Minimum(*value),
            PlatformPropertyValue::Exact(value) => Self::Exact(value),
            _ => Self::Other,
        }
    }
}

/// Shared selection for fitting and resource-kill retries. The tuple keeps the
/// effective ceiling, physical capacity and CPU from the same policy row.
pub(crate) fn eligible_capacity<'a, I>(
    policy: &ResourceEscalationSpec,
    rows: &[ResourceCapacity],
    properties: I,
    property: &str,
    reservation: Option<&MemoryReservation>,
) -> Result<Option<(u64, u64, u64)>, Error>
where
    I: Iterator<Item = Result<(&'a str, CapacityProperty<'a>), Error>> + Clone,
{
    let mut largest = None;
    for capacity in rows {
        let effective = |name: &str| {
            capacity.minimum.get(name).map(|physical| {
                (*physical).min(
                    capacity
                        .action_ceilings
                        .get(name)
                        .copied()
                        .unwrap_or(*physical),
                )
            })
        };
        // Original explicit intent is tested before selecting a row. Learned or
        // previously fitted memory must never exclude a legitimate future fleet.
        if reservation.and_then(|r| r.minimum_kb).is_some_and(|floor| {
            effective(&policy.memory.property).is_none_or(|ceiling| floor > ceiling)
        }) {
            continue;
        }
        let mut matches = true;
        for requested in properties.clone() {
            let (name, value) = requested?;
            if policy
                .capacity_ignored_properties
                .iter()
                .any(|ignored| ignored == name)
            {
                continue;
            }
            let value = if name == policy.cpu_property {
                if let Some(reservation) = reservation {
                    reservation
                        .cpu_before_memory_fit
                        .map_or(CapacityProperty::Other, CapacityProperty::Minimum)
                } else {
                    value
                }
            } else {
                value
            };
            matches = match value {
                CapacityProperty::Minimum(_) if name == property => true,
                CapacityProperty::Minimum(_)
                    if reservation.is_some() && name == policy.memory.property =>
                {
                    true
                }
                CapacityProperty::Minimum(needed) => {
                    effective(name).is_some_and(|available| needed <= available)
                }
                CapacityProperty::Exact(needed) => capacity
                    .exact
                    .get(name)
                    .is_some_and(|available| available == needed),
                CapacityProperty::Other => true,
            };
            if !matches {
                break;
            }
        }
        // A CPU baseline may be absent from today's property map (or restored
        // after a fit); its original minimum still participates in eligibility.
        if !matches
            || reservation
                .and_then(|r| r.cpu_before_memory_fit)
                .is_some_and(|cpu| {
                    effective(&policy.cpu_property).is_none_or(|available| cpu > available)
                })
        {
            continue;
        }
        let (Some(ceiling), Some(physical), Some(cpu)) = (
            effective(property),
            capacity.minimum.get(property),
            effective(&policy.cpu_property),
        ) else {
            continue;
        };
        let candidate = (ceiling, *physical, cpu);
        largest = Some(largest.map_or(candidate, |previous| candidate.max(previous)));
    }
    Ok(largest)
}

pub(crate) fn fit_memory(
    info: &ActionInfo,
    policy: &ResourceEscalationSpec,
    snapshot: &ResourceCapacitySnapshot,
    manager: &PlatformPropertyManager,
) -> Result<QueuedMemoryFit, Error> {
    let reservation = info
        .memory_reservation
        .as_ref()
        .ok_or_else(|| make_err!(Code::Internal, "Memory fitting requires reservation intent"))?;
    let properties = info.platform_properties.iter().map(|(name, value)| {
        let value = match manager.get_known_properties().get(name) {
            Some(PropertyType::Minimum) => {
                CapacityProperty::from_minimum(manager.make_prop_value(name, value)?)
            }
            Some(PropertyType::Exact) => CapacityProperty::Exact(value),
            _ => CapacityProperty::Other,
        };
        Ok((name.as_str(), value))
    });
    let Some((ceiling, physical, cpu)) = eligible_capacity(
        policy,
        &snapshot.capacities,
        properties,
        &policy.memory.property,
        Some(reservation),
    )?
    else {
        return Err(make_err!(
            Code::FailedPrecondition,
            "No eligible physical capacity satisfies original memory minimum {:?} KiB and original CPU/other platform requirements",
            reservation.minimum_kb
        ));
    };
    let memory_kb = reservation
        .desired_kb
        .or(reservation.minimum_kb)
        .map(|desired| {
            desired
                .max(reservation.minimum_kb.unwrap_or(0))
                .min(ceiling)
        });
    Ok(QueuedMemoryFit {
        memory_kb,
        cpu: if memory_kb == Some(physical) {
            Some(cpu)
        } else {
            reservation.cpu_before_memory_fit
        },
        revision: snapshot.revision,
    })
}

impl CapacityProperty<'_> {
    fn from_minimum(value: PlatformPropertyValue) -> Self {
        match value {
            PlatformPropertyValue::Minimum(value) => Self::Minimum(value),
            _ => Self::Other,
        }
    }
}

/// Apply only the fitted values and revision, retaining original intent. Existing
/// string buffers are reused so queued refits do not allocate replacement values.
pub(crate) fn apply_memory_fit(
    info: &mut ActionInfo,
    memory_property: &str,
    cpu_property: &str,
    fit: QueuedMemoryFit,
) {
    for (property, value) in [(memory_property, fit.memory_kb), (cpu_property, fit.cpu)] {
        if let Some(value) = value {
            if let Some(buffer) = info.platform_properties.get_mut(property) {
                buffer.clear();
                // Formatting into a String cannot fail.
                let _ = write!(buffer, "{value}");
            } else {
                info.platform_properties
                    .insert(property.to_owned(), value.to_string());
            }
        } else {
            info.platform_properties.remove(property);
        }
    }
    if let Some(reservation) = &mut info.memory_reservation {
        reservation.fitted_revision = Some(fit.revision);
    }
}

/// Allocate and publish only if at least one physical memory minimum decreases.
/// The sender owns retained knowledge independently of worker/census liveness.
pub(crate) fn observe_capacities(
    sender: &watch::Sender<Arc<ResourceCapacitySnapshot>>,
    memory_property: &str,
    mut observed: impl FnMut(&ResourceCapacity) -> Option<u64>,
) -> bool {
    let current = sender.borrow();
    let mut lowered: Option<Vec<ResourceCapacity>> = None;
    for (index, row) in current.capacities.iter().enumerate() {
        let Some(previous) = row.minimum.get(memory_property) else {
            continue;
        };
        let Some(memory) = observed(row).filter(|memory| memory < previous) else {
            continue;
        };
        let rows = lowered.get_or_insert_with(|| current.capacities.to_vec());
        if let Some(value) = rows[index].minimum.get_mut(memory_property) {
            *value = memory;
        }
    }
    drop(current);
    if let Some(rows) = lowered {
        sender.send_replace(Arc::new(ResourceCapacitySnapshot::new(rows)));
        true
    } else {
        false
    }
}

pub(crate) fn worker_memory(
    row: &ResourceCapacity,
    memory_property: &str,
    totals: &PlatformProperties,
) -> Option<u64> {
    if !row.exact.iter().all(|(name, expected)| {
        matches!(totals.properties.get(name), Some(PlatformPropertyValue::Exact(actual)) if actual == expected)
    }) || !row.minimum.iter().all(|(name, expected)| {
        name == memory_property || matches!(totals.properties.get(name), Some(PlatformPropertyValue::Minimum(actual)) if actual >= expected)
    }) {
        return None;
    }
    match totals.properties.get(memory_property) {
        Some(PlatformPropertyValue::Minimum(memory)) => Some(*memory),
        _ => None,
    }
}

pub(crate) fn peer_memory(
    row: &ResourceCapacity,
    memory_property: &str,
    peer: &ResourceCapacity,
) -> Option<u64> {
    (row.exact == peer.exact
        && row.minimum.iter().all(|(name, expected)| {
            name == memory_property
                || peer
                    .minimum
                    .get(name)
                    .is_some_and(|actual| actual >= expected)
        }))
    .then(|| peer.minimum.get(memory_property).copied())
    .flatten()
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::time::{Duration, UNIX_EPOCH};

    use nativelink_util::action_messages::{ActionUniqueKey, ActionUniqueQualifier};
    use nativelink_util::common::DigestInfo;
    use nativelink_util::digest_hasher::DigestHasherFunc;
    use serde_json::json;

    use super::*;

    const ESTIMATE: u64 = 63 * 1024 * 1024;
    const ACTUAL: u64 = 62_229 * 1024;

    fn policy() -> ResourceEscalationSpec {
        // A nominal 64 GiB fleet starts with its already-reserved 63 GiB
        // estimate. Observations must not subtract the reserve a second time.
        serde_json::from_value(json!({
            "memory": { "property": "memory", "percent": 200, "max_steps": 2 },
            "headroom_percent": 20,
            "cpu_property": "cpu",
            "capacities": [{
                "minimum": { "memory": ESTIMATE, "cpu": 8, "disk": 1024 },
                "exact": { "fleet": "compile" }
            }]
        }))
        .expect("valid resource policy")
    }

    fn manager() -> PlatformPropertyManager {
        PlatformPropertyManager::new(HashMap::from([
            ("memory".to_owned(), PropertyType::Minimum),
            ("cpu".to_owned(), PropertyType::Minimum),
            ("disk".to_owned(), PropertyType::Minimum),
            ("fleet".to_owned(), PropertyType::Exact),
        ]))
    }

    fn action(minimum: Option<u64>, desired: Option<u64>, cpu: Option<u64>) -> ActionInfo {
        let digest = DigestInfo::new([42; 32], 0);
        let mut platform_properties = HashMap::from([
            ("fleet".to_owned(), "compile".to_owned()),
            ("disk".to_owned(), "1".to_owned()),
        ]);
        if let Some(memory) = desired {
            platform_properties.insert("memory".to_owned(), memory.to_string());
        }
        if let Some(cpu) = cpu {
            platform_properties.insert("cpu".to_owned(), cpu.to_string());
        }
        ActionInfo {
            command_digest: digest,
            input_root_digest: digest,
            timeout: Duration::from_secs(60),
            platform_properties,
            memory_reservation: Some(MemoryReservation {
                minimum_kb: minimum,
                desired_kb: desired,
                cpu_before_memory_fit: cpu,
                fitted_revision: None,
            }),
            priority: 0,
            load_timestamp: UNIX_EPOCH,
            insert_timestamp: UNIX_EPOCH,
            unique_qualifier: ActionUniqueQualifier::Cacheable(ActionUniqueKey {
                execution_scope: None,
                instance_name: String::new(),
                digest_function: DigestHasherFunc::Sha256,
                digest,
            }),
        }
    }

    fn measured(policy: &ResourceEscalationSpec) -> ResourceCapacitySnapshot {
        let mut rows = policy.capacities.clone();
        rows[0].minimum.insert("memory".to_owned(), ACTUAL);
        ResourceCapacitySnapshot::new(rows)
    }

    #[test]
    fn learned_memory_uses_measured_whole_pool_and_preserves_intent() {
        let policy = policy();
        let snapshot = measured(&policy);
        let mut info = action(None, Some(ESTIMATE), Some(1));
        let original = info.memory_reservation.clone().expect("intent");
        let fit = fit_memory(&info, &policy, &snapshot, &manager()).expect("eligible fleet");
        assert_eq!(fit.memory_kb, Some(ACTUAL));
        assert_eq!(fit.cpu, Some(8));
        apply_memory_fit(&mut info, "memory", "cpu", fit);
        assert_eq!(info.platform_properties["memory"], ACTUAL.to_string());
        assert_eq!(info.platform_properties["cpu"], "8");
        let intent = info.memory_reservation.as_ref().expect("intent retained");
        assert_eq!(intent.minimum_kb, original.minimum_kb);
        assert_eq!(intent.desired_kb, original.desired_kb);
        assert_eq!(intent.cpu_before_memory_fit, original.cpu_before_memory_fit);
        assert_eq!(intent.fitted_revision, Some(snapshot.revision));
    }

    #[test]
    fn explicit_floor_is_never_clamped_and_larger_future_fleet_remains_eligible() {
        let mut policy = policy();
        let mut snapshot = measured(&policy);
        let info = action(Some(ACTUAL + 1), Some(ESTIMATE), Some(1));
        let error = fit_memory(&info, &policy, &snapshot, &manager()).expect_err("explicit floor");
        assert_eq!(error.code, Code::FailedPrecondition);
        let mut larger = policy.capacities[0].clone();
        larger.minimum.insert("memory".to_owned(), 2 * ESTIMATE);
        policy.capacities.push(larger.clone());
        let mut rows = snapshot.capacities.to_vec();
        rows.push(larger);
        snapshot = ResourceCapacitySnapshot::new(rows);
        let fit = fit_memory(&info, &policy, &snapshot, &manager()).expect("future fleet");
        assert_eq!(fit.memory_kb, Some(ESTIMATE));
        assert_eq!(fit.cpu, Some(1));
        let floor_only = action(Some(ACTUAL), None, Some(1));
        let fit = fit_memory(&floor_only, &policy, &snapshot, &manager())
            .expect("explicit floor without learning");
        assert_eq!(fit.memory_kb, Some(ACTUAL));
    }

    #[test]
    fn equality_promotes_cpu_but_action_ceiling_restores_original_cpu_or_absence() {
        let mut policy = policy();
        let mut snapshot = measured(&policy);
        let mut info = action(None, Some(ACTUAL), Some(1));
        let fit = fit_memory(&info, &policy, &snapshot, &manager()).expect("equality");
        assert_eq!(fit.cpu, Some(8));
        apply_memory_fit(&mut info, "memory", "cpu", fit);
        policy.capacities[0]
            .action_ceilings
            .insert("memory".to_owned(), ACTUAL - 1);
        let mut rows = snapshot.capacities.to_vec();
        rows[0].action_ceilings = policy.capacities[0].action_ceilings.clone();
        // A previously promoted CPU must not exclude a row with a lower CPU
        // ceiling when the original request can run there.
        rows[0].action_ceilings.insert("cpu".to_owned(), 4);
        snapshot = ResourceCapacitySnapshot::new(rows);
        let fit = fit_memory(&info, &policy, &snapshot, &manager()).expect("restore CPU");
        assert_eq!(fit.memory_kb, Some(ACTUAL - 1));
        assert_eq!(fit.cpu, Some(1));
        info.memory_reservation
            .as_mut()
            .expect("intent")
            .cpu_before_memory_fit = None;
        let fit = fit_memory(&info, &policy, &snapshot, &manager()).expect("restore absence");
        assert_eq!(fit.cpu, None);
        apply_memory_fit(&mut info, "memory", "cpu", fit);
        assert!(!info.platform_properties.contains_key("cpu"));
    }

    #[test]
    fn absent_and_zero_desired_memory_stay_distinct() {
        let policy = policy();
        let snapshot = measured(&policy);
        for desired in [None, Some(0)] {
            let mut info = action(None, desired, None);
            let fit = fit_memory(&info, &policy, &snapshot, &manager()).expect("eligible");
            assert_eq!(fit.memory_kb, desired);
            assert_eq!(fit.cpu, None);
            apply_memory_fit(&mut info, "memory", "cpu", fit);
            assert_eq!(
                info.platform_properties.get("memory"),
                desired.map(|v| v.to_string()).as_ref()
            );
        }
    }

    #[test]
    fn conservative_observations_are_correlated_retained_and_publish_only_changes() {
        let policy = policy();
        let (sender, receiver) = watch::channel(Arc::new(ResourceCapacitySnapshot::new(
            policy.capacities.clone(),
        )));
        let worker = |fleet: &str, memory: u64, cpu: u64| {
            PlatformProperties::new(HashMap::from([
                (
                    "fleet".to_owned(),
                    PlatformPropertyValue::Exact(fleet.to_owned()),
                ),
                ("memory".to_owned(), PlatformPropertyValue::Minimum(memory)),
                ("cpu".to_owned(), PlatformPropertyValue::Minimum(cpu)),
                ("disk".to_owned(), PlatformPropertyValue::Minimum(1024)),
            ]))
        };
        let original = receiver.borrow().clone();
        assert!(!observe_capacities(&sender, "memory", |row| {
            worker_memory(row, "memory", &worker("other", ACTUAL, 8))
        }));
        assert!(!observe_capacities(&sender, "memory", |row| {
            worker_memory(row, "memory", &worker("compile", ACTUAL, 7))
        }));
        assert!(Arc::ptr_eq(&original, &receiver.borrow()));
        assert!(observe_capacities(&sender, "memory", |row| {
            worker_memory(row, "memory", &worker("compile", ACTUAL, 8))
        }));
        let learned = receiver.borrow().clone();
        assert_ne!(learned.revision, original.revision);
        assert_eq!(learned.capacities[0].minimum["memory"], ACTUAL);
        // No workers, a same-fleet reconnect, or a higher observation all keep
        // retained knowledge and the identical immutable snapshot allocation.
        for observed in [None, Some(ACTUAL), Some(ESTIMATE)] {
            assert!(!observe_capacities(&sender, "memory", |_| observed));
            assert!(Arc::ptr_eq(&learned, &receiver.borrow()));
            assert_eq!(learned.revision, receiver.borrow().revision);
        }
        let mut peer = policy.capacities[0].clone();
        peer.minimum.insert("memory".to_owned(), ACTUAL - 1);
        peer.action_ceilings.insert("memory".to_owned(), 1);
        assert!(observe_capacities(&sender, "memory", |row| {
            peer_memory(row, "memory", &peer)
        }));
        assert_eq!(
            receiver.borrow().capacities[0].minimum["memory"],
            ACTUAL - 1
        );
        assert!(receiver.borrow().capacities[0].action_ceilings.is_empty());
        assert_eq!(receiver.borrow().capacities[0].minimum["cpu"], 8);
        assert_eq!(receiver.borrow().capacities[0].minimum["disk"], 1024);
    }
}
