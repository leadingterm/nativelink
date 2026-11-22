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
    properties: &I,
    property: &str,
    reservation: Option<&MemoryReservation>,
) -> Result<Option<(u64, u64, u64)>, Error>
where
    I: Iterator<Item = Result<(&'a str, CapacityProperty<'a>), Error>> + Clone,
{
    let mut largest = None;
    for capacity in rows {
        let effective = |name: &str| capacity.effective_capacity(name);
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
            capacity.physical_capacity(property),
            effective(&policy.cpu_property),
        ) else {
            continue;
        };
        let candidate = (ceiling, physical, cpu);
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
                CapacityProperty::from_minimum(&manager.make_prop_value(name, value)?)
            }
            Some(PropertyType::Exact) => CapacityProperty::Exact(value),
            _ => CapacityProperty::Other,
        };
        Ok((name.as_str(), value))
    });
    let Some((ceiling, physical, cpu)) = eligible_capacity(
        policy,
        &snapshot.capacities,
        &properties,
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
    const fn from_minimum(value: &PlatformPropertyValue) -> Self {
        match value {
            PlatformPropertyValue::Minimum(value) => Self::Minimum(*value),
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

/// Recompute each dimension from current facts, falling back to its baseline
/// estimate when unobserved. Unchanged values share storage.
/// Both slices retain the immutable policy's row ordering and non-minimum fields.
pub(crate) fn observe_capacities(
    baseline: &[ResourceCapacity],
    current: &[ResourceCapacity],
    mut observed: impl FnMut(&ResourceCapacity, &str) -> Option<u64>,
) -> Option<Arc<[ResourceCapacity]>> {
    let mut updated: Option<Vec<ResourceCapacity>> = None;
    for (index, row) in baseline.iter().enumerate() {
        for (property, &estimate) in &row.minimum {
            let capacity = observed(row, property).unwrap_or(estimate);
            if current[index].minimum.get(property) == Some(&capacity) {
                continue;
            }
            let rows = updated.get_or_insert_with(|| current.to_vec());
            rows[index].minimum.insert(property.clone(), capacity);
        }
    }
    updated.map(Arc::from)
}

/// Actual-only rows retain policy metadata but never substitute unknown facts
/// with prelaunch estimates. Called on fleet changes, not beacon publication.
pub(crate) fn actual_capacities(
    baseline: &[ResourceCapacity],
    mut observed: impl FnMut(&ResourceCapacity, &str) -> Option<u64>,
) -> Arc<[ResourceCapacity]> {
    baseline
        .iter()
        .filter_map(|row| {
            let minimum = row
                .minimum
                .keys()
                .filter_map(|property| {
                    observed(row, property).map(|capacity| (property.clone(), capacity))
                })
                .collect::<std::collections::BTreeMap<_, _>>();
            (!minimum.is_empty()).then(|| ResourceCapacity {
                minimum,
                exact: row.exact.clone(),
                action_ceilings: row.action_ceilings.clone(),
                pool_multipliers: row.pool_multipliers.clone(),
                physical_sources: row.physical_sources.clone(),
            })
        })
        .collect()
}

/// Old readers select entire rows using non-memory prelaunch minima. Preserve
/// that context without advertising estimates as actuals in the new protocol.
pub(crate) fn legacy_capacities<'a>(
    policy: &ResourceEscalationSpec,
    actual: &[ResourceCapacity],
    estimates: impl Iterator<Item = &'a [ResourceCapacity]>,
) -> Arc<[ResourceCapacity]> {
    let mut rows = policy.capacities.clone();
    for row in &mut rows {
        let observed = actual.iter().find(|actual| {
            actual.exact == row.exact
                && actual.pool_multipliers == row.pool_multipliers
                && actual.physical_sources == row.physical_sources
                && actual.action_ceilings == row.action_ceilings
        });
        // Raising v1 eligibility context must not raise an action limit. For
        // unknown dimensions preserve the original local physical estimate.
        for property in row.minimum.keys() {
            if let Some(ceiling) = observed
                .and_then(|actual| actual.effective_capacity(property))
                .or_else(|| row.effective_capacity(property))
            {
                row.action_ceilings.insert(property.clone(), ceiling);
            }
        }
        if let Some(memory) =
            observed.and_then(|actual| actual.minimum.get(&policy.memory.property))
        {
            row.minimum.insert(policy.memory.property.clone(), *memory);
        }
    }
    for peer in estimates {
        for row in &mut rows {
            for estimate in peer.iter().filter(|peer| peer.exact == row.exact) {
                for (property, minimum) in &mut row.minimum {
                    if property != &policy.memory.property
                        && let Some(remote) = estimate.minimum.get(property)
                    {
                        *minimum = (*minimum).max(*remote);
                    }
                }
            }
        }
    }
    rows.into()
}

pub(crate) fn worker_capacity(
    row: &ResourceCapacity,
    property: &str,
    totals: &PlatformProperties,
) -> Option<u64> {
    if !row.exact.iter().all(|(name, expected)| {
        matches!(totals.properties.get(name), Some(PlatformPropertyValue::Exact(actual)) if actual == expected)
    }) {
        return None;
    }
    if let Some(source) = row.physical_sources.get(property) {
        let physical = match totals.properties.get(&source.property)? {
            PlatformPropertyValue::Minimum(value) => *value,
            PlatformPropertyValue::Exact(value)
            | PlatformPropertyValue::Priority(value)
            | PlatformPropertyValue::Ignore(value)
            | PlatformPropertyValue::Unknown(value) => value.parse::<u64>().ok()?,
        };
        return physical.checked_mul(source.scale.get())?.checked_mul(
            row.pool_multipliers
                .get(property)
                .map_or(1, |factor| factor.get()),
        );
    }
    match totals.properties.get(property) {
        Some(PlatformPropertyValue::Minimum(capacity)) => Some(*capacity),
        _ => None,
    }
}

pub(crate) fn peer_capacity(
    row: &ResourceCapacity,
    property: &str,
    peer: &ResourceCapacity,
) -> Option<u64> {
    if row.exact != peer.exact {
        return None;
    }
    peer.physical_capacity(property)?.checked_mul(
        row.pool_multipliers
            .get(property)
            .map_or(1, |factor| factor.get()),
    )
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

    #[test]
    fn physical_and_effective_capacities_share_ledger_conversion() {
        let row: ResourceCapacity = serde_json::from_value(json!({
            "minimum": { "cpu": 96000, "memory": 64000, "disk": 100_000 },
            "pool_multipliers": { "cpu": 3 },
            "action_ceilings": { "cpu": 16000, "memory": 32000, "disk": 200_000, "missing": 1 }
        }))
        .expect("valid row");
        for (property, physical, effective) in [
            ("cpu", Some(32_000), Some(16_000)),
            ("memory", Some(64_000), Some(32_000)),
            ("disk", Some(100_000), Some(100_000)),
            ("missing", None, None),
        ] {
            assert_eq!(row.physical_capacity(property), physical);
            assert_eq!(row.effective_capacity(property), effective);
        }
    }

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
            pre_fit_reservation: None,
            command_digest: digest,
            input_root_digest: digest,
            timeout: Duration::from_mins(1),
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
        let original = info.memory_reservation.expect("intent");
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
    fn observed_oversubscribed_cpu_uses_physical_per_action_capacity() {
        let mut policy = policy();
        policy.capacities[0]
            .minimum
            .insert("cpu".to_owned(), 45_000);
        policy.capacities[0].pool_multipliers.insert(
            "cpu".to_owned(),
            std::num::NonZeroU64::new(3).expect("nonzero multiplier"),
        );
        let mut rows = policy.capacities.clone();
        rows[0].minimum.insert("cpu".to_owned(), 96_000);
        rows[0].minimum.insert("memory".to_owned(), ACTUAL);
        let snapshot = ResourceCapacitySnapshot::new(rows.clone());
        let info = action(None, Some(ACTUAL), Some(32_000));
        let fit = fit_memory(&info, &policy, &snapshot, &manager()).expect("physical CPU fits");
        assert_eq!(fit.cpu, Some(32_000));

        rows[0].action_ceilings.insert("cpu".to_owned(), 16_000);
        let snapshot = ResourceCapacitySnapshot::new(rows);
        let info = action(None, Some(ACTUAL), Some(1));
        let fit = fit_memory(&info, &policy, &snapshot, &manager()).expect("explicit CPU cap fits");
        assert_eq!(fit.cpu, Some(16_000));
        let info = action(None, Some(ACTUAL), Some(32_000));
        assert!(fit_memory(&info, &policy, &snapshot, &manager()).is_err());
    }

    #[test]
    fn whole_pool_comparison_uses_unmultiplied_capacity_not_action_ceiling() {
        let mut policy = policy();
        policy.capacities[0].pool_multipliers.insert(
            "memory".to_owned(),
            std::num::NonZeroU64::new(2).expect("nonzero multiplier"),
        );
        let mut rows = policy.capacities.clone();
        rows[0].minimum.insert("memory".to_owned(), 2 * ACTUAL);
        let info = action(None, Some(ACTUAL), Some(1));
        let snapshot = ResourceCapacitySnapshot::new(rows.clone());
        let fit = fit_memory(&info, &policy, &snapshot, &manager()).expect("whole pool");
        assert_eq!(fit.memory_kb, Some(ACTUAL));
        assert_eq!(fit.cpu, Some(8));

        rows[0]
            .action_ceilings
            .insert("memory".to_owned(), ACTUAL - 1);
        let snapshot = ResourceCapacitySnapshot::new(rows);
        let fit = fit_memory(&info, &policy, &snapshot, &manager()).expect("explicit ceiling");
        assert_eq!(fit.memory_kb, Some(ACTUAL - 1));
        assert_eq!(fit.cpu, Some(1));
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

    fn worker(fleet: &str, memory: u64, cpu: u64, disk: u64) -> PlatformProperties {
        PlatformProperties::new(HashMap::from([
            (
                "fleet".to_owned(),
                PlatformPropertyValue::Exact(fleet.to_owned()),
            ),
            ("memory".to_owned(), PlatformPropertyValue::Minimum(memory)),
            ("cpu".to_owned(), PlatformPropertyValue::Minimum(cpu)),
            ("disk".to_owned(), PlatformPropertyValue::Minimum(disk)),
        ]))
    }

    fn hardware_policy(multiplier: u64) -> ResourceEscalationSpec {
        serde_json::from_value(json!({
            "memory": { "property": "memory", "percent": 200, "max_steps": 2 },
            "headroom_percent": 20,
            "cpu_property": "cpu",
            "capacities": [{
                "minimum": { "memory": ESTIMATE, "cpu": 15_000 * multiplier, "disk": 1024 },
                "exact": { "fleet": "compile" },
                "pool_multipliers": { "cpu": multiplier },
                "physical_sources": { "cpu": { "property": "cpu_count", "scale": 1000 } },
                "action_ceilings": { "cpu": 16_000 }
            }]
        }))
        .expect("hardware policy")
    }

    #[test]
    fn hardware_sources_accept_borrowed_scalar_metadata_and_ignore_sender_ledgers() {
        let policy = hardware_policy(3);
        let row = &policy.capacities[0];
        for scalar in [
            PlatformPropertyValue::Minimum(32),
            PlatformPropertyValue::Ignore("32".to_owned()),
            PlatformPropertyValue::Exact("32".to_owned()),
            PlatformPropertyValue::Priority("32".to_owned()),
            PlatformPropertyValue::Unknown("32".to_owned()),
        ] {
            let mut totals = worker("compile", ACTUAL, 64_000, 512);
            totals.properties.insert("cpu_count".to_owned(), scalar);
            let actual = actual_capacities(&policy.capacities, |row, property| {
                worker_capacity(row, property, &totals)
            });
            assert_eq!(actual[0].minimum["cpu"], 96_000);
            assert_eq!(actual[0].physical_capacity("cpu"), Some(32_000));
            assert_eq!(actual[0].effective_capacity("cpu"), Some(16_000));
            assert_eq!(actual[0].minimum["memory"], ACTUAL);
            assert_eq!(actual[0].minimum["disk"], 512);
            assert_eq!(actual[0].physical_sources, row.physical_sources);
        }
        let mut remote = row.clone();
        remote.minimum.clear();
        remote.minimum.insert("cpu".to_owned(), 64_000);
        remote
            .pool_multipliers
            .insert("cpu".to_owned(), std::num::NonZeroU64::new(2).unwrap());
        assert_eq!(peer_capacity(row, "cpu", &remote), Some(96_000));
        assert_eq!(peer_capacity(row, "memory", &remote), None);
        remote.minimum.insert("cpu".to_owned(), u64::MAX);
        remote.minimum.insert("memory".to_owned(), ACTUAL);
        remote.pool_multipliers.clear();
        assert_eq!(peer_capacity(row, "cpu", &remote), None);
        assert_eq!(peer_capacity(row, "memory", &remote), Some(ACTUAL));
    }

    #[test]
    fn partial_actual_rows_omit_unknown_dimensions_without_discarding_memory() {
        let policy = hardware_policy(3);
        let baseline = &policy.capacities;
        for scalar in [
            None,
            Some(PlatformPropertyValue::Ignore("invalid".to_owned())),
            Some(PlatformPropertyValue::Minimum(u64::MAX)),
            // Scale fits but applying the receiving multiplier would overflow.
            Some(PlatformPropertyValue::Minimum(u64::MAX / 1000)),
        ] {
            let mut totals = worker("compile", ACTUAL, 64_000, 512);
            totals.properties.remove("disk");
            if let Some(scalar) = scalar {
                totals.properties.insert("cpu_count".to_owned(), scalar);
            }
            let actual = actual_capacities(baseline, |row, property| {
                worker_capacity(row, property, &totals)
            });
            assert_eq!(actual.len(), 1);
            assert_eq!(
                actual[0].minimum,
                std::collections::BTreeMap::from([("memory".to_owned(), ACTUAL)])
            );
            assert_eq!(actual[0].action_ceilings, baseline[0].action_ceilings);
            let fitting = observe_capacities(baseline, baseline, |row, property| {
                peer_capacity(row, property, &actual[0])
            })
            .expect("partial memory lowers the fitting row");
            assert_eq!(fitting[0].minimum["memory"], ACTUAL);
            assert_eq!(fitting[0].minimum["cpu"], baseline[0].minimum["cpu"]);
            assert_eq!(fitting[0].minimum["disk"], 1024);
        }
        assert!(actual_capacities(baseline, |_, _| None).is_empty());
        let mut wrong = worker("other", ACTUAL, 64_000, 512);
        wrong.properties.insert(
            "cpu_count".to_owned(),
            PlatformPropertyValue::Ignore("32".to_owned()),
        );
        assert!(
            actual_capacities(baseline, |row, property| {
                worker_capacity(row, property, &wrong)
            })
            .is_empty()
        );
    }

    #[test]
    fn worker_observations_cover_all_dimensions_and_recover() {
        let policy = policy();
        let original: Arc<[ResourceCapacity]> = policy.capacities.clone().into();
        let wrong_fleet = worker("other", ACTUAL, 1, 1);
        assert!(
            observe_capacities(&policy.capacities, &original, |row, property| {
                worker_capacity(row, property, &wrong_fleet)
            })
            .is_none()
        );
        // Every other dimension is below its estimate as well; none is a selector.
        let below = worker("compile", ACTUAL, 7, 512);
        let learned = observe_capacities(&policy.capacities, &original, |row, property| {
            worker_capacity(row, property, &below)
        })
        .expect("actual capacities below the estimates");
        for (property, expected) in [("memory", ACTUAL), ("cpu", 7), ("disk", 512)] {
            assert_eq!(learned[0].minimum[property], expected);
        }
        assert!(
            observe_capacities(&policy.capacities, &learned, |row, property| {
                worker_capacity(row, property, &below)
            })
            .is_none()
        );
        let above = worker("compile", ESTIMATE + 1, 16, 2048);
        let raised = observe_capacities(&policy.capacities, &learned, |row, property| {
            worker_capacity(row, property, &above)
        })
        .expect("current capacity may exceed the estimate");
        for (property, expected) in [("memory", ESTIMATE + 1), ("cpu", 16), ("disk", 2048)] {
            assert_eq!(raised[0].minimum[property], expected);
        }
        let restored = observe_capacities(&policy.capacities, &raised, |_, _| None)
            .expect("removing the last observation restores the estimates");
        assert_eq!(restored, original);
        let reobserved = observe_capacities(&policy.capacities, &restored, |row, property| {
            worker_capacity(row, property, &below)
        })
        .expect("reconnected worker is observed again");
        assert_eq!(reobserved, learned);
    }

    #[test]
    fn missing_worker_and_peer_dimensions_use_their_baseline_estimates() {
        let policy = policy();
        let baseline = &policy.capacities;
        let mut totals = worker("compile", ACTUAL, 7, 512);
        totals.properties.remove("cpu");
        totals.properties.insert(
            "disk".to_owned(),
            PlatformPropertyValue::Exact("not a minimum".to_owned()),
        );
        let observed = observe_capacities(baseline, baseline, |row, property| {
            worker_capacity(row, property, &totals)
        })
        .expect("memory is observed independently");
        assert_eq!(observed[0].minimum["memory"], ACTUAL);
        assert_eq!(observed[0].minimum["cpu"], 8);
        assert_eq!(observed[0].minimum["disk"], 1024);

        let mut peer = baseline[0].clone();
        peer.minimum.remove("cpu");
        peer.minimum.insert("disk".to_owned(), 2048);
        let observed = observe_capacities(baseline, baseline, |row, property| {
            peer_capacity(row, property, &peer)
        })
        .expect("peer disk is observed independently");
        assert_eq!(observed[0].minimum["cpu"], 8);
        assert_eq!(observed[0].minimum["disk"], 2048);
    }

    #[test]
    fn observations_are_minimized_per_dimension_and_exact_row() {
        let mut policy = policy();
        let mut other = policy.capacities[0].clone();
        other.exact.insert("fleet".to_owned(), "other".to_owned());
        policy.capacities.push(other);
        let workers = [
            worker("compile", ACTUAL, 16, 256),
            worker("compile", ESTIMATE + 1, 4, 2048),
        ];
        let local = observe_capacities(&policy.capacities, &policy.capacities, |row, property| {
            workers
                .iter()
                .filter_map(|totals| worker_capacity(row, property, totals))
                .min()
        })
        .expect("compile row changes");
        for (property, expected) in [("memory", ACTUAL), ("cpu", 4), ("disk", 256)] {
            assert_eq!(local[0].minimum[property], expected);
            assert_eq!(
                local[1].minimum[property],
                policy.capacities[1].minimum[property]
            );
        }
        let remaining = observe_capacities(&policy.capacities, &local, |row, property| {
            worker_capacity(row, property, &workers[1])
        })
        .expect("remaining worker supplies its actual capacities");
        for (property, expected) in [("memory", ESTIMATE + 1), ("cpu", 4), ("disk", 2048)] {
            assert_eq!(remaining[0].minimum[property], expected);
        }
    }

    #[test]
    fn peer_local_facts_do_not_become_local_knowledge_or_sticky_history() {
        let policy = policy();
        let local: Arc<[ResourceCapacity]> = policy.capacities.clone().into();
        let mut peer = policy.capacities[0].clone();
        peer.minimum.insert("memory".to_owned(), ACTUAL);
        peer.minimum.insert("cpu".to_owned(), 16);
        peer.minimum.insert("disk".to_owned(), 512);
        peer.action_ceilings.insert("memory".to_owned(), 1);
        let global = observe_capacities(&local, &local, |row, property| {
            peer_capacity(row, property, &peer)
        })
        .expect("peer physical capacities change the global rows");
        for (property, expected) in [("memory", ACTUAL), ("cpu", 16), ("disk", 512)] {
            assert_eq!(global[0].minimum[property], expected);
            assert_eq!(
                local[0].minimum[property],
                policy.capacities[0].minimum[property]
            );
        }
        assert!(global[0].action_ceilings.is_empty());
        assert!(
            observe_capacities(&local, &global, |row, property| {
                peer_capacity(row, property, &peer)
            })
            .is_none()
        );
        let mut different = peer.clone();
        different
            .exact
            .insert("fleet".to_owned(), "other".to_owned());
        assert_eq!(peer_capacity(&local[0], "cpu", &different), None);
        let recovered = observe_capacities(&local, &global, |_, _| None)
            .expect("removed peer facts are no longer observations");
        assert_eq!(recovered, local);
    }

    #[test]
    fn legacy_context_never_inflates_unknown_action_limits_or_actual_dimensions() {
        let mut policy = hardware_policy(1);
        policy.capacities[0]
            .minimum
            .insert("cpu".to_owned(), 32_000);
        policy.capacities[0].action_ceilings.clear();
        let mut totals = worker("compile", ACTUAL, 64_000, 512);
        totals.properties.remove("disk");
        let actual = actual_capacities(&policy.capacities, |row, property| {
            worker_capacity(row, property, &totals)
        });
        assert_eq!(actual[0].minimum.len(), 1);
        let mut legacy = policy.capacities.clone();
        legacy[0].minimum.insert("cpu".to_owned(), 96_000);
        legacy[0].minimum.insert("disk".to_owned(), 2048);
        legacy[0].minimum.insert("memory".to_owned(), 1);
        let mut unrelated = legacy.clone();
        unrelated[0]
            .exact
            .insert("fleet".to_owned(), "other".to_owned());
        unrelated[0].minimum.insert("cpu".to_owned(), u64::MAX);
        let projected = legacy_capacities(
            &policy,
            &actual,
            [legacy.as_slice(), unrelated.as_slice()].into_iter(),
        );
        assert_eq!(projected[0].minimum["cpu"], 96_000);
        assert_eq!(projected[0].minimum["disk"], 2048);
        assert_eq!(projected[0].minimum["memory"], ACTUAL);
        assert_eq!(projected[0].action_ceilings["cpu"], 32_000);
        assert_eq!(projected[0].action_ceilings["disk"], 1024);
        assert_eq!(projected[0].action_ceilings["memory"], ACTUAL);
        assert!(!actual[0].minimum.contains_key("cpu"));
        assert!(!actual[0].minimum.contains_key("disk"));
        assert_eq!(policy.capacities[0].minimum["cpu"], 32_000);
    }
}
