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

use core::time::Duration;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::UNIX_EPOCH;

use mock_instant::thread_local::MockClock;
use nativelink_config::schedulers::{
    EscalationDimensionSpec, PropertyType, ResourceCapacity, ResourceEscalationSpec, SimpleSpec,
};
use nativelink_error::{Code, Error, make_err};
use nativelink_macro::nativelink_test;
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::{
    ActionResourceUsage, Reservation, ResourceOutcome, StartExecute, UpdateForWorker,
    update_for_worker,
};
use nativelink_scheduler::awaited_action_db::PeerScheduler;
use nativelink_scheduler::default_scheduler_factory::memory_awaited_action_db_factory;
use nativelink_scheduler::simple_scheduler::SimpleScheduler;
use nativelink_scheduler::worker::Worker;
use nativelink_scheduler::worker_scheduler::{ResourceCapacitySnapshot, WorkerScheduler};
use nativelink_util::action_messages::{
    ActionInfo, ActionResult, ActionStage, MemoryReservation, OperationId, WorkerId,
};
use nativelink_util::common::DigestInfo;
use nativelink_util::instant_wrapper::MockInstantWrapped;
use nativelink_util::operation_state_manager::{
    ActionStateResult, ClientStateManager, UpdateOperationType,
};
use nativelink_util::platform_properties::{PlatformProperties, PlatformPropertyValue};
use pretty_assertions::assert_eq;
use tokio::sync::{Notify, mpsc};
use utils::scheduler_utils::make_base_action_info;

mod utils {
    pub(crate) mod scheduler_utils;
}

const NOW_TIME: u64 = 10_000;
const GIB_KB: u64 = 1024 * 1024;
const NOMINAL_KB: u64 = 64 * GIB_KB;
// The configured row is already usable: subtract the 1 GiB reserve exactly once.
const ESTIMATE_KB: u64 = NOMINAL_KB - GIB_KB;
const OBSERVED_KB: u64 = 62_229 * 1024;
const PHYSICAL_CPU: u64 = 16;
const BOUND: Duration = Duration::from_secs(2);

fn capacity(fleet: &str, memory_kb: u64) -> ResourceCapacity {
    ResourceCapacity {
        minimum: BTreeMap::from([
            ("memory_kb".to_string(), memory_kb),
            ("cpu_count".to_string(), PHYSICAL_CPU),
        ]),
        exact: BTreeMap::from([
            ("fleet".to_string(), fleet.to_string()),
            ("ISA".to_string(), "arm64".to_string()),
        ]),
        ..Default::default()
    }
}

fn make_scheduler(
    capacities: Vec<ResourceCapacity>,
) -> (Arc<SimpleScheduler>, Arc<dyn WorkerScheduler>) {
    MockClock::set_time(Duration::from_secs(NOW_TIME));
    let notify = Arc::new(Notify::new());
    let spec = SimpleSpec {
        supported_platform_properties: Some(HashMap::from([
            ("memory_kb".to_string(), PropertyType::Minimum),
            ("cpu_count".to_string(), PropertyType::Minimum),
            ("fleet".to_string(), PropertyType::Exact),
            ("ISA".to_string(), PropertyType::Exact),
        ])),
        resource_escalation: Some(ResourceEscalationSpec {
            memory: EscalationDimensionSpec {
                property: "memory_kb".to_string(),
                percent: 250,
                max_steps: 3,
            },
            disk: None,
            headroom_percent: 25,
            cpu_property: "cpu_count".to_string(),
            capacity_ignored_properties: Vec::new(),
            capacities,
        }),
        max_job_retries: 1,
        max_worker_loss_retries: 2,
        ..SimpleSpec::default()
    };
    SimpleScheduler::new_with_callback(
        &spec,
        memory_awaited_action_db_factory(0, &notify, MockInstantWrapped::default),
        || async move {},
        notify,
        MockInstantWrapped::default,
        None,
    )
}

fn action(
    digest_byte: u8,
    fleet: Option<&str>,
    minimum_kb: Option<u64>,
    desired_kb: u64,
    cpu_before_memory_fit: Option<u64>,
) -> Arc<ActionInfo> {
    let mut info = make_base_action_info(
        UNIX_EPOCH + Duration::from_secs(NOW_TIME - 7),
        DigestInfo::new([digest_byte; 32], 512),
    );
    let info_mut = Arc::make_mut(&mut info);
    info_mut.priority = 37;
    info_mut.load_timestamp = UNIX_EPOCH + Duration::from_secs(NOW_TIME - 11);
    // This is the controller's estimate-based fit, not a fake worker advertisement.
    info_mut.platform_properties = HashMap::from([
        (
            "memory_kb".to_string(),
            desired_kb.min(ESTIMATE_KB).to_string(),
        ),
        ("cpu_count".to_string(), PHYSICAL_CPU.to_string()),
        ("ISA".to_string(), "arm64".to_string()),
    ]);
    if let Some(fleet) = fleet {
        info_mut
            .platform_properties
            .insert("fleet".to_string(), fleet.to_string());
    }
    info_mut.memory_reservation = Some(MemoryReservation {
        minimum_kb,
        desired_kb: Some(desired_kb),
        cpu_before_memory_fit,
        fitted_revision: None,
    });
    info
}

async fn add_worker(
    scheduler: &SimpleScheduler,
    id: &str,
    fleet: &str,
    memory_kb: u64,
) -> Result<mpsc::Receiver<UpdateForWorker>, Error> {
    let (tx, mut rx) = mpsc::channel(64);
    scheduler
        .add_worker(Worker::new(
            WorkerId(id.to_string()),
            PlatformProperties::new(HashMap::from([
                (
                    "memory_kb".to_string(),
                    PlatformPropertyValue::Minimum(memory_kb),
                ),
                (
                    "cpu_count".to_string(),
                    PlatformPropertyValue::Minimum(PHYSICAL_CPU),
                ),
                (
                    "fleet".to_string(),
                    PlatformPropertyValue::Exact(fleet.to_string()),
                ),
                (
                    "ISA".to_string(),
                    PlatformPropertyValue::Exact("arm64".to_string()),
                ),
            ])),
            tx,
            NOW_TIME,
            4,
        ))
        .await?;
    let connection = tokio::time::timeout(BOUND, rx.recv())
        .await
        .expect("worker registration completes within the bound")
        .expect("worker channel remains open");
    assert!(matches!(
        connection.update,
        Some(update_for_worker::Update::ConnectionResult(_))
    ));
    Ok(rx)
}

async fn dispatch(
    scheduler: &SimpleScheduler,
    rx: &mut mpsc::Receiver<UpdateForWorker>,
) -> StartExecute {
    scheduler
        .do_try_match_for_test()
        .await
        .expect("matching succeeds");
    let message = tokio::time::timeout(BOUND, rx.recv())
        .await
        .expect("action dispatches within the bound")
        .expect("worker remains connected");
    let Some(update_for_worker::Update::StartAction(start)) = message.update else {
        panic!("expected StartAction, got {message:?}");
    };
    start
}

fn dispatched_property(start: &StartExecute, name: &str) -> Option<u64> {
    start.platform.as_ref().and_then(|platform| {
        platform
            .properties
            .iter()
            .find(|property| property.name == name)
            .map(|property| property.value.parse().expect("numeric resource property"))
    })
}

fn fitted_property(info: &ActionInfo, name: &str) -> Option<u64> {
    info.platform_properties
        .get(name)
        .map(|value| value.parse().expect("numeric resource property"))
}

fn current_capacity(worker: &dyn WorkerScheduler) -> Arc<ResourceCapacitySnapshot> {
    worker
        .resource_capacity_updates()
        .expect("capacity watch is supported")
        .borrow()
        .clone()
}

async fn completion(listener: &mut Box<dyn ActionStateResult>) -> ActionResult {
    tokio::time::timeout(BOUND, async {
        loop {
            if let ActionStage::Completed(result) =
                &listener.as_state().await.expect("read state").0.stage
            {
                return result.clone();
            }
            listener
                .changed()
                .await
                .expect("completion state remains observable");
        }
    })
    .await
    .expect("operation completes within the bound")
}

#[nativelink_test]
async fn learned_64_gib_fits_real_memory_and_whole_pool_kill_is_terminal() -> Result<(), Error> {
    for minimum_kb in [None, Some(0)] {
        let (scheduler, worker) = make_scheduler(vec![capacity("compile", ESTIMATE_KB)]);
        let mut rx = add_worker(&scheduler, "measured", "compile", OBSERVED_KB).await?;
        let mut listener = scheduler
            .add_action(
                OperationId::from("learned"),
                action(1, Some("compile"), minimum_kb, NOMINAL_KB, Some(1)),
            )
            .await?;
        let start = dispatch(&scheduler, &mut rx).await;
        assert_eq!(dispatched_property(&start, "memory_kb"), Some(OBSERVED_KB));
        assert_eq!(dispatched_property(&start, "cpu_count"), Some(PHYSICAL_CPU));
        worker
            .record_action_resource_usage(
                &WorkerId("measured".to_string()),
                &OperationId::from(start.operation_id.as_str()),
                ActionResourceUsage {
                    peak_memory_kb: OBSERVED_KB,
                    sampled: true,
                    outcome: ResourceOutcome::KilledMemory.into(),
                    enforced: true,
                    reserved: Some(Reservation {
                        memory_kb: OBSERVED_KB,
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            )
            .await?;
        worker
            .update_action(
                &WorkerId("measured".to_string()),
                &OperationId::from(start.operation_id.as_str()),
                UpdateOperationType::UpdateWithActionStage(ActionStage::Completed(ActionResult {
                    exit_code: 9,
                    error: Some(make_err!(Code::FailedPrecondition, "worker resource kill")),
                    ..ActionResult::default()
                })),
            )
            .await?;
        let error = completion(&mut listener)
            .await
            .error
            .expect("whole-pool kill is terminal");
        assert_eq!(error.code, Code::FailedPrecondition);
        let message = error.to_string();
        assert!(
            message.contains(&format!("capacity {OBSERVED_KB} KiB")),
            "{message}"
        );
        assert_eq!(
            message
                .matches(&format!("operation {} killed", start.operation_id))
                .count(),
            1,
            "{message}"
        );
        scheduler.do_try_match_for_test().await?;
        assert_eq!(
            rx.try_recv(),
            Err(mpsc::error::TryRecvError::Empty),
            "must not requeue at 63 GiB"
        );
        assert!(matches!(
            listener.as_state().await?.0.stage,
            ActionStage::Completed(_)
        ));
    }
    Ok(())
}

#[nativelink_test]
async fn actions_queued_before_first_worker_refit_without_losing_identity_or_intent()
-> Result<(), Error> {
    // A cold whole-estimate reservation and a learned 64 GiB reservation both
    // start at 63 GiB, before any worker has supplied an observation.
    for desired_kb in [ESTIMATE_KB, NOMINAL_KB] {
        let (scheduler, worker) = make_scheduler(vec![capacity("compile", ESTIMATE_KB)]);
        let original = action(2, Some("compile"), None, desired_kb, Some(1));
        let listener = scheduler
            .add_action(OperationId::from("queued-before-worker"), original.clone())
            .await?;
        scheduler.do_try_match_for_test().await?;
        let queued_state = listener.as_state().await?.0;
        assert_eq!(queued_state.stage, ActionStage::Queued);
        let queued = listener.as_action_info().await?.0;
        assert_eq!(fitted_property(&queued, "memory_kb"), Some(ESTIMATE_KB));
        assert_eq!(
            queued.memory_reservation.as_ref().unwrap().fitted_revision,
            Some(current_capacity(worker.as_ref()).revision)
        );
        let mut rx = add_worker(&scheduler, "first", "compile", OBSERVED_KB).await?;
        let start = dispatch(&scheduler, &mut rx).await;
        assert_eq!(dispatched_property(&start, "memory_kb"), Some(OBSERVED_KB));
        assert_eq!(dispatched_property(&start, "cpu_count"), Some(PHYSICAL_CPU));
        let fitted = listener.as_action_info().await?.0;
        assert_eq!(fitted.priority, original.priority);
        assert_eq!(fitted.insert_timestamp, original.insert_timestamp);
        assert_eq!(fitted.load_timestamp, original.load_timestamp);
        assert_eq!(fitted.unique_qualifier, original.unique_qualifier);
        let metadata = fitted.memory_reservation.as_ref().unwrap();
        assert_eq!(metadata.minimum_kb, None);
        assert_eq!(metadata.desired_kb, Some(desired_kb));
        assert_eq!(metadata.cpu_before_memory_fit, Some(1));
        assert_eq!(
            metadata.fitted_revision,
            Some(current_capacity(worker.as_ref()).revision)
        );
        assert_ne!(
            metadata.fitted_revision,
            queued.memory_reservation.as_ref().unwrap().fitted_revision
        );
        let executing = listener.as_state().await?.0;
        assert_eq!(executing.stage, ActionStage::Executing);
        assert_eq!(
            executing.client_operation_id,
            queued_state.client_operation_id
        );
        assert_eq!(executing.action_digest, queued_state.action_digest);
    }
    Ok(())
}

#[nativelink_test]
async fn explicit_61_gib_floor_is_never_lowered_to_observed_memory() -> Result<(), Error> {
    const FLOOR_KB: u64 = 61 * GIB_KB;
    const { assert!(OBSERVED_KB < FLOOR_KB && FLOOR_KB < ESTIMATE_KB) };
    let (scheduler, _) = make_scheduler(vec![capacity("compile", ESTIMATE_KB)]);
    let original = action(3, Some("compile"), Some(FLOOR_KB), NOMINAL_KB, Some(1));
    let mut listener = scheduler
        .add_action(
            OperationId::from("explicit-before-worker"),
            original.clone(),
        )
        .await?;
    assert_eq!(listener.as_state().await?.0.stage, ActionStage::Queued);
    let mut rx = add_worker(&scheduler, "small", "compile", OBSERVED_KB).await?;
    scheduler.do_try_match_for_test().await?;
    let error = completion(&mut listener)
        .await
        .error
        .expect("explicit floor cannot be fitted down");
    assert_eq!(error.code, Code::FailedPrecondition);
    let message = error.to_string();
    assert!(
        message.contains(&FLOOR_KB.to_string()),
        "diagnostic names the original floor: {message}"
    );
    assert!(message.contains("memory"), "{message}");
    let failed_info = listener.as_action_info().await?.0;
    assert_eq!(
        failed_info.memory_reservation.as_ref().unwrap().minimum_kb,
        Some(FLOOR_KB)
    );
    assert!(fitted_property(&failed_info, "memory_kb").unwrap() >= FLOOR_KB);
    let Err(error) = scheduler
        .add_action(OperationId::from("explicit-after-worker"), original)
        .await
    else {
        panic!("known impossible explicit floor must fail before persistence");
    };
    assert_eq!(error.code, Code::FailedPrecondition);
    assert_eq!(rx.try_recv(), Err(mpsc::error::TryRecvError::Empty));
    Ok(())
}

#[nativelink_test]
async fn larger_unobserved_fleet_keeps_the_original_floor_eligible_and_restores_cpu()
-> Result<(), Error> {
    let (scheduler, worker) = make_scheduler(vec![
        capacity("compile", ESTIMATE_KB),
        capacity("large", 127 * GIB_KB),
    ]);
    let mut small_rx = add_worker(&scheduler, "small", "compile", OBSERVED_KB).await?;
    let original = action(4, None, Some(61 * GIB_KB), NOMINAL_KB, Some(1));
    let listener = scheduler
        .add_action(OperationId::from("larger-fleet"), original)
        .await?;
    scheduler.do_try_match_for_test().await?;
    assert_eq!(listener.as_state().await?.0.stage, ActionStage::Queued);
    let fitted = listener.as_action_info().await?.0;
    assert_eq!(fitted_property(&fitted, "memory_kb"), Some(NOMINAL_KB));
    assert_eq!(fitted_property(&fitted, "cpu_count"), Some(1));
    assert_eq!(
        fitted.memory_reservation.as_ref().unwrap().minimum_kb,
        Some(61 * GIB_KB)
    );
    assert_eq!(
        current_capacity(worker.as_ref()).capacities[1].minimum["memory_kb"],
        127 * GIB_KB
    );
    assert_eq!(small_rx.try_recv(), Err(mpsc::error::TryRecvError::Empty));
    let mut large_rx = add_worker(&scheduler, "large", "large", 127 * GIB_KB).await?;
    let start = dispatch(&scheduler, &mut large_rx).await;
    assert_eq!(dispatched_property(&start, "memory_kb"), Some(NOMINAL_KB));
    assert_eq!(dispatched_property(&start, "cpu_count"), Some(1));
    assert_eq!(listener.as_state().await?.0.stage, ActionStage::Executing);
    Ok(())
}

#[nativelink_test]
async fn reconnect_restores_observation_without_turning_action_ceilings_into_physical_memory()
-> Result<(), Error> {
    for baseline_cpu in [Some(2), None] {
        let mut limited = capacity("compile", ESTIMATE_KB);
        limited
            .action_ceilings
            .insert("memory_kb".to_string(), 4 * GIB_KB);
        let (scheduler, worker) = make_scheduler(vec![limited, capacity("other", ESTIMATE_KB)]);
        let _first_rx = add_worker(&scheduler, "first", "compile", OBSERVED_KB).await?;
        let observed = current_capacity(worker.as_ref());
        assert_eq!(observed.capacities[0].minimum["memory_kb"], OBSERVED_KB);
        assert_eq!(
            observed.capacities[0].action_ceilings["memory_kb"],
            4 * GIB_KB
        );
        assert_eq!(observed.capacities[1].minimum["memory_kb"], ESTIMATE_KB);
        scheduler
            .remove_worker(&WorkerId("first".to_string()))
            .await?;
        let unobserved = current_capacity(worker.as_ref());
        assert_ne!(observed.revision, unobserved.revision);
        assert_eq!(unobserved.capacities[0].minimum["memory_kb"], ESTIMATE_KB);
        assert_eq!(
            unobserved.capacities[0].action_ceilings["memory_kb"],
            4 * GIB_KB
        );
        assert_eq!(unobserved.capacities[1].minimum["memory_kb"], ESTIMATE_KB);
        let listener = scheduler
            .add_action(
                OperationId::from("logical-ceiling"),
                action(5, Some("compile"), None, NOMINAL_KB, baseline_cpu),
            )
            .await?;
        scheduler.do_try_match_for_test().await?;
        assert_eq!(listener.as_state().await?.0.stage, ActionStage::Queued);
        let queued = listener.as_action_info().await?.0;
        assert_eq!(
            queued.memory_reservation.as_ref().unwrap().fitted_revision,
            Some(unobserved.revision)
        );
        let mut rx = add_worker(&scheduler, "reconnected", "compile", OBSERVED_KB).await?;
        let reobserved = current_capacity(worker.as_ref());
        assert_ne!(unobserved.revision, reobserved.revision);
        assert_eq!(reobserved.capacities[0].minimum["memory_kb"], OBSERVED_KB);
        assert_eq!(reobserved.capacities[1].minimum["memory_kb"], ESTIMATE_KB);
        let start = dispatch(&scheduler, &mut rx).await;
        assert_eq!(dispatched_property(&start, "memory_kb"), Some(4 * GIB_KB));
        assert_eq!(
            dispatched_property(&start, "cpu_count"),
            baseline_cpu,
            "4 GiB is not whole physical memory"
        );
        assert_eq!(
            fitted_property(&listener.as_action_info().await?.0, "cpu_count"),
            baseline_cpu
        );
        assert_eq!(
            listener
                .as_action_info()
                .await?
                .0
                .memory_reservation
                .as_ref()
                .unwrap()
                .fitted_revision,
            Some(reobserved.revision)
        );
        assert_eq!(
            current_capacity(worker.as_ref()).capacities[0].minimum["memory_kb"],
            OBSERVED_KB
        );
    }
    let (scheduler, worker) = make_scheduler(vec![capacity("compile", ESTIMATE_KB)]);
    let _first_rx = add_worker(&scheduler, "first", "compile", OBSERVED_KB).await?;
    scheduler
        .remove_worker(&WorkerId("first".to_string()))
        .await?;
    let unobserved = current_capacity(worker.as_ref());
    assert_eq!(unobserved.capacities[0].minimum["memory_kb"], ESTIMATE_KB);
    let listener = scheduler
        .add_action(
            OperationId::from("reconnect-refit"),
            action(7, Some("compile"), None, NOMINAL_KB, Some(1)),
        )
        .await?;
    scheduler.do_try_match_for_test().await?;
    let queued = listener.as_action_info().await?.0;
    assert_eq!(fitted_property(&queued, "memory_kb"), Some(ESTIMATE_KB));
    assert_eq!(
        queued.memory_reservation.as_ref().unwrap().fitted_revision,
        Some(unobserved.revision)
    );
    let mut rx = add_worker(&scheduler, "reconnected", "compile", OBSERVED_KB).await?;
    let reobserved = current_capacity(worker.as_ref());
    assert_eq!(reobserved.capacities[0].minimum["memory_kb"], OBSERVED_KB);
    assert_ne!(unobserved.revision, reobserved.revision);
    let start = dispatch(&scheduler, &mut rx).await;
    assert_eq!(dispatched_property(&start, "memory_kb"), Some(OBSERVED_KB));
    assert_eq!(dispatched_property(&start, "cpu_count"), Some(PHYSICAL_CPU));
    assert_eq!(
        listener
            .as_action_info()
            .await?
            .0
            .memory_reservation
            .as_ref()
            .unwrap()
            .fitted_revision,
        Some(reobserved.revision)
    );
    Ok(())
}

#[nativelink_test]
async fn remote_only_capacity_publication_updates_watch_without_conflating_peer_shapes()
-> Result<(), Error> {
    let mut limited = capacity("compile", ESTIMATE_KB);
    limited
        .action_ceilings
        .insert("memory_kb".to_string(), 4 * GIB_KB);
    let capacities = vec![limited, capacity("other", ESTIMATE_KB)];
    let (sender, sender_worker) = make_scheduler(capacities.clone());
    let _rx = add_worker(&sender, "remote", "compile", OBSERVED_KB).await?;
    let published = PeerScheduler {
        scheduler_id: sender.scheduler_id_for_test().to_string(),
        workers: sender.fleet_shapes_for_test().await,
        resource_capacities: Some(current_capacity(sender_worker.as_ref()).capacities.clone()),
    };
    let (receiver, receiver_worker) = make_scheduler(capacities);
    let mut updates = receiver_worker
        .resource_capacity_updates()
        .expect("capacity watch");
    let before = updates.borrow_and_update().clone();
    // Exercise the existing peer exchange seam, not a fake local worker.
    receiver
        .set_peer_schedulers_for_test(vec![published.clone()])
        .await;
    tokio::time::timeout(BOUND, updates.changed())
        .await
        .expect("peer observation publishes promptly")
        .expect("capacity sender remains alive");
    let observed = updates.borrow_and_update().clone();
    assert_ne!(before.revision, observed.revision);
    assert_eq!(observed.capacities[0].minimum["memory_kb"], OBSERVED_KB);
    assert_eq!(
        observed.capacities[0].action_ceilings["memory_kb"],
        4 * GIB_KB
    );
    assert_eq!(observed.capacities[1].minimum["memory_kb"], ESTIMATE_KB);
    let listener = receiver
        .add_action(
            OperationId::from("remote-only"),
            action(6, Some("compile"), None, NOMINAL_KB, Some(2)),
        )
        .await?;
    receiver.do_try_match_for_test().await?;
    assert_eq!(listener.as_state().await?.0.stage, ActionStage::Queued);
    let fitted = listener.as_action_info().await?.0;
    assert_eq!(fitted_property(&fitted, "memory_kb"), Some(4 * GIB_KB));
    assert_eq!(fitted_property(&fitted, "cpu_count"), Some(2));
    assert_eq!(
        fitted.memory_reservation.as_ref().unwrap().fitted_revision,
        Some(observed.revision)
    );
    receiver.set_peer_schedulers_for_test(vec![]).await;
    receiver.set_peers_unknown_for_test().await;
    let without_peer = current_capacity(receiver_worker.as_ref());
    assert_ne!(observed.revision, without_peer.revision);
    assert_eq!(without_peer.capacities[0].minimum["memory_kb"], ESTIMATE_KB);
    assert_eq!(without_peer.capacities[1].minimum["memory_kb"], ESTIMATE_KB);
    let mut estimated_peer = published;
    estimated_peer.resource_capacities = Some(before.capacities.clone());
    receiver
        .set_peer_schedulers_for_test(vec![estimated_peer])
        .await;
    assert!(
        Arc::ptr_eq(&without_peer, &current_capacity(receiver_worker.as_ref())),
        "an unchanged current estimate does not publish a new snapshot"
    );
    Ok(())
}
