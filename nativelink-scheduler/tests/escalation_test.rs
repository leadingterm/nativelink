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
use nativelink_error::{Code, Error, ResultExt, make_err};
use nativelink_macro::nativelink_test;
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::{
    ActionResourceUsage, Reservation, ResourceOutcome, StartExecute, UpdateForWorker,
    update_for_worker,
};
use nativelink_scheduler::default_scheduler_factory::memory_awaited_action_db_factory;
use nativelink_scheduler::simple_scheduler::SimpleScheduler;
use nativelink_scheduler::worker::Worker;
use nativelink_scheduler::worker_scheduler::WorkerScheduler;
use nativelink_util::action_messages::{
    ActionInfo, ActionResult, ActionStage, OperationId, WorkerId,
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

const NOW_TIME: u64 = 10000;
const WORKER: &str = "the_worker";

#[test]
fn capacity_serde_defaults_and_rejects_unknown_fields() {
    let empty: ResourceCapacity = serde_json::from_str("{}").expect("empty maps default");
    assert!(empty.minimum.is_empty());
    assert!(empty.exact.is_empty());
    assert!(empty.action_ceilings.is_empty());
    assert!(empty.pool_multipliers.is_empty());
    let capacity: ResourceCapacity =
        serde_json::from_str(r#"{"minimum":{"memory_kb":12345},"exact":{"ISA":"arm64"}}"#)
            .expect("capacity parses");
    assert_eq!(capacity.minimum.get("memory_kb"), Some(&12_345));
    assert_eq!(capacity.exact.get("ISA").map(String::as_str), Some("arm64"));
    assert!(capacity.pool_multipliers.is_empty());
    assert_eq!(capacity.effective_capacity("memory_kb"), Some(12_345));
    assert!(serde_json::from_str::<ResourceCapacity>(r#"{"memory_kb":12345}"#).is_err());
    assert!(
        serde_json::from_str::<ResourceCapacity>(r#"{"pool_multipliers":{"cpu_count":0}}"#)
            .is_err()
    );
}

#[test]
fn effective_capacity_converts_ledgers_and_respects_explicit_ceilings() {
    let mut row: ResourceCapacity = serde_json::from_str(
        r#"{
            "minimum":{"cpu":96000,"memory":64000,"disk":100000},
            "pool_multipliers":{"cpu":3}
        }"#,
    )
    .expect("valid capacity");
    assert_eq!(row.effective_capacity("cpu"), Some(32_000));
    assert_eq!(row.effective_capacity("memory"), Some(64_000));
    assert_eq!(row.effective_capacity("disk"), Some(100_000));
    assert_eq!(row.effective_capacity("missing"), None);
    row.action_ceilings.insert("cpu".into(), 16_000);
    row.action_ceilings.insert("memory".into(), 32_000);
    row.action_ceilings.insert("disk".into(), 200_000);
    row.action_ceilings.insert("missing".into(), 1);
    assert_eq!(row.effective_capacity("cpu"), Some(16_000));
    assert_eq!(row.effective_capacity("memory"), Some(32_000));
    assert_eq!(row.effective_capacity("disk"), Some(100_000));
    assert_eq!(row.effective_capacity("missing"), None);
}

fn capacity(memory: u64, disk: u64, cpu: u64) -> ResourceCapacity {
    ResourceCapacity {
        minimum: BTreeMap::from([
            ("memory_kb".to_string(), memory),
            ("disk_kb".to_string(), disk),
            ("cpu_count".to_string(), cpu),
        ]),
        exact: BTreeMap::from([
            ("ISA".to_string(), "arm64".to_string()),
            ("fleet".to_string(), "compile".to_string()),
            ("sandbox".to_string(), "isolated".to_string()),
        ]),
        ..Default::default()
    }
}

fn make_scheduler() -> (Arc<SimpleScheduler>, Arc<dyn WorkerScheduler>) {
    // Matching worker observations replace these nominal estimates.
    make_scheduler_with_capacities(vec![
        capacity(100_000, 100_000, 8),
        capacity(200_000, 500, 16),
    ])
}

fn make_scheduler_with_capacities(
    capacities: Vec<ResourceCapacity>,
) -> (Arc<SimpleScheduler>, Arc<dyn WorkerScheduler>) {
    MockClock::set_time(Duration::from_secs(NOW_TIME));
    let notify = Arc::new(Notify::new());
    let dimension = |property: &str| EscalationDimensionSpec {
        property: property.to_string(),
        percent: 250,
        max_steps: 3,
    };
    let spec = SimpleSpec {
        supported_platform_properties: Some(HashMap::from([
            ("memory_kb".to_string(), PropertyType::Minimum),
            ("disk_kb".to_string(), PropertyType::Minimum),
            ("cpu_count".to_string(), PropertyType::Minimum),
            ("ISA".to_string(), PropertyType::Exact),
            ("fleet".to_string(), PropertyType::Exact),
            ("sandbox".to_string(), PropertyType::Exact),
        ])),
        resource_escalation: Some(ResourceEscalationSpec {
            memory: dimension("memory_kb"),
            disk: Some(dimension("disk_kb")),
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

async fn add_worker(scheduler: &SimpleScheduler) -> Result<mpsc::Receiver<UpdateForWorker>, Error> {
    add_worker_with_capacity(scheduler, 100_000, 100_000, 8).await
}

async fn add_worker_with_capacity(
    scheduler: &SimpleScheduler,
    memory: u64,
    disk: u64,
    cpu: u64,
) -> Result<mpsc::Receiver<UpdateForWorker>, Error> {
    add_named_worker(scheduler, WORKER, memory, disk, cpu, false).await
}

async fn add_named_worker(
    scheduler: &SimpleScheduler,
    id: &str,
    memory: u64,
    disk: u64,
    cpu: u64,
    draining: bool,
) -> Result<mpsc::Receiver<UpdateForWorker>, Error> {
    let (tx, mut rx) = mpsc::channel(64);
    let mut worker = Worker::new(
        WorkerId(id.to_string()),
        PlatformProperties::new(HashMap::from([
            (
                "memory_kb".to_string(),
                PlatformPropertyValue::Minimum(memory),
            ),
            ("disk_kb".to_string(), PlatformPropertyValue::Minimum(disk)),
            ("cpu_count".to_string(), PlatformPropertyValue::Minimum(cpu)),
            (
                "ISA".to_string(),
                PlatformPropertyValue::Exact("arm64".to_string()),
            ),
            (
                "fleet".to_string(),
                PlatformPropertyValue::Exact("compile".to_string()),
            ),
            (
                "sandbox".to_string(),
                PlatformPropertyValue::Exact("isolated".to_string()),
            ),
        ])),
        tx,
        NOW_TIME,
        4,
    );
    worker.is_draining = draining;
    scheduler
        .add_worker(worker)
        .await
        .err_tip(|| "Failed to add worker")?;
    tokio::task::yield_now().await;
    assert!(matches!(
        rx.recv().await.unwrap().update,
        Some(update_for_worker::Update::ConnectionResult(_))
    ));
    Ok(rx)
}

async fn add_action(
    scheduler: &SimpleScheduler,
    memory: u64,
    disk: u64,
) -> Result<Box<dyn ActionStateResult>, Error> {
    let base = make_base_action_info(
        UNIX_EPOCH + MockClock::time(),
        DigestInfo::new([42; 32], 512),
    );
    let action = Arc::new(ActionInfo {
        platform_properties: HashMap::from([
            ("memory_kb".to_string(), memory.to_string()),
            ("disk_kb".to_string(), disk.to_string()),
            ("cpu_count".to_string(), "1".to_string()),
            ("ISA".to_string(), "arm64".to_string()),
            ("fleet".to_string(), "compile".to_string()),
            ("sandbox".to_string(), "isolated".to_string()),
        ]),
        ..(*base).clone()
    });
    let listener = scheduler.add_action(OperationId::default(), action).await?;
    tokio::task::yield_now().await;
    Ok(listener)
}

async fn next_dispatch(rx: &mut mpsc::Receiver<UpdateForWorker>) -> StartExecute {
    let msg = tokio::time::timeout(Duration::from_secs(2), rx.recv())
        .await
        .expect("a dispatch within two seconds")
        .expect("the channel is open");
    let Some(update_for_worker::Update::StartAction(start)) = msg.update else {
        panic!("expected a StartAction, got {msg:?}");
    };
    start
}

fn property(start: &StartExecute, name: &str) -> u64 {
    start
        .platform
        .as_ref()
        .and_then(|platform| {
            platform
                .properties
                .iter()
                .find(|p| p.name == name)
                .and_then(|p| p.value.parse().ok())
        })
        .expect("the dispatch carries the property")
}

async fn record_usage(
    worker: &dyn WorkerScheduler,
    id: &str,
    outcome: ResourceOutcome,
    reserved: u64,
    peak: u64,
) -> Result<(), Error> {
    let memory = outcome == ResourceOutcome::KilledMemory;
    worker
        .record_action_resource_usage(
            &WorkerId(WORKER.to_string()),
            &OperationId::from(id),
            ActionResourceUsage {
                peak_memory_kb: if memory { peak } else { 0 },
                peak_disk_kb: if memory { 0 } else { peak },
                sampled: true,
                outcome: outcome.into(),
                enforced: true,
                reserved: Some(Reservation {
                    memory_kb: if memory { reserved } else { 0 },
                    disk_kb: if memory { 0 } else { reserved },
                    ..Default::default()
                }),
                ..Default::default()
            },
        )
        .await
}

async fn finish(worker: &dyn WorkerScheduler, id: &str, result: ActionResult) -> Result<(), Error> {
    worker
        .update_action(
            &WorkerId(WORKER.to_string()),
            &OperationId::from(id),
            UpdateOperationType::UpdateWithActionStage(ActionStage::Completed(result)),
        )
        .await?;
    tokio::task::yield_now().await;
    Ok(())
}

async fn kill(
    worker: &dyn WorkerScheduler,
    id: &str,
    outcome: ResourceOutcome,
    reserved: u64,
    peak: u64,
) -> Result<(), Error> {
    record_usage(worker, id, outcome, reserved, peak).await?;
    finish(
        worker,
        id,
        ActionResult {
            exit_code: 9,
            error: Some(make_err!(Code::FailedPrecondition, "worker resource kill")),
            ..ActionResult::default()
        },
    )
    .await
}

async fn wait_for_completion(listener: &mut Box<dyn ActionStateResult>) -> ActionResult {
    loop {
        let (state, _) = tokio::time::timeout(Duration::from_secs(2), listener.changed())
            .await
            .expect("completion within two seconds")
            .expect("state stream open");
        if let ActionStage::Completed(result) = &state.stage {
            return result.clone();
        }
    }
}

async fn assert_terminal(
    listener: &mut Box<dyn ActionStateResult>,
    rx: &mut mpsc::Receiver<UpdateForWorker>,
    id: &str,
    peak: u64,
    capacity: u64,
    reason: &str,
) {
    let result = wait_for_completion(listener).await;
    let error = result.error.expect("terminal resource failure");
    assert_eq!(error.code, Code::FailedPrecondition);
    let message = error.to_string();
    assert!(message.contains(id), "{message}");
    assert!(
        message.contains(&format!("measured peak {peak} KiB")),
        "{message}"
    );
    assert!(
        message.contains(&format!("capacity {capacity} KiB")),
        "{message}"
    );
    assert!(message.contains(reason), "{message}");
    assert_eq!(
        message.matches(&format!("operation {id} killed")).count(),
        1,
        "{message}"
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(50), rx.recv())
            .await
            .is_err(),
        "terminal failure must not requeue"
    );
}

#[nativelink_test]
async fn exact_properties_bound_escalation_and_impossible_peaks() -> Result<(), Error> {
    for (key, value) in [
        ("", Some("")),
        ("ISA", Some("x86-64")),
        ("fleet", Some("link")),
        ("sandbox", Some("shared")),
        ("sandbox", None),
    ] {
        let mut large = capacity(200_000, 100_000, 16);
        if !key.is_empty() {
            if let Some(value) = value {
                large.exact.insert(key.to_string(), value.to_string());
            } else {
                large.exact.remove(key);
            }
        }
        let (scheduler, worker) =
            make_scheduler_with_capacities(vec![capacity(100_000, 100_000, 8), large]);
        let mut rx = if key.is_empty() {
            add_worker_with_capacity(&scheduler, 200_000, 100_000, 16).await?
        } else {
            add_worker(&scheduler).await?
        };
        let mut listener = add_action(&scheduler, 50_000, 1_000).await?;
        let first = next_dispatch(&mut rx).await;
        kill(
            worker.as_ref(),
            &first.operation_id,
            ResourceOutcome::KilledMemory,
            50_000,
            80_000,
        )
        .await?;
        let second = next_dispatch(&mut rx).await;
        let eligible = key.is_empty();
        assert_eq!(
            property(&second, "memory_kb"),
            if eligible { 125_000 } else { 100_000 }
        );
        assert_eq!(property(&second, "cpu_count"), if eligible { 1 } else { 8 });
        if !eligible {
            kill(
                worker.as_ref(),
                &second.operation_id,
                ResourceOutcome::KilledMemory,
                100_000,
                150_000,
            )
            .await?;
            assert_terminal(
                &mut listener,
                &mut rx,
                &second.operation_id,
                150_000,
                100_000,
                "exceeds capacity",
            )
            .await;
        }
    }
    Ok(())
}

#[nativelink_test]
async fn percent_and_measured_peak_each_dominate_and_requeues_ignore_job_budget()
-> Result<(), Error> {
    let (scheduler, worker) = make_scheduler();
    let mut rx = add_worker(&scheduler).await?;
    let _listener = add_action(&scheduler, 1_001, 1_000).await?;
    let first = next_dispatch(&mut rx).await;
    // ceil(1001 * 250 / 100), not the small measured peak.
    kill(
        worker.as_ref(),
        &first.operation_id,
        ResourceOutcome::KilledMemory,
        1_001,
        7,
    )
    .await?;
    let second = next_dispatch(&mut rx).await;
    assert_eq!(property(&second, "memory_kb"), 2_503);
    assert_eq!(second.operation_id, first.operation_id);
    // ceil(6003 * 125 / 100) exceeds ceil(2503 * 250 / 100).
    kill(
        worker.as_ref(),
        &second.operation_id,
        ResourceOutcome::KilledMemory,
        2_503,
        6_003,
    )
    .await?;
    let third = next_dispatch(&mut rx).await;
    assert_eq!(property(&third, "memory_kb"), 7_504);
    kill(
        worker.as_ref(),
        &third.operation_id,
        ResourceOutcome::KilledMemory,
        7_504,
        1,
    )
    .await?;
    let fourth = next_dispatch(&mut rx).await;
    assert_eq!(property(&fourth, "memory_kb"), 18_760);
    assert_eq!(property(&fourth, "disk_kb"), 1_000);
    Ok(())
}

#[nativelink_test]
async fn eligible_capacity_clamps_and_reserves_whole_cpu_then_kill_is_terminal() -> Result<(), Error>
{
    let (scheduler, worker) = make_scheduler();
    let mut rx = add_worker(&scheduler).await?;
    let mut listener = add_action(&scheduler, 50_000, 1_000).await?;
    let first = next_dispatch(&mut rx).await;
    kill(
        worker.as_ref(),
        &first.operation_id,
        ResourceOutcome::KilledMemory,
        50_000,
        80_000,
    )
    .await?;
    let second = next_dispatch(&mut rx).await;
    // The matching worker's actual pool, rather than the nominal rows, bounds retries.
    assert_eq!(property(&second, "memory_kb"), 100_000);
    assert_eq!(property(&second, "cpu_count"), 8);
    kill(
        worker.as_ref(),
        &second.operation_id,
        ResourceOutcome::KilledMemory,
        100_000,
        99_999,
    )
    .await?;
    assert_terminal(
        &mut listener,
        &mut rx,
        &second.operation_id,
        99_999,
        100_000,
        "whole-worker",
    )
    .await;
    Ok(())
}

#[nativelink_test]
async fn policy_ceiling_retries_without_whole_cpu_then_stops() -> Result<(), Error> {
    let mut limited = capacity(200_000, 100_000, 16);
    limited
        .action_ceilings
        .insert("memory_kb".to_string(), 100_000);
    limited.action_ceilings.insert("cpu_count".to_string(), 4);
    let (scheduler, worker) = make_scheduler_with_capacities(vec![limited]);
    let mut rx = add_worker_with_capacity(&scheduler, 200_000, 100_000, 16).await?;
    let mut listener = add_action(&scheduler, 50_000, 1_000).await?;
    let first = next_dispatch(&mut rx).await;
    kill(
        worker.as_ref(),
        &first.operation_id,
        ResourceOutcome::KilledMemory,
        50_000,
        80_000,
    )
    .await?;
    let second = next_dispatch(&mut rx).await;
    assert_eq!(property(&second, "memory_kb"), 100_000);
    assert_eq!(property(&second, "cpu_count"), 1);
    kill(
        worker.as_ref(),
        &second.operation_id,
        ResourceOutcome::KilledMemory,
        100_000,
        99_999,
    )
    .await?;
    assert_terminal(
        &mut listener,
        &mut rx,
        &second.operation_id,
        99_999,
        100_000,
        "per-action",
    )
    .await;
    Ok(())
}

#[nativelink_test]
async fn physical_resource_cap_reserves_only_allowed_cpu() -> Result<(), Error> {
    let mut limited = capacity(100_000, 100_000, 16);
    limited.action_ceilings.insert("cpu_count".to_string(), 4);
    let (scheduler, worker) = make_scheduler_with_capacities(vec![limited]);
    let mut rx = add_worker_with_capacity(&scheduler, 100_000, 100_000, 16).await?;
    let _listener = add_action(&scheduler, 50_000, 1_000).await?;
    let first = next_dispatch(&mut rx).await;
    kill(
        worker.as_ref(),
        &first.operation_id,
        ResourceOutcome::KilledMemory,
        50_000,
        80_000,
    )
    .await?;
    let second = next_dispatch(&mut rx).await;
    assert_eq!(property(&second, "memory_kb"), 100_000);
    assert_eq!(property(&second, "cpu_count"), 4);
    Ok(())
}

#[nativelink_test]
async fn eligibility_ignores_the_dimension_being_escalated() -> Result<(), Error> {
    let (scheduler, worker) = make_scheduler_with_capacities(vec![capacity(200_000, 100_000, 16)]);
    let mut rx = add_worker_with_capacity(&scheduler, 200_000, 100_000, 16).await?;
    let _listener = add_action(&scheduler, 150_000, 1_000).await?;
    let first = next_dispatch(&mut rx).await;
    // The smaller worker contributes actual capacity but cannot take the retry.
    let _small_rx = add_named_worker(&scheduler, "small_worker", 100_000, 100_000, 8, true).await?;
    kill(
        worker.as_ref(),
        &first.operation_id,
        ResourceOutcome::KilledMemory,
        150_000,
        99_000,
    )
    .await?;
    let second = next_dispatch(&mut rx).await;
    assert_eq!(property(&second, "memory_kb"), 100_000);
    assert_eq!(property(&second, "cpu_count"), 8);
    Ok(())
}

#[nativelink_test]
async fn peak_above_eligible_capacity_is_one_terminal_failure() -> Result<(), Error> {
    let (scheduler, worker) = make_scheduler();
    let mut rx = add_worker(&scheduler).await?;
    let mut listener = add_action(&scheduler, 1_000, 1_000).await?;
    let first = next_dispatch(&mut rx).await;
    kill(
        worker.as_ref(),
        &first.operation_id,
        ResourceOutcome::KilledMemory,
        1_000,
        100_001,
    )
    .await?;
    assert_terminal(
        &mut listener,
        &mut rx,
        &first.operation_id,
        100_001,
        100_000,
        "exceeds capacity",
    )
    .await;
    Ok(())
}

#[nativelink_test]
async fn max_steps_is_per_resource_and_exhaustion_is_terminal() -> Result<(), Error> {
    let (scheduler, worker) = make_scheduler();
    let mut rx = add_worker(&scheduler).await?;
    let mut listener = add_action(&scheduler, 1_000, 1_000).await?;
    for reserved in [1_000, 2_500, 6_250] {
        let dispatch = next_dispatch(&mut rx).await;
        assert_eq!(property(&dispatch, "memory_kb"), reserved);
        kill(
            worker.as_ref(),
            &dispatch.operation_id,
            ResourceOutcome::KilledMemory,
            reserved,
            1,
        )
        .await?;
    }
    let disk_dispatch = next_dispatch(&mut rx).await;
    assert_eq!(property(&disk_dispatch, "memory_kb"), 15_625);
    assert_eq!(property(&disk_dispatch, "disk_kb"), 1_000);
    kill(
        worker.as_ref(),
        &disk_dispatch.operation_id,
        ResourceOutcome::KilledDisk,
        1_000,
        1,
    )
    .await?;
    let after_disk = next_dispatch(&mut rx).await;
    assert_eq!(property(&after_disk, "memory_kb"), 15_625);
    assert_eq!(property(&after_disk, "disk_kb"), 2_500);
    kill(
        worker.as_ref(),
        &after_disk.operation_id,
        ResourceOutcome::KilledMemory,
        15_625,
        1,
    )
    .await?;
    assert_terminal(
        &mut listener,
        &mut rx,
        &after_disk.operation_id,
        1,
        100_000,
        "max_steps 3 exhausted",
    )
    .await;
    Ok(())
}

#[nativelink_test]
async fn kill_without_usage_report_is_not_escalated() -> Result<(), Error> {
    let (scheduler, worker) = make_scheduler();
    let mut rx = add_worker(&scheduler).await?;
    let mut listener = add_action(&scheduler, 1_000, 1_000).await?;
    let first = next_dispatch(&mut rx).await;
    finish(
        worker.as_ref(),
        &first.operation_id,
        ActionResult {
            exit_code: 9,
            error: Some(make_err!(Code::FailedPrecondition, "unreported kill")),
            ..ActionResult::default()
        },
    )
    .await?;
    let result = wait_for_completion(&mut listener).await;
    assert_eq!(result.exit_code, 9);
    assert!(
        result
            .error
            .expect("worker error")
            .to_string()
            .contains("unreported kill")
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(50), rx.recv())
            .await
            .is_err()
    );
    Ok(())
}

#[nativelink_test]
async fn completed_usage_does_not_escalate_or_change_result() -> Result<(), Error> {
    let (scheduler, worker) = make_scheduler();
    let mut rx = add_worker(&scheduler).await?;
    let mut listener = add_action(&scheduler, 1_000, 1_000).await?;
    let first = next_dispatch(&mut rx).await;
    record_usage(
        worker.as_ref(),
        &first.operation_id,
        ResourceOutcome::Completed,
        1_000,
        90_000,
    )
    .await?;
    let expected = ActionResult {
        exit_code: 23,
        ..ActionResult::default()
    };
    finish(worker.as_ref(), &first.operation_id, expected.clone()).await?;
    assert_eq!(wait_for_completion(&mut listener).await, expected);
    assert!(
        tokio::time::timeout(Duration::from_millis(50), rx.recv())
            .await
            .is_err()
    );
    Ok(())
}
