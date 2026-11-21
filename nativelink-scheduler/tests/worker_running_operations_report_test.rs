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

//! A worker's keepalive names the operations it is running. An acknowledged
//! operation the worker leaves out of two reports in a row finished without
//! its result reaching the scheduler: its slot is released and it is requeued
//! through the error path.

use core::sync::atomic::{AtomicBool, Ordering};
use core::time::Duration;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

use async_trait::async_trait;
use nativelink_config::schedulers::WorkerAllocationStrategy;
use nativelink_error::{Code, Error};
use nativelink_macro::nativelink_test;
use nativelink_metric::MetricsComponent;
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::UpdateForWorker;
use nativelink_scheduler::api_worker_scheduler::ApiWorkerScheduler;
use nativelink_scheduler::platform_property_manager::PlatformPropertyManager;
use nativelink_scheduler::worker::{ActionInfoWithProps, Worker};
use nativelink_scheduler::worker_registry::WorkerRegistry;
use nativelink_scheduler::worker_scheduler::WorkerScheduler;
use nativelink_util::action_messages::{ActionResult, ActionStage, OperationId, WorkerId};
use nativelink_util::common::DigestInfo;
use nativelink_util::operation_state_manager::{UpdateOperationType, WorkerStateManager};
use nativelink_util::origin_event::OriginMetadata;
use nativelink_util::platform_properties::PlatformProperties;
use pretty_assertions::assert_eq;
use tokio::sync::{Notify, mpsc};
use utils::scheduler_utils::make_base_action_info;

mod utils {
    pub(crate) mod scheduler_utils;
}

const NOW: u64 = 10_000;
const WORKER_TIMEOUT_S: u64 = 10;

/// Records every error update the worker scheduler asks for, and answers
/// whether operations still belong to their worker.
#[derive(Default, MetricsComponent)]
struct RecordingStateManager {
    errors: Mutex<Vec<(OperationId, WorkerId, Code)>>,
    revoked: AtomicBool,
    fail_terminal: AtomicBool,
    completed: Mutex<Vec<Option<Code>>>,
}

impl RecordingStateManager {
    fn errors(&self) -> Vec<(OperationId, WorkerId, Code)> {
        self.errors.lock().unwrap().clone()
    }
}

#[async_trait]
impl WorkerStateManager for RecordingStateManager {
    async fn update_operation(
        &self,
        operation_id: &OperationId,
        worker_id: &WorkerId,
        update: UpdateOperationType,
    ) -> Result<(), Error> {
        if let UpdateOperationType::UpdateWithActionStage(ActionStage::Completed(result)) = &update
        {
            if self.fail_terminal.load(Ordering::Acquire) {
                return Err(nativelink_error::make_err!(
                    Code::Unavailable,
                    "state write failed"
                ));
            }
            self.completed
                .lock()
                .unwrap()
                .push(result.error.as_ref().map(|err| err.code));
        }
        if let UpdateOperationType::UpdateWithError(err) = update {
            self.errors
                .lock()
                .unwrap()
                .push((operation_id.clone(), worker_id.clone(), err.code));
        }
        Ok(())
    }

    async fn is_executing_on_worker(
        &self,
        _operation_id: &OperationId,
        _worker_id: &WorkerId,
    ) -> Result<bool, Error> {
        Ok(!self.revoked.load(Ordering::Acquire))
    }
}

struct Fixture {
    state_manager: Arc<RecordingStateManager>,
    scheduler: Arc<ApiWorkerScheduler>,
    worker_id: WorkerId,
    operation_id: OperationId,
    /// The worker's end of its command stream; dropping it disconnects the
    /// worker.
    _rx: mpsc::Receiver<UpdateForWorker>,
}

/// One connected worker holding one dispatched operation.
async fn dispatched() -> Result<Fixture, Error> {
    dispatched_with_deadline(None).await
}

async fn dispatched_with_deadline(deadline: Option<SystemTime>) -> Result<Fixture, Error> {
    let state_manager = Arc::new(RecordingStateManager::default());
    let scheduler = ApiWorkerScheduler::new_with_clock(
        state_manager.clone(),
        Arc::new(PlatformPropertyManager::new(HashMap::new())),
        WorkerAllocationStrategy::default(),
        None,
        None,
        None,
        Arc::new(Notify::new()),
        WORKER_TIMEOUT_S,
        60,
        0,
        Arc::new(WorkerRegistry::new()),
        None,
        None,
        false,
        Duration::from_secs(15),
        "scheduler".to_string(),
        Box::new(|| SystemTime::UNIX_EPOCH + Duration::from_secs(NOW)),
    );
    let worker_id = WorkerId::from(String::from("worker"));
    let (tx, mut rx) = mpsc::channel(8);
    scheduler
        .add_worker(Worker::new(
            worker_id.clone(),
            PlatformProperties::default(),
            tx,
            NOW,
            0,
        ))
        .await?;
    let operation_id = OperationId::default();
    scheduler
        .worker_notify_run_action(
            worker_id.clone(),
            operation_id.clone(),
            ActionInfoWithProps {
                inner: make_base_action_info(SystemTime::UNIX_EPOCH, DigestInfo::new([1u8; 32], 1)),
                platform_properties: PlatformProperties::default(),
                origin_metadata: OriginMetadata::default(),
                scheduler_start_execute_event_id: None,
            },
            NOW,
            deadline,
        )
        .await?;
    rx.recv()
        .await
        .expect("the worker should be told to run the action");
    Ok(Fixture {
        state_manager,
        scheduler,
        worker_id,
        operation_id,
        _rx: rx,
    })
}

#[nativelink_test]
async fn failed_terminal_publication_keeps_ownership_until_deadline() -> Result<(), Error> {
    let deadline = SystemTime::UNIX_EPOCH + Duration::from_secs(NOW + 30);
    let f = dispatched_with_deadline(Some(deadline)).await?;
    f.state_manager.fail_terminal.store(true, Ordering::Release);
    assert!(
        f.scheduler
            .update_action(
                &f.worker_id,
                &f.operation_id,
                UpdateOperationType::UpdateWithActionStage(ActionStage::Completed(
                    ActionResult::default()
                )),
            )
            .await
            .is_err()
    );
    assert_eq!(
        f.scheduler.worker_snapshot().await[0].latest_attempt_deadline,
        Some(deadline)
    );
    f.scheduler.worker_disconnected(&f.worker_id).await?;
    assert_eq!(f.scheduler.worker_snapshot().await[0].running_actions, 1);
    assert!(f.scheduler.expire_attempts(deadline).await.is_err());
    assert_eq!(f.scheduler.worker_snapshot().await[0].running_actions, 1);
    f.state_manager
        .fail_terminal
        .store(false, Ordering::Release);
    f.scheduler.expire_attempts(deadline).await?;
    assert_eq!(
        *f.state_manager.completed.lock().unwrap(),
        vec![Some(Code::DeadlineExceeded)]
    );
    assert!(f.state_manager.errors().is_empty());
    assert!(f.scheduler.worker_snapshot().await.is_empty());
    Ok(())
}

#[nativelink_test]
async fn acknowledged_operation_left_out_of_two_reports_is_requeued() -> Result<(), Error> {
    let f = dispatched().await?;
    f.scheduler
        .worker_dispatch_accepted(&f.worker_id, &f.operation_id)
        .await?;

    // Reported: nothing to do, however often.
    for _ in 0..3 {
        f.scheduler
            .worker_running_operations_reported(&f.worker_id, vec![f.operation_id.clone()])
            .await?;
    }
    assert_eq!(f.state_manager.errors(), vec![]);

    // Left out once: the report may have crossed the dispatch.
    f.scheduler
        .worker_running_operations_reported(&f.worker_id, vec![])
        .await?;
    assert_eq!(f.state_manager.errors(), vec![]);
    assert!(
        f.scheduler
            .running_action_info(&f.worker_id, &f.operation_id)
            .await
            .is_some(),
        "one missed report keeps the slot"
    );

    // Named again in between: the count starts over.
    f.scheduler
        .worker_running_operations_reported(&f.worker_id, vec![f.operation_id.clone()])
        .await?;
    f.scheduler
        .worker_running_operations_reported(&f.worker_id, vec![])
        .await?;
    assert_eq!(f.state_manager.errors(), vec![]);

    // Left out twice in a row: the result is lost; released and requeued
    // through the error path.
    f.scheduler
        .worker_running_operations_reported(&f.worker_id, vec![])
        .await?;
    assert_eq!(
        f.state_manager.errors(),
        vec![(
            f.operation_id.clone(),
            f.worker_id.clone(),
            Code::Unavailable
        )]
    );
    assert!(
        f.scheduler
            .running_action_info(&f.worker_id, &f.operation_id)
            .await
            .is_none(),
        "the slot is released"
    );

    // Gone from this worker: later reports have nothing to count.
    f.scheduler
        .worker_running_operations_reported(&f.worker_id, vec![])
        .await?;
    assert_eq!(f.state_manager.errors().len(), 1);
    Ok(())
}

#[nativelink_test]
async fn unacknowledged_operation_is_never_counted() -> Result<(), Error> {
    let f = dispatched().await?;
    for _ in 0..4 {
        f.scheduler
            .worker_running_operations_reported(&f.worker_id, vec![])
            .await?;
    }
    assert_eq!(f.state_manager.errors(), vec![]);
    assert!(
        f.scheduler
            .running_action_info(&f.worker_id, &f.operation_id)
            .await
            .is_some(),
        "a dispatch still in transit is the acknowledgement sweep's to judge"
    );
    Ok(())
}

#[nativelink_test]
async fn operation_told_to_die_is_never_counted() -> Result<(), Error> {
    let f = dispatched().await?;
    f.scheduler
        .worker_dispatch_accepted(&f.worker_id, &f.operation_id)
        .await?;
    // The store gave the operation to someone else: the revoked sweep tells
    // this worker to kill it, and it is on its way off.
    f.state_manager.revoked.store(true, Ordering::Release);
    f.scheduler.kill_revoked_operations().await?;
    for _ in 0..4 {
        f.scheduler
            .worker_running_operations_reported(&f.worker_id, vec![])
            .await?;
    }
    assert_eq!(f.state_manager.errors(), vec![]);
    Ok(())
}
