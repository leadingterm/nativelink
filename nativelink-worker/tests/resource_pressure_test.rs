// Copyright 2024 The NativeLink Authors. All rights reserved.
// Licensed under the Functional Source License, Version 1.1, Apache 2.0 Future License.

#![cfg(target_os = "linux")]

use std::collections::HashMap;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, mpsc};
use std::time::{Duration, Instant};

use futures::{FutureExt, TryFutureExt};
use nativelink_config::cas_server::{
    EnvironmentSource, UploadActionResultConfig, UploadCacheResultsStrategy,
};
use nativelink_config::stores::{
    FastSlowSpec, FilesystemSpec, MemorySpec, StoreDirection, StoreSpec,
};
use nativelink_error::Error;
use nativelink_macro::nativelink_test;
use nativelink_proto::build::bazel::remote::execution::v2::digest_function::Value as ProtoDigestFunction;
use nativelink_proto::build::bazel::remote::execution::v2::platform::Property;
use nativelink_proto::build::bazel::remote::execution::v2::{
    Action, Command, Directory, ExecuteRequest, Platform,
};
use nativelink_proto::com::github::trace_machina::nativelink::remote_execution::StartExecute;
use nativelink_store::ac_utils::serialize_and_upload_message;
use nativelink_store::fast_slow_store::FastSlowStore;
use nativelink_store::filesystem_store::FilesystemStore;
use nativelink_store::memory_store::MemoryStore;
use nativelink_util::action_messages::{ActionResult, OperationId};
use nativelink_util::digest_hasher::DigestHasherFunc;
use nativelink_util::store_trait::{Store, StoreLike};
use nativelink_worker::running_actions_manager::{
    ExecutionConfiguration, MemoryLimit, PressureKill, ResourceEnforcement, RunningAction,
    RunningActionImpl, RunningActionsManager, RunningActionsManagerArgs, RunningActionsManagerImpl,
    UseNamespaces, memory_pressure_monitor_with,
};

const POLL: Duration = Duration::from_millis(50);
const POLICY: PressureKill = PressureKill {
    stall_percent: 7,
    poll_interval: POLL,
    after: Duration::from_millis(250),
    reserve_bytes: 1_048_576,
};
type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

async fn run_action(action: Arc<RunningActionImpl>) -> Result<ActionResult, Error> {
    action
        .clone()
        .prepare_action()
        .and_then(RunningAction::execute)
        .and_then(RunningAction::upload_results)
        .and_then(RunningAction::get_finished_result)
        .then(|result| async move {
            action.cleanup().await?;
            result
        })
        .await
}

struct Fixture {
    _temp: tempfile::TempDir,
    pool: PathBuf,
    cas: Arc<FastSlowStore>,
    manager: Arc<RunningActionsManagerImpl>,
}

impl Fixture {
    async fn new() -> TestResult<Self> {
        Self::with_pool(true).await
    }

    async fn with_pool(configure_pool: bool) -> TestResult<Self> {
        let temp = tempfile::tempdir()?;
        let pool = temp.path().join("pool");
        let root = temp.path().join("actions");
        std::fs::create_dir_all(&pool)?;
        std::fs::create_dir_all(&root)?;
        std::fs::write(pool.join("memory.current"), "11264000")?;
        std::fs::write(pool.join("memory.max"), "12000000")?;
        std::fs::write(pool.join("memory.high"), "max")?;
        let side_channels = root.join(".side_channels");
        std::fs::create_dir(&side_channels)?;
        std::fs::set_permissions(&side_channels, std::fs::Permissions::from_mode(0o700))?;
        let fast_config = FilesystemSpec {
            content_path: temp.path().join("content").to_string_lossy().into_owned(),
            temp_path: temp
                .path()
                .join("store-temp")
                .to_string_lossy()
                .into_owned(),
            ..Default::default()
        };
        let slow_config = MemorySpec::default();
        let fast: Arc<FilesystemStore> = FilesystemStore::new(&fast_config).await?;
        let slow = MemoryStore::new(&slow_config);
        let cas = FastSlowStore::new(
            &FastSlowSpec {
                fast: StoreSpec::Filesystem(fast_config),
                slow: StoreSpec::Memory(slow_config),
                fast_direction: StoreDirection::default(),
                slow_direction: StoreDirection::default(),
                bypass_dedup_threshold_bytes: 0,
            },
            Store::new(fast),
            Store::new(slow),
        );
        let manager = Arc::new(RunningActionsManagerImpl::new(RunningActionsManagerArgs {
            enforced_environment: None,
            root_action_directory: root.to_string_lossy().into_owned(),
            execution_configuration: ExecutionConfiguration {
                additional_environment: Some(HashMap::from([(
                    "SIDE_CHANNEL_FILE".into(),
                    EnvironmentSource::SideChannelFile,
                )])),
                kill_grace: Duration::from_secs(20),
                resource_enforcement: Some(ResourceEnforcement {
                    memory: Some(MemoryLimit {
                        property_name: "memory_kb".into(),
                        headroom_percent: 19,
                        soft: false,
                        soft_samples: 3,
                        soft_spike_percent: 240,
                    }),
                    disk_property_name: None,
                    disk_soft: false,
                    disk_headroom_percent: 19,
                    memory_sample_interval: Duration::from_millis(17),
                    disk_sample_interval: Duration::from_millis(131),
                    disk_soft_samples: 3,
                    memory_pool_path: configure_pool.then(|| pool.clone()),
                    pressure_kill: Some(POLICY),
                    disk_pressure_kill: None,
                }),
                ..Default::default()
            },
            cas_store: cas.clone(),
            ac_store: None,
            historical_store: Store::new(cas.clone()),
            upload_action_result_config: &UploadActionResultConfig {
                upload_ac_results_strategy: UploadCacheResultsStrategy::Never,
                ..Default::default()
            },
            max_action_timeout: Duration::from_secs(120),
            max_upload_timeout: Duration::from_secs(30),
            max_download_timeout: Duration::from_secs(30),
            max_cleanup_wait: Duration::from_secs(30),
            max_cleanup_backoff: Duration::from_millis(10),
            timeout_handled_externally: false,
            active_input_leases: false,
            readonly_input_mounts: Vec::new(),
            directory_cache: None,
            use_namespaces: UseNamespaces::No,
        })?);
        assert!(manager.reserve_memory_pressure_monitor());
        Ok(Self {
            _temp: temp,
            pool,
            cas,
            manager,
        })
    }

    async fn action(
        &self,
        name: &str,
        reservation: u64,
        current: Option<u64>,
        metadata: Metadata,
    ) -> TestResult<LiveAction> {
        let marker = self._temp.path().join(name);
        // As in running_actions_manager_test, a shell exposes its PID and the side-channel path before exec.
        let shell = which::which("sh")?;
        let sleep = which::which("sleep")?;
        let command = Command {
            arguments: vec![
                shell.to_string_lossy().into_owned(),
                "-c".into(),
                "printf '%s\\n%s\\n' \"$$\" \"$SIDE_CHANNEL_FILE\" > \"$1\"; exec \"$2\" 120"
                    .into(),
                "pressure-test".into(),
                marker.to_string_lossy().into_owned(),
                sleep.to_string_lossy().into_owned(),
            ],
            working_directory: ".".into(),
            ..Default::default()
        };
        let command_digest = serialize_and_upload_message(
            &command,
            self.cas.as_pin(),
            &mut DigestHasherFunc::Sha256.hasher(),
        )
        .await?;
        let input = serialize_and_upload_message(
            &Directory::default(),
            self.cas.as_pin(),
            &mut DigestHasherFunc::Sha256.hasher(),
        )
        .await?;
        let digest = serialize_and_upload_message(
            &Action {
                command_digest: Some(command_digest.into()),
                input_root_digest: Some(input.into()),
                ..Default::default()
            },
            self.cas.as_pin(),
            &mut DigestHasherFunc::Sha256.hasher(),
        )
        .await?;
        let operation = OperationId::default();
        let action = self
            .manager
            .create_and_add_action(
                "worker".into(),
                StartExecute {
                    request_metadata: None,
                    execute_request: Some(ExecuteRequest {
                        action_digest: Some(digest.into()),
                        digest_function: ProtoDigestFunction::Sha256.into(),
                        ..Default::default()
                    }),
                    operation_id: operation.to_string(),
                    queued_timestamp: None,
                    platform: Some(Platform {
                        properties: vec![Property {
                            name: "memory_kb".into(),
                            value: reservation.to_string(),
                        }],
                    }),
                    worker_id: "worker".into(),
                    attempt_deadline: None,
                },
            )
            .await?;
        let run = tokio::spawn(run_action(action.clone()));
        wait_until(|| {
            std::fs::read_to_string(&marker)
                .is_ok_and(|text| text.ends_with('\n') && text.lines().count() == 2)
        })
        .await;
        let marker_text = std::fs::read_to_string(marker)?;
        let mut fields = marker_text.lines();
        let pid: u32 = fields.next().ok_or("missing pid")?.parse()?;
        let report = fields.next().ok_or("missing report path")?;
        let stat = std::fs::read_to_string(format!("/proc/{pid}/stat"))?;
        let start: u64 = stat
            .rsplit_once(')')
            .ok_or("bad stat")?
            .1
            .split_whitespace()
            .nth(19)
            .ok_or("missing start time")?
            .parse()?;
        let scope = self.pool.join(format!("{pid}-{start}-73"));
        std::fs::create_dir_all(&scope)?;
        std::fs::write(
            scope.join("memory.current"),
            format!("{}", current.unwrap_or(123) * 1024),
        )?;
        std::fs::write(
            scope.join("memory.peak"),
            format!("{}", (current.unwrap_or(123) + 11) * 1024),
        )?;
        std::fs::write(scope.join("cgroup.kill"), "")?;
        let value = match metadata {
            Metadata::Valid => Some(scope.to_string_lossy().into_owned()),
            Metadata::Absent => None,
            Metadata::DotDot => Some(format!("{}/../pool/{pid}-{start}-73", self.pool.display())),
            Metadata::WrongParent => Some(format!(
                "{}/other/{pid}-{start}-73",
                self._temp.path().display()
            )),
            Metadata::WrongIdentity => Some(
                self.pool
                    .join(format!("{}-{start}-73", pid + 1))
                    .to_string_lossy()
                    .into_owned(),
            ),
        };
        if let Some(value) = value {
            std::fs::write(format!("{report}.cgroup"), value)?;
        }
        if let Some(current) = current {
            wait_until(|| action.last_memory_kb() == current).await;
        } else {
            wait_until(|| action.last_memory_kb() > 0).await;
        }
        Ok(LiveAction { run, scope, pid })
    }
    async fn finish_action(&self, action: &mut LiveAction) -> TestResult {
        if !action.run.is_finished() {
            let status = std::process::Command::new(which::which("kill")?)
                .args(["-KILL", &action.pid.to_string()])
                .status()?;
            assert!(status.success(), "failed to stop fixture process");
        }
        drop((&mut action.run).await?);
        Ok(())
    }

    async fn stop(&self, actions: &[&LiveAction]) -> TestResult {
        for action in actions {
            if !action.run.is_finished() {
                let status = std::process::Command::new(which::which("kill")?)
                    .args(["-KILL", &action.pid.to_string()])
                    .status()?;
                assert!(status.success(), "failed to stop fixture process");
            }
        }
        self.manager.kill_all().await;
        Ok(())
    }
}

#[derive(Clone, Copy)]
enum Metadata {
    Valid,
    Absent,
    DotDot,
    WrongParent,
    WrongIdentity,
}
struct LiveAction {
    run: tokio::task::JoinHandle<Result<ActionResult, Error>>,
    scope: PathBuf,
    pid: u32,
}

async fn wait_until(mut condition: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while !condition() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("condition timed out");
}

type Poll = (Option<u64>, u64, tokio::sync::oneshot::Sender<()>);

struct Monitor {
    samples: Option<mpsc::Sender<Poll>>,
    abort: tokio::task::AbortHandle,
    thread: Option<std::thread::JoinHandle<()>>,
}
impl Monitor {
    fn start(manager: &Arc<RunningActionsManagerImpl>) -> Self {
        let (samples, receiver) = mpsc::channel::<Poll>();
        let (abort_tx, abort_rx) = mpsc::sync_channel(1);
        let manager = Arc::downgrade(manager);
        let thread = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            runtime.block_on(async move {
                let clock = Arc::new(Mutex::new(None::<(u64, tokio::sync::oneshot::Sender<()>)>));
                let reader_clock = clock.clone();
                let base = Instant::now();
                let task = tokio::spawn(memory_pressure_monitor_with(
                    manager,
                    POLICY,
                    move |_path: &Path| match receiver.recv() {
                        Ok((total, ms, ack)) => {
                            *reader_clock.lock().unwrap() = Some((ms, ack));
                            total
                        }
                        Err(_) => None,
                    },
                    move || {
                        let Some((ms, ack)) = clock.lock().unwrap().take() else {
                            return base;
                        };
                        let _ = ack.send(());
                        base + Duration::from_millis(ms)
                    },
                ));
                abort_tx.send(task.abort_handle()).unwrap();
                drop(task.await);
            });
        });
        Self {
            samples: Some(samples),
            abort: abort_rx.recv().unwrap(),
            thread: Some(thread),
        }
    }
    async fn step(&self, total: Option<u64>, ms: u64) {
        let (ack, observed) = tokio::sync::oneshot::channel();
        self.samples
            .as_ref()
            .unwrap()
            .send((total, ms, ack))
            .unwrap();
        tokio::time::timeout(Duration::from_secs(10), observed)
            .await
            .unwrap()
            .unwrap();
    }
    async fn pressure(&self, start_ms: u64, start_total: u64) {
        for i in 0..=5 {
            self.step(Some(start_total + i * 4_000), start_ms + i * 50)
                .await;
        }
    }
}
impl Drop for Monitor {
    fn drop(&mut self) {
        self.abort.abort();
        drop(self.samples.take());
        self.thread.take().unwrap().join().unwrap();
    }
}

#[nativelink_test]
async fn pressure_selects_excess_then_resets_window() -> TestResult {
    let fixture = Fixture::new().await?;
    let large = fixture
        .action("large", 9_000, Some(7_000), Metadata::Valid)
        .await?;
    let mut excess = fixture
        .action("excess", 1_000, Some(4_000), Metadata::Valid)
        .await?;
    let monitor = Monitor::start(&fixture.manager);
    monitor.step(Some(0), 0).await;
    for i in 1..=4 {
        monitor.step(Some(i * 4_000), i * 50).await;
    }
    monitor.step(Some(16_000), 250).await; // transient dip clears four high intervals
    assert_eq!(std::fs::read(&excess.scope.join("cgroup.kill"))?, b"");
    assert_eq!(std::fs::read(&large.scope.join("cgroup.kill"))?, b"");
    monitor.pressure(300, 16_000).await;
    wait_until(|| std::fs::read(excess.scope.join("cgroup.kill")).is_ok_and(|v| v == b"1")).await;
    assert_eq!(std::fs::read(large.scope.join("cgroup.kill"))?, b"");
    assert!(
        !excess.run.is_finished(),
        "cgroup kill must not signal wrapper's process group"
    );
    for i in 1..=13 {
        monitor.step(Some(36_000 + i * 4_000), 550 + i * 50).await;
    }
    assert_eq!(std::fs::read(large.scope.join("cgroup.kill"))?, b"");
    assert!(!large.run.is_finished());
    fixture.finish_action(&mut excess).await?;

    std::fs::write(fixture.pool.join("memory.max"), "67108864")?;
    monitor.pressure(1250, 92_000).await;
    monitor.pressure(1550, 116_000).await;
    monitor.step(Some(140_000), 1850).await;
    monitor.step(Some(144_000), 1900).await;
    assert_eq!(std::fs::read(large.scope.join("cgroup.kill"))?, b"");
    assert!(!large.run.is_finished());

    std::fs::write(fixture.pool.join("memory.max"), "12000000")?;
    for i in 0..=3 {
        monitor.step(Some(148_000 + i * 4_000), 1950 + i * 50).await;
    }
    monitor.step(Some(160_000), 2150).await;
    assert_eq!(std::fs::read(large.scope.join("cgroup.kill"))?, b"");
    monitor.pressure(2200, 160_000).await;
    wait_until(|| std::fs::read(large.scope.join("cgroup.kill")).is_ok_and(|v| v == b"1")).await;
    fixture.stop(&[&large]).await?;
    Ok(())
}

#[nativelink_test]
async fn ambient_stalls_with_pool_headroom_do_not_kill() -> TestResult {
    let fixture = Fixture::new().await?;
    std::fs::write(fixture.pool.join("memory.current"), "11264000")?;
    std::fs::write(fixture.pool.join("memory.max"), "67108864")?;
    std::fs::write(fixture.pool.join("memory.high"), "max")?;
    let large = fixture
        .action("large", 9_000, Some(7_000), Metadata::Valid)
        .await?;
    let excess = fixture
        .action("excess", 1_000, Some(4_000), Metadata::Valid)
        .await?;
    let monitor = Monitor::start(&fixture.manager);
    monitor.pressure(0, 0).await;
    monitor.pressure(300, 24_000).await;
    monitor.step(Some(48_000), 600).await;
    assert_eq!(std::fs::read(large.scope.join("cgroup.kill"))?, b"");
    assert_eq!(std::fs::read(excess.scope.join("cgroup.kill"))?, b"");
    assert!(!large.run.is_finished());
    assert!(!excess.run.is_finished());
    fixture.stop(&[&large, &excess]).await?;
    Ok(())
}

#[nativelink_test]
async fn errors_and_decreasing_counters_require_a_new_window() -> TestResult {
    let fixture = Fixture::new().await?;
    let a = fixture
        .action("a", 9_000, Some(7_000), Metadata::Valid)
        .await?;
    let b = fixture
        .action("b", 1_000, Some(4_000), Metadata::Valid)
        .await?;
    let monitor = Monitor::start(&fixture.manager);
    monitor.step(Some(0), 0).await;
    for i in 1..=4 {
        monitor.step(Some(i * 4_000), i * 50).await;
    }
    monitor.step(None, 250).await;
    for i in 0..=4 {
        monitor.step(Some(30_000 + i * 4_000), 300 + i * 50).await;
    }
    monitor.step(Some(1), 550).await;
    for i in 0..=4 {
        monitor.step(Some(1_000 + i * 4_000), 600 + i * 50).await;
    }
    assert_eq!(std::fs::read(b.scope.join("cgroup.kill"))?, b"");
    monitor.step(Some(21_000), 850).await;
    wait_until(|| std::fs::read(b.scope.join("cgroup.kill")).is_ok_and(|v| v == b"1")).await;
    assert_eq!(std::fs::read(a.scope.join("cgroup.kill"))?, b"");
    fixture.stop(&[&a, &b]).await?;
    Ok(())
}

#[nativelink_test]
async fn largest_below_reservation_and_already_killing_are_selected_correctly() -> TestResult {
    let fixture = Fixture::new().await?;
    let mut large = fixture
        .action("large", 9_000, Some(7_000), Metadata::Valid)
        .await?;
    let small = fixture
        .action("small", 8_000, Some(4_000), Metadata::Valid)
        .await?;
    let monitor = Monitor::start(&fixture.manager);
    monitor.pressure(0, 0).await;
    wait_until(|| std::fs::read(large.scope.join("cgroup.kill")).is_ok_and(|v| v == b"1")).await;
    assert_eq!(std::fs::read(small.scope.join("cgroup.kill"))?, b"");
    // The first action remains registered until its process exits and cleanup finishes.
    monitor.pressure(300, 24_000).await;
    monitor.pressure(600, 48_000).await;
    monitor.step(Some(72_000), 900).await;
    assert_eq!(std::fs::read(small.scope.join("cgroup.kill"))?, b"");
    assert!(!small.run.is_finished());
    fixture.finish_action(&mut large).await?;
    monitor.pressure(950, 76_000).await;
    monitor.step(Some(100_000), 1250).await;
    wait_until(|| std::fs::read(small.scope.join("cgroup.kill")).is_ok_and(|v| v == b"1")).await;
    fixture.stop(&[&small]).await?;
    Ok(())
}

#[nativelink_test]
async fn finite_high_causes_shortage_even_with_ample_max() -> TestResult {
    let fixture = Fixture::new().await?;
    std::fs::write(fixture.pool.join("memory.max"), "67108864")?;
    std::fs::write(fixture.pool.join("memory.high"), "12000000")?;
    let victim = fixture
        .action("victim", 1_000, Some(4_000), Metadata::Valid)
        .await?;
    let monitor = Monitor::start(&fixture.manager);
    monitor.pressure(0, 0).await;
    wait_until(|| std::fs::read(victim.scope.join("cgroup.kill")).is_ok_and(|v| v == b"1")).await;
    fixture.stop(&[&victim]).await?;
    Ok(())
}

#[nativelink_test]
async fn invalid_or_missing_pool_samples_reset_pressure_window() -> TestResult {
    for (file, value) in [
        ("memory.current", None),
        ("memory.current", Some("invalid")),
        ("memory.max", None),
        ("memory.max", Some("invalid")),
        ("memory.high", None),
        ("memory.high", Some("invalid")),
        ("memory.max", Some("max")),
    ] {
        let fixture = Fixture::new().await?;
        let victim = fixture
            .action("victim", 1_000, Some(4_000), Metadata::Valid)
            .await?;
        let monitor = Monitor::start(&fixture.manager);
        monitor.step(Some(0), 0).await;
        for i in 1..=4 {
            monitor.step(Some(i * 4_000), i * 50).await;
        }
        match value {
            Some(value) => std::fs::write(fixture.pool.join(file), value)?,
            None => std::fs::remove_file(fixture.pool.join(file))?,
        }
        monitor.pressure(250, 20_000).await;
        monitor.step(Some(44_000), 550).await;
        monitor.step(Some(48_000), 600).await;
        assert_eq!(
            std::fs::read(victim.scope.join("cgroup.kill"))?,
            b"",
            "{file}: {value:?}"
        );

        let restored = match file {
            "memory.current" => "11264000",
            "memory.max" => "12000000",
            "memory.high" => "max",
            _ => unreachable!(),
        };
        std::fs::write(fixture.pool.join(file), restored)?;
        for i in 0..=3 {
            monitor.step(Some(52_000 + i * 4_000), 650 + i * 50).await;
        }
        monitor.step(Some(64_000), 850).await;
        assert_eq!(
            std::fs::read(victim.scope.join("cgroup.kill"))?,
            b"",
            "{file}: {value:?}"
        );
        monitor.pressure(900, 64_000).await;
        wait_until(|| std::fs::read(victim.scope.join("cgroup.kill")).is_ok_and(|v| v == b"1"))
            .await;
        fixture.stop(&[&victim]).await?;
    }
    Ok(())
}

#[nativelink_test]
async fn absent_pool_never_reads_ancestor_pressure() -> TestResult {
    let fixture = Fixture::with_pool(false).await?;
    tokio::time::timeout(
        Duration::from_secs(10),
        memory_pressure_monitor_with(
            Arc::downgrade(&fixture.manager),
            POLICY,
            |_| panic!("must not read pressure without an explicit pool"),
            Instant::now,
        ),
    )
    .await?;
    Ok(())
}

#[nativelink_test]
async fn missing_or_invalid_metadata_uses_process_group_kill() -> TestResult {
    for (index, metadata) in [
        Metadata::Absent,
        Metadata::DotDot,
        Metadata::WrongParent,
        Metadata::WrongIdentity,
    ]
    .into_iter()
    .enumerate()
    {
        let fixture = Fixture::new().await?;
        let victim = fixture
            .action(&format!("victim-{index}"), 1, None, metadata)
            .await?;
        let survivor = fixture
            .action(
                &format!("survivor-{index}"),
                1_000_000,
                Some(100),
                Metadata::Valid,
            )
            .await?;
        let monitor = Monitor::start(&fixture.manager);
        monitor.pressure(0, 0).await;
        wait_until(|| victim.run.is_finished()).await;
        assert!(!Path::new(&format!("/proc/{}", victim.pid)).exists());
        assert_eq!(std::fs::read(victim.scope.join("cgroup.kill"))?, b"");
        assert_eq!(std::fs::read(survivor.scope.join("cgroup.kill"))?, b"");
        let result = victim.run.await??;
        assert!(result.error.is_some());
        fixture.stop(&[&survivor]).await?;
    }
    Ok(())
}
