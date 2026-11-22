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
use std::collections::HashMap;
use std::time::SystemTime;

use nativelink_config::schedulers::{CpuReservationPercentile, SizeClassSpec};
use nativelink_scheduler::resource_sketch::{
    Budget, ENTRY_BYTES, Entry, Family, Half, Hist, HistoryScope, Observation, Policy,
    ResourceSketch, SAMPLER_GENERATION, SketchKey, Snapshot,
};
use nativelink_util::action_messages::{ActionInfo, ActionUniqueKey, ActionUniqueQualifier};
use nativelink_util::common::DigestInfo;
use nativelink_util::digest_hasher::DigestHasherFunc;
use pretty_assertions::assert_eq;

fn class(name: &str, cpu_count: u64, memory_kb: u64, disk_kb: u64) -> SizeClassSpec {
    SizeClassSpec {
        name: name.to_string(),
        cpu_count,
        memory_kb,
        disk_kb,
    }
}

const fn policy(headroom_percent: u64, classes: Vec<SizeClassSpec>) -> Policy {
    Policy {
        headroom_percent,
        cpu_reservation_percentile: CpuReservationPercentile::new(95).expect("95 is valid"),
        classes,
        reserve_cpu: true,
        reserve_memory: true,
        reserve_disk: true,
    }
}

/// The clock the tests look up at: a week in, so every sample in the tests
/// (seconds 1 to a few thousand, or NOW itself) sits inside the window.
const NOW: u64 = 7 * 86_400;

#[test]
fn sampler_generation_identifies_peak_floor_format() {
    assert_eq!(SAMPLER_GENERATION, 4);
}

/// One shard, so the eviction order is the one LRU order of each family;
/// the scheduler uses `SHARDS`.
fn sketch(policy: Policy, budget: Budget) -> ResourceSketch {
    ResourceSketch::with_shards(policy, budget, Duration::from_hours(7 * 24), 1)
}

const fn small_budget() -> Budget {
    Budget {
        target: 3,
        mnemonic: 2,
        command: 2,
        probation: 4,
    }
}

fn target(name: &str, mnemonic: &str) -> SketchKey {
    SketchKey::for_attempt(Some(name), Some(mnemonic), "unused")
}

fn command(digest: &str) -> SketchKey {
    SketchKey::for_attempt(None, None, digest)
}

fn action(identity: &str) -> SketchKey {
    SketchKey::Action {
        identity: identity.into(),
    }
}

fn mnemonic(name: &str) -> SketchKey {
    SketchKey::Mnemonic {
        mnemonic: name.into(),
    }
}

#[test]
fn attempt_without_target_uses_command_even_with_mnemonic() {
    assert_eq!(
        SketchKey::for_attempt(None, Some("Compile"), "digest"),
        command("digest")
    );
    assert_eq!(
        SketchKey::for_attempt(Some(""), Some("Compile"), "digest"),
        command("digest")
    );
}

fn memory(peak_kb: u64, now_s: u64) -> Observation {
    Observation {
        peak_memory_kb: peak_kb,
        completed: true,
        now_s,
        ..Observation::default()
    }
}

/// The rollup's `hist_bucket`: 0 under 1000, then floor(log2(v / 1000)) + 1.
#[test]
fn histogram_buckets_follow_the_rollup() {
    for (value, bucket) in [
        (0u64, 0usize),
        (999, 0),
        (1000, 1),
        (1999, 1),
        (2000, 2),
        (3999, 2),
        (4000, 3),
        (1_048_576_000, 21),
        (u64::MAX, 24),
    ] {
        assert_eq!(Hist::bucket(value), bucket, "{value}");
    }
}

/// The p95 is the upper bound of the bucket the 95th sample falls in.
#[test]
fn p95_is_the_upper_bound_of_the_95th_samples_bucket() {
    let mut hist = Hist::default();
    assert_eq!(hist.p95_upper(), 0, "empty");
    hist.add(1500);
    assert_eq!(hist.p95_upper(), 2000, "one sample in [1000, 2000)");
    // 95 small samples and 5 large: the 95th of 100 is still small.
    let mut hist = Hist::default();
    for _ in 0..95 {
        hist.add(1500);
    }
    for _ in 0..5 {
        hist.add(100_000);
    }
    assert_eq!(hist.p95_upper(), 2000);
    hist.add(100_000);
    assert_eq!(hist.p95_upper(), 128_000, "the 96th of 101 is large");
}

#[test]
fn p95_and_p100_select_different_buckets() {
    let mut previous = Hist::default();
    let mut current = Hist::default();
    for _ in 0..20 {
        previous.add(1500);
    }
    current.add(100_000);
    assert_eq!(previous.p95_upper_with(&current), 2000);
    assert_eq!(
        previous.percentile_upper_with(&current, CpuReservationPercentile::new(100).unwrap()),
        128_000
    );
}

#[test]
fn cpu_percentile_uses_both_windows_and_preserves_other_dimensions() {
    let mut median_policy = policy(0, vec![]);
    median_policy.cpu_reservation_percentile = CpuReservationPercentile::new(50).unwrap();
    let median = sketch(median_policy, small_budget());
    let p95 = sketch(policy(0, vec![]), small_budget());
    let mut max_policy = policy(0, vec![]);
    max_policy.cpu_reservation_percentile = CpuReservationPercentile::new(100).unwrap();
    let maximum = sketch(max_policy, small_budget());
    let key = action("bursty");
    let boundary = NOW;
    for i in 0..20 {
        let observation = Observation {
            cpu_millicores: Some(if i < 18 { 1000 } else { 12_000 }),
            peak_memory_kb: if i < 18 { 1000 } else { 12_000 },
            peak_disk_kb: if i < 18 { 1000 } else { 12_000 },
            killed_peak_memory_kb: (i == 19).then_some(15_000),
            killed_peak_disk_kb: (i == 19).then_some(17_000),
            killed: i == 19,
            completed: i != 19,
            now_s: if i < 10 { boundary - 1 } else { boundary },
        };
        median.observe(key.clone(), &observation);
        p95.observe(key.clone(), &observation);
        maximum.observe(key.clone(), &observation);
    }
    let entry = median.lookup(&key, boundary).unwrap();
    assert!(entry.previous.cpu.total() > 0);
    assert!(entry.current.cpu.total() > 0);
    let shaped = median.shape(&entry, HistoryScope::Action);
    assert_eq!(shaped.cpu_count, 2000);
    assert_eq!(shaped.memory_kb, 15_000);
    assert_eq!(shaped.disk_kb, 17_000);
    assert_eq!(p95.shape(&entry, HistoryScope::Action).cpu_count, 12_000);
    assert_eq!(
        maximum.shape(&entry, HistoryScope::Action).cpu_count,
        12_000
    );
    assert_eq!(
        p95.shape(&entry, HistoryScope::Action).memory_kb,
        shaped.memory_kb
    );
    assert_eq!(
        maximum.shape(&entry, HistoryScope::Action).disk_kb,
        shaped.disk_kb
    );
}

#[test]
fn dimensions_share_shaping_contract_with_distinct_statistics() {
    for (cpu, memory_enabled, disk, expected) in [
        (true, true, true, (2_500, 10_002, 2_500)),
        (true, false, false, (2_500, 0, 0)),
        (false, true, false, (0, 10_002, 0)),
        (false, false, true, (0, 0, 2_500)),
        (false, false, false, (0, 0, 0)),
    ] {
        let mut configured = policy(25, vec![]);
        configured.cpu_reservation_percentile = CpuReservationPercentile::new(50).unwrap();
        configured.reserve_cpu = cpu;
        configured.reserve_memory = memory_enabled;
        configured.reserve_disk = disk;
        let sketch = sketch(configured, small_budget());
        let key = action("dimensions");
        for i in 0..20 {
            let value = if i == 19 { 8_001 } else { 1_000 };
            sketch.observe(
                key.clone(),
                &Observation {
                    cpu_millicores: Some(value),
                    peak_memory_kb: value,
                    peak_disk_kb: value,
                    now_s: NOW,
                    ..Observation::default()
                },
            );
        }
        let shaped = sketch.shape(&sketch.lookup(&key, NOW).unwrap(), HistoryScope::Action);
        assert_eq!(
            (shaped.cpu_count, shaped.memory_kb, shaped.disk_kb),
            expected
        );
        let aggregate = sketch.shape(&sketch.lookup(&key, NOW).unwrap(), HistoryScope::Aggregate);
        assert_eq!(aggregate.cpu_count, shaped.cpu_count);
        assert_eq!(aggregate.disk_kb, shaped.disk_kb);
        assert_eq!(aggregate.memory_kb, if memory_enabled { 2_500 } else { 0 });
    }
}

#[test]
fn rare_memory_peak_survives_rotation_and_snapshot_until_expiry() {
    let sketch = sketch(policy(20, vec![]), small_budget());
    let key = action("rare-peak");
    sketch.observe(key.clone(), &memory(8_000_001, NOW - 1));
    for _ in 0..100 {
        sketch.observe(key.clone(), &memory(1_000, NOW));
    }
    let entry = sketch.lookup(&key, NOW).unwrap();
    assert_eq!(entry.memory_p95_upper(), 2_000);
    assert_eq!(entry.previous.max_memory_kb, 8_000_001);
    assert_eq!(entry.current.max_memory_kb, 1_000);
    assert_eq!(
        sketch.shape(&entry, HistoryScope::Action).memory_kb,
        9_600_002
    );

    let bytes = sketch.encode_snapshot(NOW);
    let restored = self::sketch(policy(20, vec![]), small_budget());
    restored
        .load(Snapshot::decode(&bytes, usize::MAX).unwrap(), NOW)
        .unwrap();
    let loaded = restored.lookup(&key, NOW).unwrap();
    assert_eq!(loaded, entry);
    assert_eq!(
        restored.shape(&loaded, HistoryScope::Action).memory_kb,
        9_600_002
    );

    let expiry = NOW + 7 * 86_400 / 2;
    restored.age_out(expiry);
    let expired = restored.lookup(&key, expiry).unwrap();
    assert_eq!(expired.max_memory_kb(), 1_000);
    assert_eq!(
        restored.shape(&expired, HistoryScope::Action).memory_kb,
        1_200
    );
}

#[test]
fn class_fallback_never_undersizes_measured_memory() {
    for (class_memory, expected_class, expected_memory) in [
        (8_000, None, 10_002),
        (9_000, Some("top"), 9_000),
        (11_000, Some("top"), 11_000),
    ] {
        let sketch = sketch(
            policy(25, vec![class("top", 1_000, class_memory, 0)]),
            small_budget(),
        );
        let key = action("class-peak");
        sketch.observe(
            key.clone(),
            &Observation {
                cpu_millicores: Some(4_000),
                peak_memory_kb: 8_001,
                now_s: NOW,
                ..Observation::default()
            },
        );
        let shaped = sketch.shape(&sketch.lookup(&key, NOW).unwrap(), HistoryScope::Action);
        assert_eq!(shaped.class.as_deref(), expected_class);
        assert_eq!(shaped.memory_kb, expected_memory);
        let aggregate = sketch.shape(&sketch.lookup(&key, NOW).unwrap(), HistoryScope::Aggregate);
        assert_eq!(aggregate.class.as_deref(), Some("top"));
        assert_eq!(aggregate.memory_kb, class_memory);
    }
}

#[test]
fn class_fallback_preserves_action_kill_headroom_but_not_aggregate_kill_floors() {
    for (class_memory, expected_class, expected_memory) in
        [(9_000, None, 10_000), (10_000, Some("top"), 10_000)]
    {
        let sketch = sketch(
            policy(25, vec![class("top", 1_000, class_memory, 0)]),
            small_budget(),
        );
        let key = action("kill-class-floor");
        sketch.observe(
            key.clone(),
            &Observation {
                peak_memory_kb: 1_000,
                killed_peak_memory_kb: Some(8_000),
                killed: true,
                now_s: NOW,
                ..Observation::default()
            },
        );
        let entry = sketch.lookup(&key, NOW).unwrap();
        let shaped = sketch.shape(&entry, HistoryScope::Action);
        assert_eq!(shaped.class.as_deref(), expected_class);
        assert_eq!(shaped.memory_kb, expected_memory);
        let aggregate = sketch.shape(&entry, HistoryScope::Aggregate);
        assert_eq!(aggregate.class.as_deref(), Some("top"));
        assert_eq!(aggregate.memory_kb, class_memory);
    }
}

#[test]
fn cpu_percentile_caps_steady_samples_and_handles_zero_and_missing() {
    let mut configured = policy(20, vec![]);
    configured.cpu_reservation_percentile = CpuReservationPercentile::new(50).unwrap();
    let sketch = sketch(configured, small_budget());
    for (name, cpu, expected) in [
        ("//steady", Some(1500), 1800),
        ("//zero", Some(0), 0),
        ("//missing", None, 0),
    ] {
        let key = target(name, "X");
        sketch.observe(
            key.clone(),
            &Observation {
                cpu_millicores: cpu,
                peak_memory_kb: 1000,
                now_s: NOW,
                ..Observation::default()
            },
        );
        assert_eq!(
            sketch
                .shape(&sketch.lookup(&key, NOW).unwrap(), HistoryScope::Aggregate)
                .cpu_count,
            expected
        );
    }
}

#[test]
fn snapshot_reinterprets_cpu_percentile_without_changing_snapshot() {
    let mut median_policy = policy(0, vec![]);
    median_policy.cpu_reservation_percentile = CpuReservationPercentile::new(50).unwrap();
    let median = sketch(median_policy, small_budget());
    let key = target("//snapshot", "X");
    for i in 0..20 {
        median.observe(
            key.clone(),
            &Observation {
                cpu_millicores: Some(if i < 18 { 1000 } else { 12_000 }),
                now_s: NOW,
                ..Observation::default()
            },
        );
    }
    let bytes = median.encode_snapshot(NOW);
    let restored = sketch(policy(0, vec![]), small_budget());
    restored
        .load(Snapshot::decode(&bytes, usize::MAX).unwrap(), NOW)
        .unwrap();
    let original = median.lookup(&key, NOW).unwrap();
    let loaded = restored.lookup(&key, NOW).unwrap();
    assert_eq!(original, loaded);
    assert_eq!(
        median.shape(&original, HistoryScope::Aggregate).cpu_count,
        2000
    );
    assert_eq!(
        restored.shape(&loaded, HistoryScope::Aggregate).cpu_count,
        12_000
    );
    assert_eq!(restored.encode_snapshot(NOW), bytes);
}

/// A steady maximum receives headroom even when its histogram bucket is wider.
#[test]
fn shape_caps_the_bucket_by_the_exact_maximum() {
    let sketch = sketch(policy(20, vec![]), small_budget());
    let key = action("maximum");
    sketch.observe(key.clone(), &memory(3_000_000, 1));
    let shaped = sketch.shape(&sketch.lookup(&key, NOW).unwrap(), HistoryScope::Action);
    assert_eq!(shaped.memory_kb, 3_600_000);
    assert_eq!(shaped.cpu_count, 0, "no cpu measured");
    assert!(shaped.class.is_none());
    // A thousand samples at 3M: the bucket's bound is 4M, the max 3M.
    for _ in 0..1000 {
        sketch.observe(key.clone(), &memory(3_000_000, 1));
    }
    assert_eq!(
        sketch
            .shape(&sketch.lookup(&key, NOW).unwrap(), HistoryScope::Action)
            .memory_kb,
        3_600_000
    );
    // One at 3.9M moves the max, not the bucket.
    sketch.observe(key.clone(), &memory(3_900_000, 1));
    assert_eq!(
        sketch
            .shape(&sketch.lookup(&key, NOW).unwrap(), HistoryScope::Action)
            .memory_kb,
        4_680_000
    );
}

/// Killed peaks shape the next reservation, including class selection.
#[test]
fn shape_sits_above_the_kill() {
    let ladder = vec![
        class("s", 1000, 2_000_000, 0),
        class("m", 2000, 6_000_000, 0),
        class("l", 4000, 12_000_000, 0),
    ];
    let sketch = sketch(policy(25, ladder), small_budget());
    let key = action("kill-class");
    sketch.observe(
        key.clone(),
        &Observation {
            peak_memory_kb: 1_500_000,
            killed_peak_memory_kb: Some(1_500_000),
            killed: true,
            now_s: 1,
            ..Observation::default()
        },
    );
    let shaped = sketch.shape(&sketch.lookup(&key, NOW).unwrap(), HistoryScope::Action);
    assert_eq!(shaped.class.as_deref(), Some("s"));
    assert_eq!(shaped.memory_kb, 2_000_000);
    sketch.observe(
        key.clone(),
        &Observation {
            peak_memory_kb: 11_000_000,
            killed_peak_memory_kb: Some(11_000_000),
            killed: true,
            now_s: 2,
            ..Observation::default()
        },
    );
    let shaped = sketch.shape(&sketch.lookup(&key, NOW).unwrap(), HistoryScope::Action);
    assert!(shaped.class.is_none(), "{shaped:?}");
    assert_eq!(shaped.memory_kb, 13_750_000);
}

#[test]
fn killed_peaks_override_p95_for_both_resources_and_expire() {
    let sketch = sketch(policy(25, vec![]), small_budget());
    let key = action("kill-peak");
    let day = 86_400;
    let now = 20 * day;
    for _ in 0..30 {
        sketch.observe(
            key.clone(),
            &Observation {
                peak_memory_kb: 1_000,
                peak_disk_kb: 2_000,
                now_s: now,
                ..Observation::default()
            },
        );
    }
    sketch.observe(
        key.clone(),
        &Observation {
            peak_memory_kb: 8_001,
            peak_disk_kb: 12_001,
            killed_peak_memory_kb: Some(8_001),
            killed_peak_disk_kb: Some(12_001),
            killed: true,
            now_s: now,
            ..Observation::default()
        },
    );
    let shaped = sketch.shape(&sketch.lookup(&key, now).unwrap(), HistoryScope::Action);
    assert_eq!(shaped.memory_kb, 10_002);
    assert_eq!(shaped.disk_kb, 15_002);
    sketch.observe(
        key.clone(),
        &Observation {
            peak_memory_kb: 1_000,
            peak_disk_kb: 2_000,
            now_s: now + 6 * day,
            ..Observation::default()
        },
    );
    let entry = sketch.lookup(&key, now + 8 * day).unwrap();
    assert_eq!(entry.killed_peak_memory_kb, 0);
    assert_eq!(entry.killed_peak_disk_kb, 0);
    assert_eq!(sketch.shape(&entry, HistoryScope::Action).memory_kb, 1_250);
    assert_eq!(sketch.shape(&entry, HistoryScope::Action).disk_kb, 2_500);
}

#[test]
fn clear_then_load_restores_entries_and_clears_probation() {
    let sketch = sketch(policy(17, vec![]), small_budget());
    let key = target("//saved", "Compile");
    sketch.observe(key.clone(), &memory(4_321, NOW));
    let snapshot = sketch.snapshot(NOW);
    sketch.observe(command("probation"), &memory(2_345, NOW));
    let samples = sketch.stats().samples;
    sketch.clear();
    assert_eq!(sketch.stats().entries, [0, 0, 0]);
    assert_eq!(sketch.stats().probation, 0);
    assert_eq!(sketch.stats().samples, samples);
    assert_eq!(sketch.load(snapshot, NOW).unwrap(), 2);
    assert_eq!(
        sketch
            .shape(&sketch.lookup(&key, NOW).unwrap(), HistoryScope::Aggregate)
            .memory_kb,
        5_056
    );
    assert!(!sketch.observe(command("probation"), &memory(2_345, NOW)));
}

/// A dimension the ladder does not reserve is dropped before anything is
/// decided from it: a disk-only measurement is no hint.
#[test]
fn shape_drops_a_dimension_the_fleet_does_not_reserve() {
    let mut policy = policy(0, vec![class("s", 1000, 2_000_000, 0)]);
    policy.reserve_disk = false;
    let sketch = sketch(policy, small_budget());
    let key = target("//a", "Link");
    sketch.observe(
        key.clone(),
        &Observation {
            peak_disk_kb: 9000,
            now_s: 1,
            ..Observation::default()
        },
    );
    let shaped = sketch.shape(&sketch.lookup(&key, NOW).unwrap(), HistoryScope::Aggregate);
    assert!(shaped.is_empty(), "{shaped:?}");
    sketch.observe(key.clone(), &memory(500_000, 2));
    let shaped = sketch.shape(&sketch.lookup(&key, NOW).unwrap(), HistoryScope::Aggregate);
    assert_eq!(shaped.class.as_deref(), Some("s"));
    assert_eq!(shaped.disk_kb, 0);
}

/// A command aggregate earns an entry on its second sighting; kills are rejected.
#[test]
fn a_command_key_is_on_probation_until_seen_twice() {
    let sketch = sketch(policy(0, vec![]), small_budget());
    assert!(!sketch.observe(command("aa-1"), &memory(1000, 1)));
    assert!(sketch.lookup(&command("aa-1"), NOW).is_none());
    assert_eq!(sketch.stats().probation, 1);
    assert!(sketch.observe(command("aa-1"), &memory(1000, 2)));
    assert_eq!(sketch.lookup(&command("aa-1"), NOW).unwrap().samples, 1);
    assert_eq!(sketch.stats().probation, 0);
    assert!(!sketch.observe(
        command("bb-1"),
        &Observation {
            peak_memory_kb: 1000,
            killed: true,
            now_s: 3,
            ..Observation::default()
        }
    ));
    assert!(
        sketch.lookup(&command("bb-1"), NOW).is_none(),
        "a kill cannot teach an aggregate"
    );
    // Target keys are admitted on first sight.
    assert!(sketch.observe(target("//a", "X"), &memory(1000, 4)));
}

/// A full family evicts its least recently seen key; a lookup counts as
/// use; another family is untouched.
#[test]
fn a_full_family_evicts_the_least_recently_seen() {
    let sketch = sketch(policy(0, vec![]), small_budget());
    for name in ["//a", "//b", "//c"] {
        sketch.observe(target(name, "X"), &memory(1000, 1));
    }
    assert!(
        sketch.lookup(&target("//a", "X"), NOW).is_some(),
        "promotes //a"
    );
    sketch.observe(target("//d", "X"), &memory(1000, 2));
    assert!(
        sketch.lookup(&target("//b", "X"), NOW).is_none(),
        "//b was the oldest"
    );
    assert!(sketch.lookup(&target("//a", "X"), NOW).is_some());
    let stats = sketch.stats();
    // The four attempts named mnemonic X, so the mnemonic family holds it.
    assert_eq!(stats.entries, [3, 1, 0]);
    assert_eq!(stats.evictions, [1, 0, 0]);
    assert_eq!(stats.samples, [4, 4, 0]);
}

/// A snapshot round-trips every field, drops keys outside the window, and
/// comes back in the same recency order.
#[test]
fn a_snapshot_round_trips_and_ages_out() {
    let sketch = sketch(policy(0, vec![]), small_budget());
    let old = 1_000_000u64;
    let now = old + 10 * 86_400;
    sketch.observe(target("//old", "X"), &memory(1000, old));
    sketch.observe(
        action("new"),
        &Observation {
            peak_memory_kb: 123_456,
            cpu_millicores: Some(2500),
            peak_disk_kb: 77,
            killed_peak_memory_kb: Some(123_456),
            killed_peak_disk_kb: Some(77),
            killed: true,
            completed: false,
            now_s: now,
        },
    );
    sketch.observe(command("cc-9"), &memory(5000, now));
    sketch.observe(command("cc-9"), &memory(6000, now));
    let snapshot = sketch.snapshot(now);
    assert_eq!(
        snapshot.entries.len(),
        2,
        "//old and the killed attempt's mnemonic are absent"
    );
    assert_eq!(sketch.stats().entries, [1, 1, 2], "nothing dropped yet");
    assert_eq!(sketch.age_out(now), 2, "//old and its mnemonic X dropped");
    assert_eq!(sketch.stats().entries, [0, 0, 2]);
    assert_eq!(sketch.snapshot(now), snapshot);
    let bytes = snapshot.encode();
    let decoded = Snapshot::decode(&bytes, usize::MAX).unwrap();
    assert_eq!(decoded, snapshot);
    let (_, entry) = decoded
        .entries
        .iter()
        .find(|(key, _)| *key == action("new"))
        .unwrap();
    assert_eq!(entry.samples, 1);
    assert_eq!(entry.current.max_memory_kb, 123_456);
    assert_eq!(entry.current.max_cpu_millicores, 2500);
    assert_eq!(entry.current.max_disk_kb, 77);
    assert_eq!(entry.previous, Half::default());
    assert_eq!(entry.killed_peak_memory_kb, 123_456);
    assert_eq!(entry.killed_memory_s, now);
    assert_eq!(entry.killed_peak_disk_kb, 77);
    assert_eq!(entry.killed_disk_s, now);
    assert_eq!(entry.last_seen_s, now);
    assert_eq!(entry.period, now / (7 * 86_400 / 2));
    assert!(!entry.seeded);

    let fresh = self::sketch(policy(0, vec![]), small_budget());
    assert_eq!(fresh.load(decoded.clone(), now).unwrap(), 2);
    // The command key's first sighting was probation; one sample counted.
    assert_eq!(fresh.lookup(&command("cc-9"), now).unwrap().samples, 1);
    // Replaying the same owned snapshot does not count samples again.
    assert_eq!(fresh.load(decoded, now).unwrap(), 0);
    assert_eq!(fresh.lookup(&command("cc-9"), now).unwrap().samples, 1);
    assert_eq!(
        fresh.lookup(&action("new"), now).unwrap().memory_samples(),
        1
    );

    // Truncated is an error, not a partial load.
    assert!(Snapshot::decode(&bytes[..bytes.len() - 3], usize::MAX).is_err());
    // Another sampler generation is refused.
    let mut other = sketch.snapshot(now);
    other.sampler_generation = SAMPLER_GENERATION + 1;
    let fresh = self::sketch(policy(0, vec![]), small_budget());
    assert!(fresh.load(other, now).is_err());
    assert_eq!(fresh.stats().entries, [0, 0, 0]);
}

/// A seed stands until the first real sample of its key, which replaces it
/// whole (an oversized seed must neither cap nor lift what is measured),
/// and a seed never replaces a measurement.
#[test]
fn a_seed_yields_to_the_first_sample_and_never_replaces_one() {
    let sketch = sketch(policy(0, vec![]), small_budget());
    let key = target("//a", "X");
    assert!(sketch.seed(key.clone(), Entry::seeded(1000, 500_000_000, 0, 1, 1)));
    let entry = sketch.lookup(&key, NOW).unwrap();
    assert!(entry.seeded);
    assert_eq!(
        sketch.shape(&entry, HistoryScope::Aggregate).memory_kb,
        500_000_000
    );
    assert_eq!(
        sketch.shape(&entry, HistoryScope::Aggregate).cpu_count,
        1000
    );
    // A seed does not age out: it stands until its first sample.
    assert_eq!(sketch.age_out(NOW + 30 * 86_400), 0);
    sketch.observe(key.clone(), &memory(2_000_000, 2));
    let entry = sketch.lookup(&key, NOW).unwrap();
    assert!(!entry.seeded);
    assert_eq!(entry.samples, 1, "the seed is gone, not merged");
    assert_eq!(
        entry.max_memory_kb(),
        2_000_000,
        "the seed's maximum with it"
    );
    assert_eq!(
        sketch.shape(&entry, HistoryScope::Aggregate).memory_kb,
        2_000_000
    );
    assert_eq!(
        sketch.shape(&entry, HistoryScope::Aggregate).cpu_count,
        0,
        "the seed's cpu with it"
    );
    assert!(
        !sketch.seed(key, Entry::seeded(1, 1, 1, 1, 1)),
        "a measured key is never reseeded"
    );
}

/// The budget divides into families and the admin listing shapes every
/// entry, most recently used first.
#[test]
fn the_budget_and_the_listing() {
    let budget = Budget::from_bytes(128 << 20);
    assert!(budget.target > budget.mnemonic);
    assert_eq!(budget.mnemonic, budget.command);
    let empty = ResourceSketch::new(policy(0, vec![]), budget, Duration::from_hours(1));
    assert!(empty.charged_bytes() + empty.snapshot_byte_limit() * 3 <= 128 << 20);
    let sketch = sketch(
        policy(0, vec![class("s", 1000, 2_000_000, 0)]),
        small_budget(),
    );
    sketch.observe(target("//a", "X"), &memory(1000, 1));
    sketch.observe(mnemonic("Link"), &memory(2000, 2));
    let rows = sketch.hints(None, 10);
    // The target, its mnemonic X, and Link; most recently seen first.
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].key, mnemonic("Link"));
    let a = rows
        .iter()
        .find(|row| row.key == target("//a", "X"))
        .unwrap();
    assert_eq!(a.shaped.class.as_deref(), Some("s"));
    assert_eq!(a.shaped.memory_kb, 2_000_000);
    assert_eq!(sketch.hints(Some(Family::Mnemonic), 10).len(), 2);
    assert_eq!(sketch.hints(Some(Family::Command), 10).len(), 0);
    assert_eq!(sketch.hints(None, 1).len(), 1);
    // The scheduler's own shard count spreads a family's capacity.
    let sharded = ResourceSketch::new(policy(0, vec![]), small_budget(), Duration::from_hours(1));
    assert_eq!(sharded.stats().capacity, [3, 2, 2]);
}

/// A sample counts for between half a window and a whole one, never
/// longer: what a key used last week stops shaping it however often it
/// runs, a kill floor expires with the window, and a lookup past the window
/// finds nothing.
#[test]
fn samples_leave_with_the_window_and_the_kill_floor_expires() {
    let window = Duration::from_hours(7 * 24);
    let sketch = ResourceSketch::with_shards(policy(0, vec![]), small_budget(), window, 1);
    let key = action("window");
    let day = 86_400u64;
    // Day 100 is half-window 28 (3.5 days each).
    let start = 100 * day;
    for _ in 0..100 {
        sketch.observe(key.clone(), &memory(7_000_000, start));
    }
    sketch.observe(
        key.clone(),
        &Observation {
            peak_memory_kb: 7_500_000,
            killed_peak_memory_kb: Some(7_500_000),
            killed: true,
            now_s: start,
            ..Observation::default()
        },
    );
    let entry = sketch.lookup(&key, start).unwrap();
    assert_eq!(
        sketch.shape(&entry, HistoryScope::Action).memory_kb,
        7_500_000
    );
    // Three days on, the next half-window: a few small samples, and the
    // big ones still count.
    for i in 0..3u64 {
        sketch.observe(key.clone(), &memory(1_000_000, start + 3 * day + i));
    }
    let entry = sketch.lookup(&key, start + 3 * day).unwrap();
    assert_eq!(entry.current.memory.total(), 3);
    assert_eq!(entry.previous.memory.total(), 101);
    assert_eq!(
        sketch.shape(&entry, HistoryScope::Action).memory_kb,
        7_500_000,
        "the retained maximum still holds"
    );
    // Eight days on: two half-windows past the big samples and a window
    // past the kill. The tick ages the key; the small samples are what is
    // left, and the maximum left with the big ones.
    assert_eq!(sketch.age_out(start + 8 * day), 0);
    let entry = sketch.lookup(&key, start + 8 * day).unwrap();
    assert_eq!(entry.killed_peak_memory_kb, 0, "the floor expired");
    assert_eq!(entry.previous.memory.total(), 3);
    assert_eq!(entry.current, Half::default());
    assert_eq!(entry.max_memory_kb(), 1_000_000);
    assert_eq!(
        sketch.shape(&entry, HistoryScope::Action).memory_kb,
        1_000_000,
        "capped by the exact maximum"
    );
    // A sample a week old is gone whatever came after it: a key seen once
    // a week sizes from its latest sample.
    let rare = target("//rare", "X");
    sketch.observe(rare.clone(), &memory(9_000_000, start));
    sketch.observe(rare.clone(), &memory(1_000, start + 7 * day + 1));
    assert_eq!(
        sketch
            .shape(
                &sketch.lookup(&rare, start + 7 * day + 1).unwrap(),
                HistoryScope::Aggregate
            )
            .memory_kb,
        1_000
    );
    // Past the window the key is dropped by a lookup, not sized from.
    assert!(sketch.lookup(&key, start + 16 * day).is_none());
    assert_eq!(sketch.stats().entries[0], 1, "only //rare is left");
}

/// An attempt that names a target and a mnemonic teaches the mnemonic
/// family too, so a target never seen is sized from its kind of action.
#[test]
fn a_named_attempt_teaches_its_mnemonic() {
    let sketch = sketch(policy(0, vec![]), small_budget());
    sketch.observe(target("//a", "Javac"), &memory(3_000_000, 1));
    sketch.observe(target("//b", "Javac"), &memory(1_000_000, 2));
    let javac = mnemonic("Javac");
    let entry = sketch.lookup(&javac, NOW).unwrap();
    assert_eq!(entry.samples, 2);
    assert_eq!(entry.max_memory_kb(), 3_000_000);
    assert_eq!(sketch.stats().samples, [2, 2, 0]);
    // A key without a mnemonic teaches no mnemonic; a mnemonic-only key
    // counts once.
    sketch.observe(
        SketchKey::for_attempt(Some("//c"), None, "unused"),
        &memory(1_000, 3),
    );
    sketch.observe(javac.clone(), &memory(1_000, 4));
    assert_eq!(sketch.stats().samples, [3, 3, 0]);
    assert_eq!(sketch.lookup(&javac, NOW).unwrap().samples, 3);
}

#[test]
fn killed_action_does_not_pollute_mnemonic_and_success_clears_floors() {
    let sketch = sketch(policy(0, vec![]), small_budget());
    let key = action("large");
    let mnemonic = mnemonic("Compile");
    for _ in 0..20 {
        sketch.observe(
            target("//small", "Compile"),
            &Observation {
                cpu_millicores: Some(1_000),
                peak_memory_kb: 1_000,
                peak_disk_kb: 1_000,
                now_s: NOW,
                ..Observation::default()
            },
        );
    }
    let fallback = sketch.lookup(&mnemonic, NOW).unwrap();
    assert!(!sketch.observe(
        mnemonic.clone(),
        &Observation {
            peak_memory_kb: 16_000,
            killed: true,
            now_s: NOW,
            ..Observation::default()
        },
    ));
    sketch.observe(
        key.clone(),
        &Observation {
            cpu_millicores: Some(8_001),
            peak_memory_kb: 8_001,
            peak_disk_kb: 8_001,
            killed_peak_memory_kb: Some(16_000),
            killed_peak_disk_kb: Some(16_000),
            killed: true,
            completed: false,
            now_s: NOW,
        },
    );
    assert_eq!(sketch.lookup(&mnemonic, NOW).unwrap(), fallback);
    let killed = sketch.shape(&sketch.lookup(&key, NOW).unwrap(), HistoryScope::Action);
    assert_eq!(
        (killed.cpu_count, killed.memory_kb, killed.disk_kb),
        (8_001, 16_000, 16_000)
    );
    sketch.observe(
        key.clone(),
        &Observation {
            now_s: NOW,
            ..Observation::default()
        },
    );
    let unknown = sketch.lookup(&key, NOW).unwrap();
    assert_eq!(unknown.killed_peak_memory_kb, 16_000);
    assert_eq!(unknown.killed_peak_disk_kb, 16_000);

    for _ in 0..20 {
        sketch.observe(
            key.clone(),
            &Observation {
                cpu_millicores: Some(1_000),
                peak_memory_kb: 1_000,
                peak_disk_kb: 1_000,
                completed: true,
                now_s: NOW,
                ..Observation::default()
            },
        );
    }
    let entry = sketch.lookup(&key, NOW).unwrap();
    assert_eq!(entry.killed_peak_memory_kb, 0);
    assert_eq!(entry.killed_peak_disk_kb, 0);
    let shaped = sketch.shape(&entry, HistoryScope::Action);
    assert_eq!(
        (shaped.cpu_count, shaped.memory_kb, shaped.disk_kb),
        (2_000, 8_001, 2_000)
    );
    let fallback = sketch.shape(
        &sketch.lookup(&mnemonic, NOW).unwrap(),
        HistoryScope::Aggregate,
    );
    assert_eq!(
        (fallback.cpu_count, fallback.memory_kb, fallback.disk_kb),
        (1_000, 1_000, 1_000)
    );

    let expiry = NOW + 7 * 86_400;
    sketch.observe(key.clone(), &memory(1_000, expiry));
    assert_eq!(
        sketch
            .shape(&sketch.lookup(&key, expiry).unwrap(), HistoryScope::Action)
            .memory_kb,
        1_000
    );
}

#[test]
fn successful_action_clears_only_measured_floor_and_retains_peak() {
    let sketch = sketch(policy(0, vec![]), small_budget());
    let key = action("killed");
    let fallback = mnemonic("Compile");
    sketch.observe(fallback.clone(), &memory(1_000, NOW));
    let before = sketch.lookup(&fallback, NOW).unwrap();
    sketch.observe(
        key.clone(),
        &Observation {
            peak_memory_kb: 8_000,
            peak_disk_kb: 8_000,
            killed_peak_memory_kb: Some(16_000),
            killed_peak_disk_kb: Some(16_000),
            killed: true,
            now_s: NOW,
            ..Observation::default()
        },
    );
    assert_eq!(sketch.lookup(&fallback, NOW).unwrap(), before);
    sketch.observe(key.clone(), &memory(1_000, NOW));
    sketch.observe(fallback.clone(), &memory(1_000, NOW));
    let entry = sketch.lookup(&key, NOW).unwrap();
    assert_eq!(entry.killed_peak_memory_kb, 0);
    assert_eq!(entry.killed_peak_disk_kb, 16_000);
    assert_eq!(sketch.shape(&entry, HistoryScope::Action).memory_kb, 8_000);
    assert_eq!(
        sketch
            .shape(
                &sketch.lookup(&fallback, NOW).unwrap(),
                HistoryScope::Aggregate
            )
            .memory_kb,
        1_000
    );
}

#[test]
fn unmeasured_success_preserves_floors_until_each_dimension_proves_decay() {
    let sketch = sketch(policy(0, vec![]), small_budget());
    let key = action("fallback-floor");
    sketch.observe(
        key.clone(),
        &Observation {
            cpu_millicores: Some(8_001),
            peak_memory_kb: 8_001,
            peak_disk_kb: 8_001,
            now_s: NOW,
            ..Observation::default()
        },
    );
    // The kill had no measured peaks: these floors came from reservations.
    sketch.observe(
        key.clone(),
        &Observation {
            killed_peak_memory_kb: Some(16_000),
            killed_peak_disk_kb: Some(16_000),
            killed: true,
            now_s: NOW + 1,
            ..Observation::default()
        },
    );
    // A low measurement with an unknown result is not proof of success.
    sketch.observe(
        key.clone(),
        &Observation {
            peak_memory_kb: 1_000,
            peak_disk_kb: 1_000,
            now_s: NOW + 2,
            ..Observation::default()
        },
    );
    for _ in 0..20 {
        sketch.observe(
            key.clone(),
            &Observation {
                cpu_millicores: Some(1_000),
                completed: true,
                now_s: NOW + 3,
                ..Observation::default()
            },
        );
    }
    let entry = sketch.lookup(&key, NOW + 3).unwrap();
    assert_eq!(
        (entry.killed_peak_memory_kb, entry.killed_memory_s),
        (16_000, NOW + 1)
    );
    assert_eq!(
        (entry.killed_peak_disk_kb, entry.killed_disk_s),
        (16_000, NOW + 1)
    );
    let shaped = sketch.shape(&entry, HistoryScope::Action);
    assert_eq!(
        (shaped.cpu_count, shaped.memory_kb, shaped.disk_kb),
        (2_000, 16_000, 16_000)
    );

    sketch.observe(
        key.clone(),
        &Observation {
            peak_memory_kb: 1_000,
            completed: true,
            now_s: NOW + 4,
            ..Observation::default()
        },
    );
    let entry = sketch.lookup(&key, NOW + 4).unwrap();
    assert_eq!((entry.killed_peak_memory_kb, entry.killed_memory_s), (0, 0));
    assert_eq!(
        (entry.killed_peak_disk_kb, entry.killed_disk_s),
        (16_000, NOW + 1)
    );
    assert_eq!(sketch.shape(&entry, HistoryScope::Action).memory_kb, 8_001);
    assert_eq!(sketch.shape(&entry, HistoryScope::Action).disk_kb, 16_000);

    for _ in 0..20 {
        sketch.observe(
            key.clone(),
            &Observation {
                peak_disk_kb: 1_000,
                completed: true,
                now_s: NOW + 5,
                ..Observation::default()
            },
        );
    }
    let entry = sketch.lookup(&key, NOW + 5).unwrap();
    assert_eq!((entry.killed_peak_disk_kb, entry.killed_disk_s), (0, 0));
    let shaped = sketch.shape(&entry, HistoryScope::Action);
    // CPU and disk use their percentiles; memory retains the exact maximum.
    assert_eq!(
        (shaped.cpu_count, shaped.memory_kb, shaped.disk_kb),
        (2_000, 8_001, 2_000)
    );
}

#[test]
fn successful_peak_at_or_above_floor_does_not_clear_it() {
    let sketch = sketch(policy(0, vec![]), small_budget());
    let key = action("floor-boundary");
    sketch.observe(
        key.clone(),
        &Observation {
            killed_peak_memory_kb: Some(16_000),
            killed_peak_disk_kb: Some(16_000),
            killed: true,
            now_s: NOW,
            ..Observation::default()
        },
    );
    sketch.observe(
        key.clone(),
        &Observation {
            peak_memory_kb: 16_000,
            peak_disk_kb: 16_001,
            completed: true,
            now_s: NOW + 1,
            ..Observation::default()
        },
    );
    let entry = sketch.lookup(&key, NOW + 1).unwrap();
    assert_eq!(
        (entry.killed_peak_memory_kb, entry.killed_memory_s),
        (16_000, NOW)
    );
    assert_eq!(
        (entry.killed_peak_disk_kb, entry.killed_disk_s),
        (16_000, NOW)
    );
}

#[test]
fn legacy_snapshot_restores_narrow_keys_but_discards_mnemonic_history() {
    let sketch = sketch(policy(0, vec![]), small_budget());
    let target = target("//legacy", "Compile");
    let mnemonic = mnemonic("Compile");
    let command = command("legacy");
    sketch.observe(target.clone(), &memory(8_001, NOW));
    sketch.observe(command.clone(), &memory(4_000, NOW));
    sketch.observe(command.clone(), &memory(4_000, NOW));
    let mut legacy = sketch.snapshot(NOW);
    legacy.sampler_generation = 2;
    let bytes = legacy.encode();
    assert_eq!(Snapshot::compatible_written_at(&bytes).unwrap(), NOW);
    let restored = self::sketch(policy(0, vec![]), small_budget());
    assert_eq!(
        restored
            .load(Snapshot::decode(&bytes, usize::MAX).unwrap(), NOW)
            .unwrap(),
        2
    );
    assert_eq!(restored.lookup(&mnemonic, NOW), None);
    assert_eq!(
        restored
            .shape(
                &restored.lookup(&target, NOW).unwrap(),
                HistoryScope::Aggregate
            )
            .memory_kb,
        8_001
    );
    assert_eq!(
        restored
            .shape(
                &restored.lookup(&command, NOW).unwrap(),
                HistoryScope::Aggregate
            )
            .memory_kb,
        4_000
    );
    assert_eq!(
        restored.snapshot(NOW).sampler_generation,
        SAMPLER_GENERATION
    );
}

/// Independent u16 saturation previously turned a 0.4% tail into the p95.
#[test]
fn busy_histogram_preserves_the_distribution_and_snapshot_counts() {
    let sketch = sketch(policy(0, vec![]), small_budget());
    let key = action("busy");
    for _ in 0..1_000_000 {
        sketch.observe(key.clone(), &memory(1000, NOW));
    }
    for _ in 0..4000 {
        sketch.observe(key.clone(), &memory(8_000_000, NOW));
    }
    let entry = sketch.lookup(&key, NOW).unwrap();
    assert_eq!(entry.memory_samples(), 1_004_000);
    assert_eq!(entry.memory_p95_upper(), 2000);
    assert_eq!(
        sketch.shape(&entry, HistoryScope::Action).memory_kb,
        8_000_000
    );
    let bytes = sketch.encode_snapshot(NOW);
    let restored = self::sketch(policy(0, vec![]), small_budget());
    restored
        .load(Snapshot::decode(&bytes, usize::MAX).unwrap(), NOW)
        .unwrap();
    assert_eq!(restored.lookup(&key, NOW).unwrap(), entry);
}

/// Charge long keys and probation as well as entries. Encoding must stay
/// within its preallocated allowance rather than cloning the full sketch.
#[test]
fn memory_budget_covers_long_keys_probation_and_snapshot_buffer() {
    let limit = 1 << 20;
    let sketch = ResourceSketch::new(
        policy(0, vec![]),
        Budget::from_bytes(limit),
        Duration::from_hours(24),
    );
    assert!(usize::try_from(ENTRY_BYTES).unwrap() > size_of::<Entry>() + size_of::<SketchKey>());
    for i in 0..5000 {
        sketch.observe(
            target(&format!("//{}:{i}", "x".repeat(2048)), "X"),
            &memory(1000, NOW),
        );
        sketch.observe(
            command(&format!("{i}-{}", "c".repeat(64))),
            &memory(1000, NOW),
        );
    }
    assert!(sketch.stats().evictions[0] > 0);
    assert!(sketch.stats().probation > 0);
    let bytes = sketch.encode_snapshot(NOW);
    assert!(bytes.len() <= bytes.capacity());
    assert_eq!(bytes.capacity() as u64, sketch.snapshot_byte_limit());
    assert!(sketch.charged_bytes() + 3 * bytes.capacity() as u64 <= limit);
    assert_eq!(
        Snapshot::decode(&bytes, usize::MAX).unwrap(),
        sketch.snapshot(NOW)
    );
    let too_long = target(&"x".repeat(usize::try_from(limit).unwrap()), "X");
    assert!(!sketch.observe(too_long.clone(), &memory(1000, NOW)));
    assert!(sketch.lookup(&too_long, NOW).is_none());
    assert!(sketch.charged_bytes() + 3 * bytes.capacity() as u64 <= limit);
}

/// Exercise the largest serialized histograms rather than only sparse ones.
#[test]
fn dense_snapshot_fits_the_reserved_buffer() {
    let sketch = sketch(policy(0, vec![]), small_budget());
    let key = target("//dense", "X");
    let mut entry = Entry::default();
    entry.current.memory = Hist([u64::MAX; 25]);
    entry.current.cpu = entry.current.memory;
    entry.current.disk = entry.current.memory;
    entry.previous = entry.current;
    entry.last_seen_s = NOW;
    assert_eq!(entry.memory_samples(), u64::MAX);
    assert_eq!(entry.memory_p95_upper(), 1000 << 23);
    assert!(sketch.seed(key, entry));
    let bytes = sketch.encode_snapshot(NOW);
    assert_eq!(bytes.capacity() as u64, sketch.snapshot_byte_limit());
    assert_eq!(
        Snapshot::decode(&bytes, usize::MAX).unwrap(),
        sketch.snapshot(NOW)
    );
}

/// Sparse encoding and an untrusted count must not allocate an unbounded
/// array of full histograms before the receiving sketch applies its budget.
#[test]
fn snapshot_decode_rejects_entry_counts_above_the_budget() {
    let sketch = sketch(policy(0, vec![]), small_budget());
    for i in 0..3 {
        sketch.observe(target(&format!("//target-{i}"), "X"), &memory(1000, NOW));
    }
    let mut bytes = sketch.encode_snapshot(NOW);
    assert_eq!(Snapshot::decode(&bytes, 4).unwrap().entries.len(), 4);
    assert!(Snapshot::decode(&bytes, 3).is_err());
    // The old narrow-counter format must not be interpreted as wide counters.
    bytes[4] = 2;
    assert!(Snapshot::decode(&bytes, 4).is_err());
    bytes[4] = 3;
    bytes[17..25].copy_from_slice(&u64::MAX.to_le_bytes());
    assert!(Snapshot::decode(&bytes, usize::MAX).is_err());
}

#[test]
fn action_identity_uses_only_instance_function_and_action_digest() {
    let key = ActionUniqueKey {
        execution_scope: None,
        instance_name: "instance".into(),
        digest_function: DigestHasherFunc::Sha256,
        digest: DigestInfo::new([1; 32], 10),
    };
    let mut info = ActionInfo {
        command_digest: DigestInfo::new([2; 32], 10),
        input_root_digest: DigestInfo::new([3; 32], 10),
        timeout: Duration::from_secs(1),
        platform_properties: HashMap::new(),
        memory_reservation: None,
        pre_fit_reservation: None,
        priority: 0,
        load_timestamp: SystemTime::UNIX_EPOCH,
        insert_timestamp: SystemTime::UNIX_EPOCH,
        unique_qualifier: ActionUniqueQualifier::Cacheable(key.clone()),
    };
    let original = SketchKey::for_action(&info);
    let mut changed = key.clone();
    changed.execution_scope = Some("another execution".into());
    info.unique_qualifier = ActionUniqueQualifier::Uncacheable(changed);
    info.command_digest = DigestInfo::new([4; 32], 10);
    info.platform_properties
        .insert("target".into(), "another".into());
    assert_eq!(SketchKey::for_action(&info), original);
    for changed in [
        ActionUniqueKey {
            digest: DigestInfo::new([5; 32], 10),
            ..key.clone()
        },
        ActionUniqueKey {
            instance_name: "other".into(),
            ..key.clone()
        },
        ActionUniqueKey {
            digest_function: DigestHasherFunc::Blake3,
            ..key
        },
    ] {
        info.unique_qualifier = ActionUniqueQualifier::Cacheable(changed);
        assert_ne!(SketchKey::for_action(&info), original);
    }
}

#[test]
fn action_first_sample_and_aggregate_scopes_are_separate() {
    let sketch = sketch(policy(0, vec![]), small_budget());
    let exact = action("same");
    let aggregate = command("same");
    assert_ne!(exact, aggregate);
    assert!(sketch.observe(exact.clone(), &memory(8_000_000, NOW)));
    assert!(!sketch.observe(aggregate.clone(), &memory(1_000, NOW)));
    assert!(sketch.observe(aggregate.clone(), &memory(1_000, NOW)));
    for _ in 0..100 {
        sketch.observe(exact.clone(), &memory(1_000, NOW));
    }
    let entry = sketch.lookup(&exact, NOW).unwrap();
    assert_eq!(
        sketch.shape(&entry, HistoryScope::Action).memory_kb,
        8_000_000
    );
    assert_eq!(
        sketch.shape(&entry, HistoryScope::Aggregate).memory_kb,
        2_000
    );
    let kill = Observation {
        killed: true,
        killed_peak_memory_kb: Some(16_000_000),
        killed_peak_disk_kb: Some(16_000_000),
        now_s: NOW,
        ..Observation::default()
    };
    assert!(!sketch.observe(aggregate.clone(), &kill));
    assert!(!sketch.observe(target("//other", "Compile"), &kill));
    assert!(!sketch.observe(mnemonic("Compile"), &kill));
    assert!(sketch.observe(exact.clone(), &kill));
    let entry = sketch.lookup(&exact, NOW).unwrap();
    let action_hint = sketch.shape(&entry, HistoryScope::Action);
    let aggregate_hint = sketch.shape(&entry, HistoryScope::Aggregate);
    assert_eq!(action_hint.memory_kb, 16_000_000);
    assert_eq!(action_hint.disk_kb, 16_000_000);
    assert_eq!(aggregate_hint.memory_kb, 2_000);
    assert_eq!(aggregate_hint.disk_kb, 0);
    let bytes = sketch.encode_snapshot(NOW);
    assert_eq!(bytes[4], 4);
    let restored = self::sketch(policy(0, vec![]), small_budget());
    restored
        .load(Snapshot::decode(&bytes, usize::MAX).unwrap(), NOW)
        .unwrap();
    assert_eq!(restored.lookup(&exact, NOW).unwrap(), entry);
    assert!(restored.lookup(&aggregate, NOW).is_some());
}

#[test]
fn legacy_versions_never_import_action_or_mnemonic_keys() {
    let source = sketch(policy(0, vec![]), small_budget());
    source.observe(action("legacy-invalid"), &memory(8_000, NOW));
    source.observe(target("//legacy", "Compile"), &memory(4_000, NOW));
    source.observe(command("legacy"), &memory(2_000, NOW));
    source.observe(command("legacy"), &memory(2_000, NOW));
    for generation in [2, 3] {
        let mut snapshot = source.snapshot(NOW);
        snapshot.sampler_generation = generation;
        let mut bytes = snapshot.encode();
        bytes[4] = 3;
        assert_eq!(Snapshot::compatible_written_at(&bytes).unwrap(), NOW);
        let restored = sketch(policy(0, vec![]), small_budget());
        assert_eq!(
            restored
                .load(Snapshot::decode(&bytes, usize::MAX).unwrap(), NOW)
                .unwrap(),
            2
        );
        assert!(restored.lookup(&action("legacy-invalid"), NOW).is_none());
        assert!(restored.lookup(&mnemonic("Compile"), NOW).is_none());
        assert!(
            restored
                .lookup(&target("//legacy", "Compile"), NOW)
                .is_some()
        );
        assert!(restored.lookup(&command("legacy"), NOW).is_some());
    }
}
