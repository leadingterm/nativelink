// Copyright 2024 The NativeLink Authors. All rights reserved.
// Licensed under the Functional Source License, Version 1.1, Apache 2.0 Future License.

use std::time::{Duration, Instant};

/// Parse the cumulative stall time in microseconds, preferring `some`.
pub(crate) fn psi_total(text: &str) -> Option<u64> {
    let total = |kind: &str| {
        text.lines().find_map(|line| {
            let mut fields = line.split_whitespace();
            if fields.next()? != kind {
                return None;
            }
            fields.find_map(|field| field.strip_prefix("total=")?.parse().ok())
        })
    };
    total("some").or_else(|| total("full"))
}

/// A continuous window of per-interval PSI stall rates.
#[derive(Debug)]
pub(crate) struct PsiWindow {
    threshold: u32,
    after: Duration,
    baseline: Option<(u64, Instant)>,
    since: Option<Instant>,
}

impl PsiWindow {
    pub(crate) const fn new(threshold: u32, after: Duration) -> Self {
        Self {
            threshold,
            after,
            baseline: None,
            since: None,
        }
    }

    pub(crate) fn reset(&mut self) {
        self.baseline = None;
        self.since = None;
    }

    pub(crate) fn observe(&mut self, sample: Option<u64>, now: Instant) -> bool {
        let Some(total) = sample else {
            self.reset();
            return false;
        };
        let Some((previous, time)) = self.baseline.replace((total, now)) else {
            return false;
        };
        let Some(elapsed) = now.checked_duration_since(time).filter(|d| !d.is_zero()) else {
            self.reset();
            return false;
        };
        let Some(stalled) = total.checked_sub(previous) else {
            self.reset();
            return false;
        };
        if u128::from(stalled) * 100 < u128::from(self.threshold) * elapsed.as_micros() {
            self.since = None;
            return false;
        }
        let since = *self.since.get_or_insert(time);
        if now.duration_since(since) >= self.after {
            self.reset();
            return true;
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cumulative_stalls_reset_on_flat_counter_errors_and_clock_regression() {
        let start = Instant::now();
        let mut w = PsiWindow::new(7, Duration::from_millis(230));
        let t = |ms| start + Duration::from_millis(ms);
        assert!(!w.observe(Some(120), t(0)));
        assert!(!w.observe(Some(8_120), t(100)));
        assert!(!w.observe(Some(8_120), t(200)));
        assert!(!w.observe(Some(16_120), t(300)));
        assert!(!w.observe(None, t(400)));
        assert!(!w.observe(Some(24_120), t(500)));
        assert!(!w.observe(Some(32_120), t(600)));
        assert!(!w.observe(Some(40_120), t(700)));
        assert!(w.observe(Some(48_120), t(800)));
        assert!(!w.observe(Some(48_120), t(900)));
        assert!(!w.observe(Some(48_120), t(1000)));
        assert!(!w.observe(Some(3), t(1100)));
        assert!(!w.observe(Some(9000), t(1050)));
        assert_eq!(
            psi_total("full avg10=0 total=53\nsome avg10=0 total=91\n"),
            Some(91)
        );
        assert_eq!(psi_total("full total=53\n"), Some(53));
    }
}
