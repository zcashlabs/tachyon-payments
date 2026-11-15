//! The wall-clock budget of one end-to-end test.
//!
//! Every test must finish under 30 s (spec section 13.1, `tools/ci/test_budget.py`).
//! [`DEFAULT_BUDGET`] leaves headroom under that rule so a slow test fails here,
//! with its name and elapsed time, before the CI runner kills it.
use std::future::Future;
use std::time::{Duration, Instant};

/// The default budget of an end-to-end test: 25 s, under the 30 s rule.
pub const DEFAULT_BUDGET: Duration = Duration::from_secs(25);

/// A running budget. Create one at the start of a test; it fails the test when
/// [`Budget::check`] is called, when [`Budget::run`] times out, or when it is
/// dropped, if the budget is exceeded by then.
#[derive(Debug)]
pub struct Budget {
    name: String,
    started: Instant,
    limit: Duration,
}

impl Budget {
    /// A [`DEFAULT_BUDGET`] for the current test (named after the test thread).
    pub fn start() -> Self {
        Self::with_limit(DEFAULT_BUDGET)
    }

    /// A budget of `limit` for the current test.
    pub fn with_limit(limit: Duration) -> Self {
        let name = std::thread::current()
            .name()
            .unwrap_or("<unnamed test>")
            .to_owned();
        Self {
            name,
            started: Instant::now(),
            limit,
        }
    }

    pub fn limit(&self) -> Duration {
        self.limit
    }

    pub fn elapsed(&self) -> Duration {
        self.started.elapsed()
    }

    /// Time left, zero once exceeded. Use it as the deadline of waits inside the
    /// test (`process.wait_ready(addr, budget.remaining())`).
    pub fn remaining(&self) -> Duration {
        self.limit.saturating_sub(self.elapsed())
    }

    /// The instant the budget runs out.
    pub fn deadline(&self) -> Instant {
        self.started + self.limit
    }

    /// Fails the test if the budget is exceeded.
    pub fn check(&self) {
        let elapsed = self.elapsed();
        if elapsed > self.limit {
            panic!("{}", self.exceeded(elapsed));
        }
    }

    /// Runs `test` inside the remaining budget, failing the test with a clear
    /// message if it does not finish in time.
    pub async fn run<F: Future>(&self, test: F) -> F::Output {
        match tokio::time::timeout(self.remaining(), test).await {
            Ok(output) => {
                self.check();
                output
            }
            Err(_) => panic!("{}", self.exceeded(self.elapsed())),
        }
    }

    fn exceeded(&self, elapsed: Duration) -> String {
        format!(
            "end-to-end test `{}` exceeded its wall-clock budget: {:.1} s elapsed, budget {:.1} s \
             (every test must finish under 30 s, spec section 13.1)",
            self.name,
            elapsed.as_secs_f64(),
            self.limit.as_secs_f64()
        )
    }
}

impl Drop for Budget {
    fn drop(&mut self) {
        let elapsed = self.elapsed();
        if elapsed > self.limit && !std::thread::panicking() {
            panic!("{}", self.exceeded(elapsed));
        }
    }
}

/// Runs `test` inside [`DEFAULT_BUDGET`]: the usual body of an end-to-end test.
pub async fn e2e<F: Future>(test: F) -> F::Output {
    within(DEFAULT_BUDGET, test).await
}

/// Runs `test` inside a budget of `limit`.
pub async fn within<F: Future>(limit: Duration, test: F) -> F::Output {
    Budget::with_limit(limit).run(test).await
}
