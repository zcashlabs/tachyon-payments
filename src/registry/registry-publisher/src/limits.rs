//! Sliding-window rate limits of the write API (spec 9.3): per client and
//! global, over a 60-second window of requests accepted for processing.
//!
//! A refused request does not count. `Retry-After` is the whole number of
//! seconds until the window admits the client again, at least 1.
use std::{
    collections::{HashMap, VecDeque},
    sync::Mutex,
    time::{Duration, Instant},
};

/// The window every rate is counted over.
pub const WINDOW: Duration = Duration::from_secs(60);

/// Which rate refused a request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Limited {
    /// The global cap; retry after this many seconds.
    Global(u64),
    /// The client's own rate; retry after this many seconds.
    Client(u64),
}

impl Limited {
    pub fn retry_after(self) -> u64 {
        match self {
            Self::Global(seconds) | Self::Client(seconds) => seconds,
        }
    }
}

/// Accepted requests in the last [`WINDOW`], oldest first.
#[derive(Default)]
struct Window(VecDeque<Instant>);

impl Window {
    fn prune(&mut self, now: Instant) {
        while self
            .0
            .front()
            .is_some_and(|t| now.saturating_duration_since(*t) >= WINDOW)
        {
            self.0.pop_front();
        }
    }

    /// `None` if one more fits under `cap`, else the seconds until it does.
    fn refusal(&mut self, now: Instant, cap: u32) -> Option<u64> {
        self.prune(now);
        if self.0.len() < cap as usize {
            return None;
        }
        // The window admits again once enough of the oldest accepted
        // requests have aged out to leave room for one more.
        let freeing = self.0[self.0.len() - cap as usize];
        let wait = (freeing + WINDOW).saturating_duration_since(now);
        Some(seconds_ceil(wait).max(1))
    }
}

fn seconds_ceil(duration: Duration) -> u64 {
    duration.as_secs() + u64::from(duration.subsec_nanos() > 0)
}

struct State {
    global: Window,
    clients: HashMap<String, Window>,
}

/// The global and per-client windows, checked and counted atomically.
pub struct RateLimits {
    global_per_minute: u32,
    per_client_per_minute: u32,
    state: Mutex<State>,
}

impl RateLimits {
    pub fn new(global_per_minute: u32, per_client_per_minute: u32) -> Self {
        Self {
            global_per_minute,
            per_client_per_minute,
            state: Mutex::new(State {
                global: Window::default(),
                clients: HashMap::new(),
            }),
        }
    }

    /// Admit one request against the global cap only (the challenge route),
    /// counting it if admitted.
    pub fn admit_global(&self, now: Instant) -> Result<(), Limited> {
        let mut state = self.state.lock().unwrap();
        if let Some(seconds) = state.global.refusal(now, self.global_per_minute) {
            return Err(Limited::Global(seconds));
        }
        state.global.0.push_back(now);
        Ok(())
    }

    /// Admit one registration of `client` against the global cap and the
    /// client's rate, counting it in both if admitted.
    pub fn admit(&self, client: &str, now: Instant) -> Result<(), Limited> {
        let mut state = self.state.lock().unwrap();
        if let Some(seconds) = state.global.refusal(now, self.global_per_minute) {
            return Err(Limited::Global(seconds));
        }
        let cap = self.per_client_per_minute;
        if let Some(window) = state.clients.get_mut(client)
            && let Some(seconds) = window.refusal(now, cap)
        {
            return Err(Limited::Client(seconds));
        }
        state.global.0.push_back(now);
        // Clients are counted only once accepted, and accepted requests are
        // bounded by the global cap, so idle windows are dropped once the map
        // outgrows it.
        if state.clients.len() > self.global_per_minute as usize {
            state.clients.retain(|_, window| {
                window.prune(now);
                !window.0.is_empty()
            });
        }
        state
            .clients
            .entry(client.to_owned())
            .or_default()
            .0
            .push_back(now);
        Ok(())
    }

    /// Clients with a nonempty window (for tests).
    pub fn tracked(&self) -> usize {
        self.state.lock().unwrap().clients.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn per_client_rate_slides_and_reports_retry_after() {
        let limits = RateLimits::new(100, 3);
        let t0 = Instant::now();
        for i in 0..3 {
            assert_eq!(limits.admit("a", t0 + Duration::from_secs(i * 10)), Ok(()));
        }
        // The fourth, at t0 + 30 s, waits for the first to age out at t0 + 60 s.
        assert_eq!(
            limits.admit("a", t0 + Duration::from_secs(30)),
            Err(Limited::Client(30))
        );
        assert_eq!(
            limits.admit("a", t0 + Duration::from_millis(59_500)),
            Err(Limited::Client(1))
        );
        // A refused request did not count: at t0 + 60 s one slot is free.
        assert_eq!(limits.admit("a", t0 + Duration::from_secs(60)), Ok(()));
        assert_eq!(
            limits.admit("a", t0 + Duration::from_secs(61)),
            Err(Limited::Client(9))
        );
        // Another client is unaffected.
        assert_eq!(limits.admit("b", t0 + Duration::from_secs(61)), Ok(()));
    }

    #[test]
    fn global_cap_covers_every_client_and_the_challenge_route() {
        let limits = RateLimits::new(3, 10);
        let t0 = Instant::now();
        assert_eq!(limits.admit("a", t0), Ok(()));
        assert_eq!(limits.admit("b", t0), Ok(()));
        assert_eq!(limits.admit_global(t0 + Duration::from_millis(1)), Ok(()));
        assert_eq!(
            limits.admit("c", t0 + Duration::from_secs(1)),
            Err(Limited::Global(59))
        );
        assert_eq!(
            limits.admit_global(t0 + Duration::from_millis(1500)),
            Err(Limited::Global(59))
        );
        assert_eq!(limits.admit("c", t0 + Duration::from_secs(60)), Ok(()));
        // The window of client "c" counted only its accepted request.
        assert_eq!(limits.tracked(), 3);
    }

    #[test]
    fn idle_client_windows_are_dropped() {
        let limits = RateLimits::new(2, 1);
        let t0 = Instant::now();
        assert_eq!(limits.admit("a", t0), Ok(()));
        assert_eq!(limits.admit("b", t0), Ok(()));
        let later = t0 + Duration::from_secs(61);
        assert_eq!(limits.admit("c", later), Ok(()));
        assert_eq!(limits.admit("d", later), Ok(()));
        assert_eq!(limits.admit("e", later + WINDOW), Ok(()));
        assert!(limits.tracked() <= 3, "{}", limits.tracked());
    }
}
