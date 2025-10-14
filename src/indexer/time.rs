//! Time-related utilies.
//!
//! TODO: move this into a "primitives" folder, requires refactory that should not be done in the
//! PR that introduced this.

use std::{
    future::{poll_fn, Future as _},
    iter::Iterator,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

/// A random number generator for applying jitter to [`std::time::Duration`].
#[derive(Debug, Clone)]
pub(crate) struct Jitter;

impl Jitter {
    /// Apply jitter to provided duration, by multiplying it for a random number between 0 and 2.
    pub(crate) fn apply_to(duration: Duration) -> Duration {
        duration.mul_f64(rand::random::<f64>() * 2_f64)
    }
}

/// A retry strategy driven by exponential back-off.
///
/// The power corresponds to the number of past attempts.
///
/// Taken from <https://docs.rs/tokio-retry/latest/src/tokio_retry/strategy/exponential_backoff.rs.html>
#[derive(Debug, Clone)]
pub(crate) struct ExponentialBackoff {
    current: u64,
    base: u64,
    factor: u64,
    max_delay: Option<Duration>,
}

#[allow(dead_code)]
impl ExponentialBackoff {
    /// Constructs a new exponential back-off strategy,
    /// given a base duration in milliseconds.
    ///
    /// The resulting duration is calculated by taking the base to the `n`-th power,
    /// where `n` denotes the number of past attempts.
    pub(crate) fn from_millis(base: u64) -> ExponentialBackoff {
        ExponentialBackoff { current: base, base, factor: 1u64, max_delay: None }
    }

    /// A multiplicative factor that will be applied to the retry delay.
    ///
    /// For example, using a factor of `1000` will make each delay in units of seconds.
    ///
    /// Default factor is `1`.
    pub(crate) fn factor(mut self, factor: u64) -> ExponentialBackoff {
        self.factor = factor;
        self
    }

    /// Apply a maximum delay. No retry delay will be longer than this `Duration`.
    pub(crate) fn max_delay(mut self, duration: Duration) -> ExponentialBackoff {
        self.max_delay = Some(duration);
        self
    }

    /// Reset the backoff to the initial state.
    pub(crate) fn reset(&mut self) {
        self.current = self.base;
    }
}

impl Iterator for ExponentialBackoff {
    type Item = Duration;

    fn next(&mut self) -> Option<Duration> {
        // set delay duration by applying factor
        let duration = if let Some(duration) = self.current.checked_mul(self.factor) {
            Duration::from_millis(duration)
        } else {
            Duration::from_millis(u64::MAX)
        };

        // check if we reached max delay
        if let Some(ref max_delay) = self.max_delay {
            if duration > *max_delay {
                return Some(*max_delay);
            }
        }

        if let Some(next) = self.current.checked_mul(self.base) {
            self.current = next;
        } else {
            self.current = u64::MAX;
        }

        Some(duration)
    }
}

/// An interval heavily inspired by [`tokio::time::Interval`], that supports exponential back-off
/// and jitter.
#[derive(Debug)]
pub(crate) struct BackoffInterval {
    /// Future that completes the next time the `Interval` yields a value.
    delay: Pin<Box<tokio::time::Sleep>>,

    /// The exponential backoff configuration.
    backoff: ExponentialBackoff,

    /// An optional jitter to apply to the ticks.
    jitter: bool,
}

impl BackoffInterval {
    /// Creates a new interval that ticks immediately.
    pub(crate) fn new(backoff: ExponentialBackoff) -> Self {
        let start = tokio::time::Instant::now();
        let delay = Box::pin(tokio::time::sleep_until(start));
        Self { delay, backoff, jitter: false }
    }

    pub(crate) fn with_jitter(mut self) -> Self {
        self.jitter = true;
        self
    }

    pub(crate) fn poll_tick(&mut self, cx: &mut Context<'_>) -> Poll<tokio::time::Instant> {
        // Wait for the delay to be done
        std::task::ready!(Pin::new(&mut self.delay).poll(cx));

        // Get the time when we were schedulued to tick
        let timeout = self.delay.deadline();

        // CHANGE: use custom logic that takes into a account backoff and jitter to calculate new
        // instant.
        let next = self.next();

        // CHANGE: Unfortunately, [`tokio::time::Sleep::reset_without_reregister`] isn't
        // pub(crate)lic so we have to register the waker again.
        self.delay.as_mut().reset(next);

        Poll::Ready(timeout)
    }

    /// Completes when the next instant in the interval has been reached.
    pub(crate) async fn tick(&mut self) -> tokio::time::Instant {
        let instant = poll_fn(|cx| self.poll_tick(cx));

        instant.await
    }

    /// Resets backoff to the initial state, and the next tick will happen after the initial period
    /// returned by [`ExponentialBackoff`].
    pub(crate) fn reset(&mut self) {
        self.backoff.reset();
        let next = self.next();
        self.delay.as_mut().reset(next);
    }

    /// Return the next instant at which the interval should tick.
    fn next(&mut self) -> tokio::time::Instant {
        let now = tokio::time::Instant::now();
        // We provide a [`tokio::time::MissedTickBehavior::Delay`] behavior but we also add backoff
        // and jitter if the user configured it.
        let mut period = self.backoff.next().expect("ExponentialBackoff never returns None");
        if self.jitter {
            period = Jitter::apply_to(period);
        }
        now.checked_add(period).expect("no overflow")
    }
}

impl Default for BackoffInterval {
    fn default() -> Self {
        // So will return 4, 16, 32, 64, 128, ... milliseconds with jitter.
        Self::new(ExponentialBackoff::from_millis(4)).with_jitter()
    }
}

#[cfg(test)]
mod tests {
    use tokio::time::{self, Duration, Instant};

    use super::*;

    #[test]
    fn exp_backoff_returns_some_exponential_base_10() {
        let mut s = ExponentialBackoff::from_millis(10);

        assert_eq!(s.next(), Some(Duration::from_millis(10)));
        assert_eq!(s.next(), Some(Duration::from_millis(100)));
        assert_eq!(s.next(), Some(Duration::from_millis(1000)));
    }

    #[test]
    fn exp_backoff_returns_some_exponential_base_2() {
        let mut s = ExponentialBackoff::from_millis(2);

        assert_eq!(s.next(), Some(Duration::from_millis(2)));
        assert_eq!(s.next(), Some(Duration::from_millis(4)));
        assert_eq!(s.next(), Some(Duration::from_millis(8)));
    }

    #[test]
    fn exp_backoff_saturates_at_maximum_value() {
        let mut s = ExponentialBackoff::from_millis(u64::MAX - 1);

        assert_eq!(s.next(), Some(Duration::from_millis(u64::MAX - 1)));
        assert_eq!(s.next(), Some(Duration::from_millis(u64::MAX)));
        assert_eq!(s.next(), Some(Duration::from_millis(u64::MAX)));
    }

    #[test]
    fn exp_backoff_can_use_factor_to_get_seconds() {
        let factor = 1000;
        let mut s = ExponentialBackoff::from_millis(2).factor(factor);

        assert_eq!(s.next(), Some(Duration::from_secs(2)));
        assert_eq!(s.next(), Some(Duration::from_secs(4)));
        assert_eq!(s.next(), Some(Duration::from_secs(8)));
    }

    #[test]
    fn exp_backoff_stops_increasing_at_max_delay() {
        let mut s = ExponentialBackoff::from_millis(2).max_delay(Duration::from_millis(4));

        assert_eq!(s.next(), Some(Duration::from_millis(2)));
        assert_eq!(s.next(), Some(Duration::from_millis(4)));
        assert_eq!(s.next(), Some(Duration::from_millis(4)));
    }

    #[test]
    fn exp_backoff_returns_max_when_max_less_than_base() {
        let mut s = ExponentialBackoff::from_millis(20).max_delay(Duration::from_millis(10));

        assert_eq!(s.next(), Some(Duration::from_millis(10)));
        assert_eq!(s.next(), Some(Duration::from_millis(10)));
    }

    #[tokio::test]
    async fn backoff_interval_ticks_as_expected() {
        let backoff = ExponentialBackoff::from_millis(2);
        // Should yiled now, now + 2ms, now + 4ms, now + 8ms, ...
        let mut interval = BackoffInterval::new(backoff);
        interval.tick().await;

        let before = Instant::now();
        interval.tick().await; // +2ms
        interval.tick().await; // +4ms
        interval.tick().await; // +8ms
        interval.tick().await; // +16ms
        let total = 2 + 4 + 8 + 16;
        // It may drift, but should be bounded.
        assert!(before + Duration::from_millis(total + 10) >= Instant::now());
        assert!(Instant::now() >= before + Duration::from_millis(total));
    }

    #[tokio::test]
    async fn backoff_interval_ticks_immediately_then_backoffs() {
        let backoff = ExponentialBackoff::from_millis(8);
        // Should yiled now, now + 8ms, now + 64ms, now + 512ms, ...
        let mut interval = BackoffInterval::new(backoff);

        // The first tick should fire immediately

        let before_1 = Instant::now();
        let t1 = interval.tick().await;
        assert!(before_1 + Duration::from_millis(1) >= t1); // Ensure it's bounded.

        // Move forward by 2ms, should tick in about 6ms.
        time::sleep(Duration::from_millis(2)).await;
        tokio::select! {
            _ = interval.tick() => { panic!("should not have ticked yet"); },
            _ = time::sleep(Duration::from_millis(4)) => {},
        };
        time::sleep(Duration::from_millis(6)).await;

        let before_2 = Instant::now();
        let t2 = interval.tick().await;
        assert!(before_2 + Duration::from_millis(1) >= t2); // Ensure it's bounded.

        // Move forward by 16ms, should tick in about 48ms.
        time::sleep(Duration::from_millis(16)).await;
        tokio::select! {
            _ = interval.tick() => { panic!("should not have ticked yet"); },
            _ = time::sleep(Duration::from_millis(32)) => {},
        };
        time::sleep(Duration::from_millis(48)).await;

        let before_3 = Instant::now();
        let t3 = interval.tick().await;
        assert!(before_3 + Duration::from_millis(1) >= t3); // Ensure it's bounded.
    }

    #[tokio::test]
    async fn backoff_interval_resets_properly() {
        let mut interval = BackoffInterval::new(ExponentialBackoff::from_millis(2));
        interval.tick().await;

        let acceptable_drift = 10;

        let before = Instant::now();
        interval.tick().await; // +2ms
        interval.tick().await; // +4ms
        interval.tick().await; // +8ms
        interval.tick().await; // +16ms
        let total = 2 + 4 + 8 + 16;
        // It may drift, but should be bounded.
        assert!(before + Duration::from_millis(total + acceptable_drift) >= Instant::now());
        assert!(Instant::now() >= before + Duration::from_millis(total));

        let acceptable_drift = 1;
        interval.reset();
        let before = Instant::now();
        interval.tick().await;
        assert!(before + Duration::from_millis(2 + acceptable_drift) >= Instant::now());
        assert!(Instant::now() >= before + Duration::from_millis(2));
    }

    #[tokio::test]
    async fn backoff_interval_with_jitter_works() {
        time::pause(); // manual control of time with time::advance

        // No jitter
        {
            let beginning = Instant::now();

            let backoff = ExponentialBackoff::from_millis(5);
            let mut interval = BackoffInterval::new(backoff);
            let t1 = interval.tick().await;
            assert_eq!(t1, beginning); // First tick is immediate

            let expected_drift = 1; // in this mode, tokio intervally advances time by 1ms after sleep completion

            // Next tick will be 5ms later, we have no jitter.
            let t2 = interval.tick().await;
            assert_eq!(t2, beginning + Duration::from_millis(5 + expected_drift));

            // Next tick will be 25ms later, we have no jitter.
            let t2 = interval.tick().await;
            assert_eq!(t2, beginning + Duration::from_millis(5 + 25 + expected_drift * 2));
        }

        // Jitter
        {
            let beginning = Instant::now();

            let backoff = ExponentialBackoff::from_millis(5);
            let mut interval = BackoffInterval::new(backoff).with_jitter();
            let t1 = interval.tick().await;
            assert_eq!(t1, beginning); // First tick is immediate

            // Next tick will be 5ms later, but jitter changes it.
            let t2 = interval.tick().await;
            assert_ne!(t2, beginning + Duration::from_millis(5));
        }
    }
}
