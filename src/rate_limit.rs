use crate::utils;
use std::{
    collections::BTreeMap,
    time::{Duration, Instant},
};

#[derive(Debug)]
pub struct CounterOverTime {
    lookback: Duration,
    nb_buckets: u32,
    entries: BTreeMap<Instant, u64>,
    // Invariant: the sum of the values in `entries` should be equal to `sum_entries`.
    sum_entries: u64,
}

impl CounterOverTime {
    /// `lookback` is the duration of the rolling lookback window.
    /// `nb_buckets` is the number of buckets we split it into (the more, the more precise we are)
    pub fn new(lookback: Duration, nb_buckets: u32) -> Self {
        Self { lookback, nb_buckets, entries: BTreeMap::new(), sum_entries: 0 }
    }

    fn cleanup(&mut self) {
        // We remove the first entry of `self.entries` as long as it's outside
        // of our look-back window.
        let now = Instant::now();
        while let Some(entry) = self.entries.first_entry() {
            if entry.key() >= &(now - self.lookback) {
                return;
            }
            self.sum_entries -= entry.remove();
        }
        assert_eq!(self.sum_entries, 0);
    }

    pub fn add(&mut self, amount: u64, time: Instant) {
        self.cleanup();
        let duration_bucket =
            utils::clamp_to_duration_bucket(time, self.lookback / self.nb_buckets);
        *self.entries.entry(duration_bucket).or_default() += amount;
        self.sum_entries += amount;
    }

    pub fn inc(&mut self) -> u64 {
        self.add(1, Instant::now());
        self.sum_entries
    }

    pub fn count(&mut self) -> u64 {
        self.cleanup();
        self.sum_entries
    }
}
