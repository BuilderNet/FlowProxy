use crate::{priority::Priority, rate_limit::CounterOverTime, utils};
use alloy_consensus::transaction::PooledTransaction;
use alloy_primitives::Address;
use rbuilder_primitives::serialize::RawBundle;
use revm_primitives::keccak256;
use serde::Deserialize;
use std::{
    collections::BTreeMap,
    time::{Duration, Instant},
};

/// Primary identifier of the entity.
#[derive(PartialEq, Eq, Clone, Copy, Hash, Debug, Deserialize)]
pub enum Entity {
    /// Bundle signer.
    Signer(Address),
    /// Entity could not be determined.
    Unknown,
}

impl Entity {
    /// Returns entity type as string.
    pub fn as_str_ty(&self) -> &str {
        match self {
            Self::Signer(_) => "signer",
            Self::Unknown => "unknown",
        }
    }

    /// Returns `true` if entity is unknown.
    pub fn is_unknown(&self) -> bool {
        matches!(self, Self::Unknown)
    }

    /// Returns [`Priority`] for the entity based on bundle content and its score.
    pub fn priority(
        &self,
        request: EntityRequest<'_>,
        scores: &mut EntityScores,
        spam_thresholds: &SpamThresholds,
    ) -> Priority {
        // Unknown entities get low processing priority.
        if self.is_unknown() {
            return Priority::Low;
        }

        // Bundles with blob transactions get low processing priority
        // due to excessive bandwidth consumption.
        if request.has_eip4844_txs() {
            return Priority::Low;
        }

        let score = scores.calculate();
        let is_replacement = request.is_replacement();
        let is_non_revertible = request.is_non_revertible();

        if (score < spam_thresholds.medium && is_replacement) || is_non_revertible {
            return Priority::High;
        }

        if spam_thresholds.high > score && score < spam_thresholds.medium {
            return Priority::Medium;
        }

        Priority::Low
    }
}

#[derive(Debug)]
pub struct SpamThresholds {
    /// Score above this threshold is considered high spam.
    high: f64,
    /// Score above this threshold and below
    medium: f64,
}

/// Opinionated default implementation of the spam thresholds.
impl Default for SpamThresholds {
    fn default() -> Self {
        Self { high: 6., medium: 3. }
    }
}

/// Entity related information kept in the system.
#[derive(Debug)]
pub struct EntityData {
    pub rate_limit: CounterOverTime,
    /// Score metrics bucketed by scoring windows.
    pub scores: EntityScores,
}

#[derive(Debug)]
pub struct EntityScores {
    lookback: Duration,
    bucket: Duration,
    entries: BTreeMap<Instant, EntityScore>,
}

impl EntityScores {
    pub fn new(lookback: Duration, bucket: Duration) -> Self {
        Self { lookback, bucket, entries: BTreeMap::default() }
    }

    fn cleanup(&mut self) {
        // We remove the first entry of `self.entries` as long as it's outside
        // of our look-back window.
        let now = Instant::now();
        while let Some(entry) = self.entries.first_entry() {
            if entry.key() >= &(now - self.lookback) {
                return;
            }
            entry.remove();
        }
    }

    /// Returns `true` if there are no entries.
    pub fn is_empty(&mut self) -> bool {
        self.cleanup();
        self.entries.is_empty()
    }

    /// Returns the number of entries.
    pub fn len(&mut self) -> usize {
        self.cleanup();
        self.entries.len()
    }

    pub fn score_mut(&mut self, time: Instant) -> &mut EntityScore {
        self.cleanup();
        let duration_bucket = utils::clamp_to_duration_bucket(time, self.bucket);
        self.entries.entry(duration_bucket).or_default()
    }

    pub fn extend_score(&mut self, time: Instant, score: EntityScore) {
        self.score_mut(time).extend(score);
    }

    pub fn calculate(&mut self) -> f64 {
        self.cleanup();
        self.entries.values().map(|score| score.calculate()).sum::<f64>() /
            self.entries.len() as f64
    }
}

/// Entity score metrics.
#[derive(Default, Debug)]
pub struct EntityScore {
    /// Number of requests sent by the entity.
    pub number_of_requests: u64,
    /// The number of invalid requests sent by the entity.
    pub invalid_requests: u64,
    /// Entity stats from the builder.
    pub builder_stats: EntityBuilderStats,
}

impl EntityScore {
    /// Extend entity scores
    pub fn extend(&mut self, other: Self) {
        let Self { number_of_requests, invalid_requests, builder_stats } = other;
        self.number_of_requests += number_of_requests;
        self.invalid_requests += invalid_requests;
        self.builder_stats.extend(builder_stats);
    }
}

impl EntityScore {
    /// Calculates a spam score for the entity based on request behavior and builder statistics.
    ///
    /// The score is a floating-point value where **higher scores indicate more spam-like
    /// behavior**. It combines penalties for high request volume and invalid request ratio,
    /// and subtracts rewards for constructive participation in block building.
    ///
    /// ### Components:
    /// - **Request Penalty**: Based on the log-scaled number of requests made by the entity.
    /// - **Invalid Penalty**: Linear penalty based on the ratio of invalid to total requests.
    /// - **Builder Reward**:
    ///   - Number of times the entity passed ToB simulation.
    ///   - Number of times the entity was included in a block.
    ///   - Maximum contribution percentage to block value.
    ///
    /// ### Returns:
    /// A score where:
    /// - `0` is neutral / low spam
    /// - `>3` is considered potential spam
    /// - `>6` is considered likely spam
    ///
    /// ### Notes:
    /// - The function uses internal caps to prevent outliers from dominating the score:
    ///   - Request penalty is capped at 3
    ///   - Invalid penalty is capped at 5
    ///   - Builder reward is capped at 10
    pub fn calculate(&self) -> f64 {
        // Penalize high request rate
        let request_penalty = (self.number_of_requests as f64).log10().min(3.); // cap penalty

        // Penalize high invalid ratio
        let invalid_ratio = if self.number_of_requests > 0 {
            self.invalid_requests as f64 / self.number_of_requests as f64
        } else {
            0.
        };
        let invalid_penalty = (invalid_ratio * 5.0).min(5.0); // cap penalty

        // Reward good builder performance
        let builder = &self.builder_stats;
        let builder_reward = {
            let sim_success = builder.passed_tob_simulation_total as f64;
            let inclusion = builder.included_in_the_block_total as f64;
            let contribution = builder.max_block_contribution_pct;

            let base = sim_success * 0.1 + inclusion * 0.2 + contribution * 2.0;
            base.min(10.0) // cap reward
        };

        // Final score: higher means more spammy
        request_penalty + invalid_penalty - builder_reward
    }
}

/// Builder entity stats.
#[derive(Deserialize, Default, Debug)]
pub struct EntityBuilderStats {
    /// Number of times entity items passed ToB simulation.
    pub passed_tob_simulation_total: u64,
    /// Number of times entity items were included in the block.
    pub included_in_the_block_total: u64,
    /// Entity's maximum contribution percentage to block value.
    pub max_block_contribution_pct: f64,
}

impl EntityBuilderStats {
    /// Extend the stats.
    pub fn extend(&mut self, other: Self) {
        let Self {
            passed_tob_simulation_total,
            included_in_the_block_total,
            max_block_contribution_pct,
        } = other;
        self.passed_tob_simulation_total += passed_tob_simulation_total;
        self.included_in_the_block_total += included_in_the_block_total;
        self.max_block_contribution_pct =
            self.max_block_contribution_pct.max(max_block_contribution_pct);
    }
}

/// Enum encapsulating entity items.
#[derive(Debug)]
pub enum EntityRequest<'a> {
    PrivateTx(&'a PooledTransaction),
    Bundle(&'a RawBundle),
}

impl EntityRequest<'_> {
    /// Returns `true` if request is a replacement.
    fn is_replacement(&self) -> bool {
        match self {
            Self::PrivateTx(_) => false,
            Self::Bundle(bundle) => bundle.replacement_uuid.is_some(),
        }
    }

    /// Returns `true` if all transactions within the request are non-revertible (can revert but
    /// still be included in the block).
    fn is_non_revertible(&self) -> bool {
        match self {
            Self::PrivateTx(_) => true,
            Self::Bundle(bundle) => {
                bundle.txs.iter().all(|tx| bundle.reverting_tx_hashes.contains(&keccak256(tx)))
            }
        }
    }

    /// Returns `true` if the item contains blob transactions.
    fn has_eip4844_txs(&self) -> bool {
        match self {
            Self::PrivateTx(tx) => tx.is_eip4844(),
<<<<<<< HEAD
            Self::Bundle(bundle) => bundle.txs.iter().any(utils::looks_like_canonical_blob_tx),
=======
            Self::Bundle(bundle) => {
                bundle.txs.iter().any(|tx| utils::looks_like_canonical_blob_tx(tx))
            }
>>>>>>> 1b5d8b8 (refactor: use rbuilder-primitives)
        }
    }
}
