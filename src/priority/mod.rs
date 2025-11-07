use strum_macros::{Display, EnumString};

pub mod channel;
pub mod workers;

/// Bundle processing priority.
#[derive(PartialEq, Eq, Clone, Copy, Default, Debug, Display, EnumString)]
#[strum(serialize_all = "lowercase")]
pub enum Priority {
    /// High processing priority: assigned to replacement bundles from entities with low spam score
    /// and non-reverting bundles.
    High,
    /// Medium processing priority: assigned to regular bundles from entities with low to medium
    /// spam score.
    Medium,
    /// Low priority: assigned to bundles from unknown entities, bundles with blob transactions and
    /// bundles from entities with high spam score.
    #[default]
    Low,
}

impl Priority {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::High => "high",
            Self::Medium => "medium",
            Self::Low => "low",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn priority_str_roundtrip() {
        assert_eq!(Priority::High.to_string(), "high");
        assert_eq!(Priority::Medium.to_string(), "medium");
        assert_eq!(Priority::Low.to_string(), "low");

        for priority in [Priority::High, Priority::Medium, Priority::Low] {
            assert_eq!(priority, Priority::from_str(&priority.to_string()).unwrap());
        }
    }
}
