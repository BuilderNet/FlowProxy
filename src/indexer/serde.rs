//! Extra serde serialization/deserialization helpers for `Vec<alloy_primitives::U256>`

use alloy_primitives::U256;
use serde::{
    de::{Deserializer, SeqAccess, Visitor},
    ser::{SerializeSeq, Serializer},
};

/// Serialize Vec<U256> following ClickHouse RowBinary format.
///
/// EVM U256 is represented in big-endian, but ClickHouse expects little-endian.
pub(crate) fn serialize_vec_u256<S: Serializer>(
    vec: &Vec<U256>,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    // It consists of a LEB128 length prefix followed by the raw bytes of each U256 in
    // little-endian order.

    // <https://github.com/ClickHouse/clickhouse-rs/blob/v0.13.3/src/rowbinary/ser.rs#L159-L164>
    let mut seq = serializer.serialize_seq(Some(vec.len()))?;
    for u256 in vec {
        let buf: [u8; 32] = u256.to_le_bytes();
        seq.serialize_element(&buf)?;
    }
    seq.end()
}

/// Deserialize Vec<U256> following ClickHouse RowBinary format.
///
/// ClickHouse stores U256 in little-endian, we have to convert it back to big-endian.
pub(crate) fn deserialize_vec_u256<'de, D>(deserializer: D) -> Result<Vec<U256>, D::Error>
where
    D: Deserializer<'de>,
{
    struct U256VecVisitor;

    impl<'de> Visitor<'de> for U256VecVisitor {
        type Value = Vec<U256>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("a sequence of U256 values")
        }

        fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
        where
            A: SeqAccess<'de>,
        {
            let mut vec = Vec::new();
            while let Some(buf) = seq.next_element::<[u8; 32]>()? {
                vec.push(U256::from_le_bytes(buf));
            }
            Ok(vec)
        }
    }

    deserializer.deserialize_seq(U256VecVisitor)
}

pub(super) mod hashes {
    use alloy_primitives::B256;
    use serde::{
        de::Deserializer,
        ser::{SerializeSeq, Serializer},
        Deserialize,
    };

    pub(crate) fn serialize<S: Serializer>(
        vec: &Vec<B256>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        let mut seq = serializer.serialize_seq(Some(vec.len()))?;
        for hash in vec {
            // Converts a String to ASCII bytes
            seq.serialize_element(&hash.0)?;
        }

        seq.end()
    }

    pub(crate) fn deserialize<'de, D>(deserializer: D) -> Result<Vec<B256>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let vec: Vec<[u8; 32]> = Deserialize::deserialize(deserializer)?;
        Ok(vec.into_iter().map(|b| B256::from(b)).collect())
    }
}

pub(super) mod addresses {
    use alloy_primitives::Address;
    use serde::{
        de::Deserializer,
        ser::{SerializeSeq, Serializer},
        Deserialize,
    };

    pub(crate) fn serialize<S: Serializer>(
        vec: &Vec<Address>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        let mut seq = serializer.serialize_seq(Some(vec.len()))?;
        for address in vec {
            seq.serialize_element(address.as_slice())?;
        }

        seq.end()
    }

    pub(crate) fn deserialize<'de, D>(deserializer: D) -> Result<Vec<Address>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let vec: Vec<&[u8]> = Deserialize::deserialize(deserializer)?;
        Ok(vec.into_iter().map(|b| Address::from_slice(b)).collect())
    }
}
