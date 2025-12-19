//! Extra serde serialization/deserialization helpers for `Vec<alloy_primitives::U256>`
#[allow(dead_code)]
pub(super) mod u256 {
    use alloy_primitives::U256;
    use serde::{Deserialize, Serialize as _, de::Deserializer, ser::Serializer};

    /// EVM U256 is represented in big-endian, but ClickHouse expects little-endian.
    pub(crate) fn serialize<S: Serializer>(u256: &U256, serializer: S) -> Result<S::Ok, S::Error> {
        let buf: [u8; 32] = u256.to_le_bytes();
        buf.serialize(serializer)
    }

    /// Deserialize U256 following ClickHouse RowBinary format.
    ///
    /// ClickHouse stores U256 in little-endian, we have to convert it back to big-endian.
    pub(crate) fn deserialize<'de, D>(deserializer: D) -> Result<U256, D::Error>
    where
        D: Deserializer<'de>,
    {
        let buf: [u8; 32] = Deserialize::deserialize(deserializer)?;
        Ok(U256::from_le_bytes(buf))
    }

    #[allow(dead_code)]
    pub(crate) mod option {
        use super::*;

        pub(crate) fn serialize<S: Serializer>(
            maybe_u256: &Option<U256>,
            serializer: S,
        ) -> Result<S::Ok, S::Error> {
            if let Some(u256) = maybe_u256 {
                let buf: [u8; 32] = u256.to_le_bytes();
                serializer.serialize_some(&buf)
            } else {
                serializer.serialize_none()
            }
        }

        pub(crate) fn deserialize<'de, D>(deserializer: D) -> Result<Option<U256>, D::Error>
        where
            D: Deserializer<'de>,
        {
            let option: Option<[u8; 32]> = Deserialize::deserialize(deserializer)?;
            Ok(option.map(U256::from_le_bytes))
        }
    }
}

pub(super) mod u256es {
    use alloy_primitives::U256;
    use serde::{
        Deserialize,
        de::Deserializer,
        ser::{SerializeSeq, Serializer},
    };

    /// Serialize Vec<U256> following ClickHouse RowBinary format.
    ///
    /// EVM U256 is represented in big-endian, but ClickHouse expects little-endian.
    pub(crate) fn serialize<S: Serializer>(
        u256es: &[U256],
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        // It consists of a LEB128 length prefix followed by the raw bytes of each U256 in
        // little-endian order.

        // <https://github.com/ClickHouse/clickhouse-rs/blob/v0.13.3/src/rowbinary/ser.rs#L159-L164>
        let mut seq = serializer.serialize_seq(Some(u256es.len()))?;
        for u256 in u256es {
            let buf: [u8; 32] = u256.to_le_bytes();
            seq.serialize_element(&buf)?;
        }
        seq.end()
    }

    /// Deserialize Vec<U256> following ClickHouse RowBinary format.
    ///
    /// ClickHouse stores U256 in little-endian, we have to convert it back to big-endian.
    pub(crate) fn deserialize<'de, D>(deserializer: D) -> Result<Vec<U256>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let vec: Vec<[u8; 32]> = Deserialize::deserialize(deserializer)?;
        Ok(vec.into_iter().map(U256::from_le_bytes).collect())
    }
}

pub(super) mod hashes {
    use alloy_primitives::B256;
    use serde::{
        Deserialize,
        de::Deserializer,
        ser::{SerializeSeq, Serializer},
    };

    pub(crate) fn serialize<S: Serializer>(vec: &[B256], serializer: S) -> Result<S::Ok, S::Error> {
        let mut seq = serializer.serialize_seq(Some(vec.len()))?;
        for hash in vec {
            seq.serialize_element(&hash.0)?;
        }

        seq.end()
    }

    pub(crate) fn deserialize<'de, D>(deserializer: D) -> Result<Vec<B256>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let vec: Vec<[u8; 32]> = Deserialize::deserialize(deserializer)?;
        Ok(vec.into_iter().map(B256::from).collect())
    }
}

pub(super) mod hash {
    use alloy_primitives::B256;
    use serde::{
        Deserialize,
        de::Deserializer,
        ser::{SerializeTuple as _, Serializer},
    };

    pub(crate) fn serialize<S: Serializer>(hash: &B256, serializer: S) -> Result<S::Ok, S::Error> {
        let mut tup = serializer.serialize_tuple(32)?;

        for byte in hash.0 {
            tup.serialize_element(&byte)?;
        }

        tup.end()
    }

    pub(crate) fn deserialize<'de, D>(deserializer: D) -> Result<B256, D::Error>
    where
        D: Deserializer<'de>,
    {
        let bytes: [u8; 32] = Deserialize::deserialize(deserializer)?;
        Ok(B256::from(bytes))
    }
}

#[allow(dead_code)]
pub(super) mod address {
    use alloy_primitives::Address;
    use serde::{Deserialize, Serialize as _, de::Deserializer, ser::Serializer};

    pub(crate) fn serialize<S: Serializer>(
        address: &Address,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        let address_bytes = &address.0.0;
        address_bytes.serialize(serializer)
    }

    pub(crate) fn deserialize<'de, D>(deserializer: D) -> Result<Address, D::Error>
    where
        D: Deserializer<'de>,
    {
        let buf: [u8; 20] = Deserialize::deserialize(deserializer)?;
        Ok(Address::from(buf))
    }

    pub(crate) mod option {
        use super::*;

        pub(crate) fn serialize<S: Serializer>(
            address: &Option<Address>,
            serializer: S,
        ) -> Result<S::Ok, S::Error> {
            if let Some(address) = address {
                let address_bytes = &address.0.0;
                serializer.serialize_some(address_bytes)
            } else {
                serializer.serialize_none()
            }
        }

        pub(crate) fn deserialize<'de, D>(deserializer: D) -> Result<Option<Address>, D::Error>
        where
            D: Deserializer<'de>,
        {
            let option: Option<[u8; 20]> = Deserialize::deserialize(deserializer)?;
            Ok(option.map(Address::from))
        }
    }
}

pub(super) mod addresses {
    use alloy_primitives::Address;
    use serde::{
        Deserialize,
        de::Deserializer,
        ser::{SerializeSeq, Serializer},
    };

    pub(crate) mod option {
        use super::*;

        pub(crate) fn serialize<S: Serializer>(
            vec: &[Option<Address>],
            serializer: S,
        ) -> Result<S::Ok, S::Error> {
            let mut seq = serializer.serialize_seq(Some(vec.len()))?;
            for address in vec {
                let address_bytes = address.map(|a| a.0.0);
                seq.serialize_element(&address_bytes)?;
            }
            seq.end()
        }

        pub(crate) fn deserialize<'de, D>(deserializer: D) -> Result<Vec<Option<Address>>, D::Error>
        where
            D: Deserializer<'de>,
        {
            let vec: Vec<Option<[u8; 20]>> = Deserialize::deserialize(deserializer)?;
            Ok(vec.into_iter().map(|b| b.map(Address::from)).collect())
        }
    }

    pub(crate) fn serialize<S: Serializer>(
        vec: &[Address],
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        let mut seq = serializer.serialize_seq(Some(vec.len()))?;
        for address in vec {
            let address_bytes = &address.0.0;
            seq.serialize_element(address_bytes)?;
        }

        seq.end()
    }

    pub(crate) fn deserialize<'de, D>(deserializer: D) -> Result<Vec<Address>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let vec: Vec<[u8; 20]> = Deserialize::deserialize(deserializer)?;
        Ok(vec.into_iter().map(Address::from).collect())
    }
}
