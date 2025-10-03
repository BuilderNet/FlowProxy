// Helper functions to extract data from Arrow arrays
use alloy_primitives::{Address, B256, U256};
use arrow::array::*;

pub fn extract_hash_list(array: &dyn Array, row_idx: usize) -> eyre::Result<Vec<B256>> {
    let list_array = array
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| eyre::eyre!("Expected ListArray"))?;

    let list_value = list_array.value(row_idx);
    let binary_array = list_value
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .ok_or_else(|| eyre::eyre!("Expected FixedSizeBinaryArray"))?;

    let mut hashes = Vec::new();
    for i in 0..binary_array.len() {
        let bytes = binary_array.value(i);
        if bytes.len() == 32 {
            hashes.push(B256::from_slice(bytes));
        }
    }
    Ok(hashes)
}

pub fn extract_i64(array: &dyn Array, row_idx: usize) -> eyre::Result<i64> {
    // Try different timestamp array types first
    if let Some(timestamp_array) = array.as_any().downcast_ref::<TimestampMicrosecondArray>() {
        return Ok(timestamp_array.value(row_idx));
    }

    if let Some(timestamp_array) = array.as_any().downcast_ref::<TimestampMillisecondArray>() {
        return Ok(timestamp_array.value(row_idx) * 1000); // Convert to microseconds
    }

    if let Some(timestamp_array) = array.as_any().downcast_ref::<TimestampSecondArray>() {
        return Ok(timestamp_array.value(row_idx) * 1_000_000); // Convert to microseconds
    }

    // Fallback to regular Int64Array
    if let Some(i64_array) = array.as_any().downcast_ref::<Int64Array>() {
        return Ok(i64_array.value(row_idx));
    }

    Err(eyre::eyre!("Expected TimestampArray or Int64Array, got {:?}", array.data_type()))
}

pub fn extract_address_list(array: &dyn Array, row_idx: usize) -> eyre::Result<Vec<Address>> {
    let list_array = array
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| eyre::eyre!("Expected ListArray"))?;

    let list_value = list_array.value(row_idx);
    let binary_array = list_value
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .ok_or_else(|| eyre::eyre!("Expected FixedSizeBinaryArray"))?;

    let mut addresses = Vec::new();
    for i in 0..binary_array.len() {
        let bytes = binary_array.value(i);
        if bytes.len() == 20 {
            addresses.push(Address::from_slice(bytes));
        }
    }
    Ok(addresses)
}

pub fn extract_optional_address_list(
    array: &dyn Array,
    row_idx: usize,
) -> eyre::Result<Vec<Option<Address>>> {
    let list_array = array
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| eyre::eyre!("Expected ListArray"))?;

    let list_value = list_array.value(row_idx);
    let binary_array = list_value
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .ok_or_else(|| eyre::eyre!("Expected FixedSizeBinaryArray"))?;

    let mut addresses = Vec::new();
    for i in 0..binary_array.len() {
        if binary_array.is_null(i) {
            addresses.push(None);
        } else {
            let bytes = binary_array.value(i);
            if bytes.len() == 20 {
                addresses.push(Some(Address::from_slice(bytes)));
            } else {
                addresses.push(None);
            }
        }
    }
    Ok(addresses)
}

pub fn extract_u64_list(array: &dyn Array, row_idx: usize) -> eyre::Result<Vec<u64>> {
    let list_array = array
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| eyre::eyre!("Expected ListArray"))?;

    let list_value = list_array.value(row_idx);
    let u64_array = list_value
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| eyre::eyre!("Expected UInt64Array"))?;

    let mut values = Vec::new();
    for i in 0..u64_array.len() {
        values.push(u64_array.value(i));
    }
    Ok(values)
}

pub fn extract_u8_list(array: &dyn Array, row_idx: usize) -> eyre::Result<Vec<u8>> {
    let list_array = array
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| eyre::eyre!("Expected ListArray"))?;

    let list_value = list_array.value(row_idx);
    let u8_array = list_value
        .as_any()
        .downcast_ref::<UInt8Array>()
        .ok_or_else(|| eyre::eyre!("Expected UInt8Array"))?;

    let mut values = Vec::new();
    for i in 0..u8_array.len() {
        values.push(u8_array.value(i));
    }
    Ok(values)
}

pub fn extract_u256_list(array: &dyn Array, row_idx: usize) -> eyre::Result<Vec<U256>> {
    let list_array = array
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| eyre::eyre!("Expected ListArray"))?;

    let list_value = list_array.value(row_idx);
    let binary_array = list_value
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .ok_or_else(|| eyre::eyre!("Expected FixedSizeBinaryArray"))?;

    let mut values = Vec::new();
    for i in 0..binary_array.len() {
        let bytes = binary_array.value(i);
        if bytes.len() == 32 {
            let mut array = [0u8; 32];
            array.copy_from_slice(bytes);
            values.push(U256::from_le_bytes(array));
        }
    }
    Ok(values)
}

pub fn extract_optional_u128_list(
    array: &dyn Array,
    row_idx: usize,
) -> eyre::Result<Vec<Option<u128>>> {
    let list_array = array
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| eyre::eyre!("Expected ListArray"))?;

    let list_value = list_array.value(row_idx);
    let binary_array = list_value
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .ok_or_else(|| eyre::eyre!("Expected FixedSizeBinaryArray"))?;

    let mut values = Vec::new();
    for i in 0..binary_array.len() {
        if binary_array.is_null(i) {
            values.push(None);
        } else {
            let bytes = binary_array.value(i);
            if bytes.len() == 16 {
                let mut array = [0u8; 16];
                array.copy_from_slice(bytes);
                values.push(Some(u128::from_le_bytes(array)));
            } else {
                values.push(None);
            }
        }
    }
    Ok(values)
}

pub fn extract_optional_string(array: &dyn Array, row_idx: usize) -> eyre::Result<Option<String>> {
    // Try Binary array first (which is what we actually have)
    if let Some(binary_array) = array.as_any().downcast_ref::<BinaryArray>() {
        if binary_array.is_null(row_idx) {
            return Ok(None);
        } else {
            let bytes = binary_array.value(row_idx);
            // Try to convert bytes to UTF-8 string, handling invalid UTF-8 gracefully
            match std::str::from_utf8(bytes) {
                Ok(s) => return Ok(Some(s.to_string())),
                Err(_) => {
                    // If not valid UTF-8, convert to hex string or use lossy conversion
                    let lossy = String::from_utf8_lossy(bytes);
                    return Ok(Some(lossy.to_string()));
                }
            }
        }
    }

    // Fallback to StringArray (in case some columns are actually UTF-8)
    if let Some(string_array) = array.as_any().downcast_ref::<StringArray>() {
        if string_array.is_null(row_idx) {
            Ok(None)
        } else {
            Ok(Some(string_array.value(row_idx).to_string()))
        }
    } else {
        Err(eyre::eyre!("Expected BinaryArray or StringArray"))
    }
}

pub fn extract_optional_u8(array: &dyn Array, row_idx: usize) -> eyre::Result<Option<u8>> {
    let u8_array = array
        .as_any()
        .downcast_ref::<UInt8Array>()
        .ok_or_else(|| eyre::eyre!("Expected UInt8Array"))?;

    if u8_array.is_null(row_idx) { Ok(None) } else { Ok(Some(u8_array.value(row_idx))) }
}

pub fn extract_optional_u64(array: &dyn Array, row_idx: usize) -> eyre::Result<Option<u64>> {
    let u64_array = array
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| eyre::eyre!("Expected UInt64Array"))?;
    if u64_array.is_null(row_idx) { Ok(None) } else { Ok(Some(u64_array.value(row_idx))) }
}

pub fn extract_optional_address(
    array: &dyn Array,
    row_idx: usize,
) -> eyre::Result<Option<Address>> {
    let binary_array = array
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .ok_or_else(|| eyre::eyre!("Expected FixedSizeBinaryArray"))?;

    if binary_array.is_null(row_idx) {
        Ok(None)
    } else {
        let bytes = binary_array.value(row_idx);
        if bytes.len() == 20 { Ok(Some(Address::from_slice(bytes))) } else { Ok(None) }
    }
}

pub fn extract_binary_list(array: &dyn Array, row_idx: usize) -> eyre::Result<Vec<Vec<u8>>> {
    let list_array = array
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| eyre::eyre!("Expected ListArray"))?;

    let list_value = list_array.value(row_idx);
    let binary_array = list_value
        .as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or_else(|| eyre::eyre!("Expected BinaryArray"))?;

    let mut binaries = Vec::new();
    for i in 0..binary_array.len() {
        let bytes = binary_array.value(i);
        // Convert bytes to string, handling invalid UTF-8 gracefully
        binaries.push(bytes.to_vec());
    }

    Ok(binaries)
}

pub fn extract_optional_binary_list(
    array: &dyn Array,
    row_idx: usize,
) -> eyre::Result<Vec<Option<Vec<u8>>>> {
    let list_array = array
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| eyre::eyre!("Expected ListArray"))?;

    let list_value = list_array.value(row_idx);
    let binary_array = list_value
        .as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or_else(|| eyre::eyre!("Expected BinaryArray"))?;

    let mut binaries = Vec::new();
    for i in 0..binary_array.len() {
        if binary_array.is_null(i) {
            binaries.push(None);
        } else {
            let bytes = binary_array.value(i);
            binaries.push(Some(bytes.to_vec()));
        }
    }
    Ok(binaries)
}
