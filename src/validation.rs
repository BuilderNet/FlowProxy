use alloy_eips::eip7825::MAX_TX_GAS_LIMIT_OSAKA;
use alloy_hardforks::mainnet::MAINNET_OSAKA_TIMESTAMP;
use revm_primitives::hardfork::SpecId;

/// Mainnet chain ID.
pub const MAINNET_CHAIN_ID: u64 = 1;

/// Maximum initcode to permit in a creation transaction and create instructions.
///
/// Limit of maximum initcode size is `2 * MAX_CODE_SIZE`.
pub const MAX_INIT_CODE_BYTE_SIZE: usize = 2 * 0x6000;

#[derive(thiserror::Error, Debug)]
pub enum ValidationError {
    /// Thrown when a new transaction is added to the pool, but then immediately discarded to
    /// respect the [`MAX_INIT_CODE_BYTE_SIZE`].
    #[error("transaction's input size {0} exceeds max_init_code_size {1}")]
    ExceedsMaxInitCodeSize(usize, usize),
    /// Thrown when the transaction gas limit exceeds the maximum allowed.
    #[error("transaction gas limit {0} exceeds the cap {1}")]
    ExceedsTxGasLimitCap(u64, u64),
    /// Thrown to ensure no one is able to specify a transaction with a tip higher than the total
    /// fee cap.
    #[error("max priority fee per gas higher than max fee per gas")]
    TipAboveFeeCap,
    /// The chain ID in the transaction does not match the current network configuration.
    #[error("transaction's chain ID does not match")]
    ChainIdMismatch,
    /// Thrown if the transaction has no items in its authorization list
    #[error("no items in authorization list for EIP7702 transaction")]
    MissingEip7702AuthorizationList,
    /// The transaction is specified to use less gas than required to start the
    /// invocation.
    #[error("intrinsic gas too low")]
    IntrinsicGasTooLow,
    /// Thrown if an EIP-4844 transaction without any blobs arrives
    #[error("blobless blob transaction")]
    NoEip4844Blobs,
}

pub fn validate_transaction(
    transaction: &impl alloy_consensus::Transaction,
    timestamp: u64,
) -> Result<(), ValidationError> {
    // Validate input length.
    let input_len = transaction.input().len();
    if transaction.is_create() && input_len > MAX_INIT_CODE_BYTE_SIZE {
        return Err(ValidationError::ExceedsMaxInitCodeSize(input_len, MAX_INIT_CODE_BYTE_SIZE));
    }

    // Ensure max_priority_fee_per_gas (if EIP1559) is less than max_fee_per_gas if any.
    if transaction.max_priority_fee_per_gas() > Some(transaction.max_fee_per_gas()) {
        return Err(ValidationError::TipAboveFeeCap);
    }

    // Checks for chainid
    if let Some(chain_id) = transaction.chain_id() &&
        chain_id != MAINNET_CHAIN_ID
    {
        return Err(ValidationError::ChainIdMismatch);
    }

    let spec_id = spec_by_timestamp(timestamp);
    let gas_limit = transaction.gas_limit();
    if spec_id >= SpecId::OSAKA && gas_limit > MAX_TX_GAS_LIMIT_OSAKA {
        return Err(ValidationError::ExceedsTxGasLimitCap(gas_limit, MAX_TX_GAS_LIMIT_OSAKA));
    }

    let gas = revm_interpreter::gas::calculate_initial_tx_gas(
        spec_id,
        transaction.input(),
        transaction.is_create(),
        transaction.access_list().map(|l| l.len()).unwrap_or_default() as u64,
        transaction
            .access_list()
            .map(|l| l.iter().map(|i| i.storage_keys.len()).sum::<usize>())
            .unwrap_or_default() as u64,
        transaction.authorization_list().map(|l| l.len()).unwrap_or_default() as u64,
    );
    if gas_limit < gas.initial_gas || gas_limit < gas.floor_gas {
        return Err(ValidationError::IntrinsicGasTooLow);
    }

    if transaction.is_eip4844() {
        let blob_count = transaction.blob_versioned_hashes().map(|b| b.len() as u64).unwrap_or(0);
        if blob_count == 0 {
            // no blobs
            return Err(ValidationError::NoEip4844Blobs);
        }
    }

    if transaction.is_eip7702() && transaction.authorization_list().is_none_or(|l| l.is_empty()) {
        return Err(ValidationError::MissingEip7702AuthorizationList);
    }

    Ok(())
}

/// Determine revm's [`SpecId`] based on the current timestamp.
/// Earliest spec supported by the proxy is [`SpecId::PRAGUE`].
pub fn spec_by_timestamp(timestamp: u64) -> SpecId {
    if timestamp >= MAINNET_OSAKA_TIMESTAMP { SpecId::OSAKA } else { SpecId::PRAGUE }
}
