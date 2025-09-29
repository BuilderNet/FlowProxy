pub(crate) struct BundleRow {
    /// The timestamp at which the bundle was observed.
    #[serde(with = "clickhouse::serde::time::datetime64::micros")]
    pub time: OffsetDateTime,
    #[serde(rename = "transactions.hash", with = "hashes")]
    /// Collection of hashes for transactions in the bundle.
    pub transactions_hash: Vec<B256>,
    /// Collection of from addresses for transactions in the bundle.
    #[serde(rename = "transactions.from", with = "addresses")]
    pub transactions_from: Vec<Address>,
    /// Collection of nonces for transactions in the bundle.
    #[serde(rename = "transactions.nonce")]
    pub transactions_nonce: Vec<u64>,
    /// Collection of signature `r` values for transactions in the bundle.
    #[serde(rename = "transactions.r", with = "u256es")]
    pub transactions_r: Vec<U256>,
    /// Collection of signature `s` values for transactions in the bundle.
    #[serde(rename = "transactions.s", with = "u256es")]
    pub transactions_s: Vec<U256>,
    /// Collection of signature `v` values for transactions in the bundle.
    #[serde(rename = "transactions.v")]
    pub transactions_v: Vec<u8>,
    /// Collection of to addresses for transactions in the bundle.
    #[serde(rename = "transactions.to", with = "addresses::option")]
    pub transactions_to: Vec<Option<Address>>,
    /// Collection of gas limit values for transactions in the bundle.
    #[serde(rename = "transactions.gas")]
    pub transactions_gas: Vec<u64>,
    /// Collection of transaction types for transactions in the bundle.
    #[serde(rename = "transactions.type")]
    pub transactions_type: Vec<u64>,
    /// Collection of inputs for transactions in the bundle.
    #[serde(rename = "transactions.input")]
    pub transactions_input: Vec<String>,
    /// Collection of values for transactions in the bundle.
    #[serde(rename = "transactions.value", with = "u256es")]
    pub transactions_value: Vec<U256>,
    /// Collection of gas prices for transactions in the bundle.
    #[serde(rename = "transactions.gasPrice")]
    pub transactions_gas_price: Vec<Option<u128>>,
    /// Collection of max fee per gas values for transactions in the bundle.
    #[serde(rename = "transactions.maxFeePerGas")]
    pub transactions_max_fee_per_gas: Vec<Option<u128>>,
    /// Collection of max priority fee per gas values for transactions in the bundle.
    #[serde(rename = "transactions.maxPriorityFeePerGas")]
    pub transactions_max_priority_fee_per_gas: Vec<Option<u128>>,
    /// Collection of access lists for transactions in the bundle.
    #[serde(rename = "transactions.accessList")]
    pub transactions_access_list: Vec<Option<String>>,
    /// Collection of authorization lists for transactions in the bundle.
    #[serde(rename = "transactions.authorizationList")]
    pub transactions_authorization_list: Vec<Option<String>>,

    /// Bundle block number.
    pub block_number: Option<u64>,
    /// Minimum timestamp for the bundle.
    pub min_timestamp: Option<u64>,
    /// Maximum timestamp for the bundle.
    pub max_timestamp: Option<u64>,

    /// Collection of reverting transaction hashes.
    #[serde(with = "hashes")]
    pub reverting_tx_hashes: Vec<B256>,
    /// Collection of dropping transaction hashes.
    #[serde(with = "hashes")]
    pub dropping_tx_hashes: Vec<B256>,
    /// Collection of refund transaction hashes.
    #[serde(with = "hashes")]
    pub refund_tx_hashes: Vec<B256>,

    /// The hash of the bundle (unique identifier)
    #[serde(with = "hash")]
    pub hash: B256,
    /// Bundle uuid.
    #[serde(with = "clickhouse::serde::uuid::option")]
    pub internal_uuid: Option<Uuid>,
    /// Replacement bundle uuid.
    #[serde(with = "clickhouse::serde::uuid::option")]
    pub replacement_uuid: Option<Uuid>,
    pub replacement_nonce: Option<u64>,
    /// Bundle refund percent.
    pub refund_percent: Option<u8>,
    /// Bundle refund recipient.
    #[serde(with = "address::option")]
    pub refund_recipient: Option<Address>,
    /// The signer of the bundle,
    #[serde(with = "address::option")]
    pub signer_address: Option<Address>,
    /// For 2nd price refunds done by buildernet
    #[serde(with = "address::option")]
    pub refund_identity: Option<Address>,

    /// Builder name.
    pub builder_name: String,
}
