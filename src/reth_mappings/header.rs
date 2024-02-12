use crate::pb::acme::verifiable_block::v1::BlockHeader;
use decoder::headers::error;
use reth_primitives::{Address, Bloom, Bytes, Header, H256, U256};

impl TryFrom<&BlockHeader> for Header {
    type Error = anyhow::Error;

    fn try_from(block_header: &BlockHeader) -> Result<Self, Self::Error> {
        let parent_hash = H256::from_slice(block_header.parent_hash.as_slice());
        let ommers_hash = H256::from_slice(block_header.uncle_hash.as_slice());
        let beneficiary = Address::from_slice(block_header.coinbase.as_slice());
        let state_root = H256::from_slice(block_header.state_root.as_slice());
        let transactions_root = H256::from_slice(block_header.transactions_root.as_slice());
        let receipts_root = H256::from_slice(block_header.receipt_root.as_slice());
        let logs_bloom = Bloom::from_slice(block_header.logs_bloom.as_slice());
        let difficulty = U256::from_be_slice(
            block_header
                .difficulty
                .as_ref()
                .ok_or(error::BlockHeaderError::InvalidInput)?
                .bytes
                .as_slice(),
        );
        let number = block_header.number;
        let gas_limit = block_header.gas_limit;
        let gas_used = block_header.gas_used;
        let timestamp = block_header
            .timestamp
            .clone()
            .ok_or(error::BlockHeaderError::InvalidInput)?
            .seconds as u64;
        let extra_data = Bytes::from(block_header.extra_data.as_slice());
        let mix_hash = H256::from_slice(block_header.mix_hash.as_slice());
        let nonce = block_header.nonce;
        let withdrawals_root = match block_header.withdrawals_root.is_empty() {
            true => None,
            false => Some(H256::from_slice(block_header.withdrawals_root.as_slice())),
        };
        let base_fee_per_gas = match block_header.base_fee_per_gas.as_ref() {
            Some(base_fee_per_gas) => {
                let bytes = base_fee_per_gas.bytes.as_slice();
                // if bytes is empty return None, else return u64 converted from bytes
                match bytes.is_empty() {
                    true => None,
                    false => Some(U256::from_be_slice(bytes).try_into()?),
                }
            }

            None => None,
        };
        Ok(Header {
            parent_hash,
            ommers_hash,
            beneficiary,
            state_root,
            transactions_root,
            receipts_root,
            withdrawals_root,
            logs_bloom,
            difficulty,
            number,
            gas_limit,
            gas_used,
            timestamp,
            extra_data,
            mix_hash,
            nonce,
            base_fee_per_gas,
        })
    }
}
