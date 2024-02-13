use crate::pb::acme::verifiable_block::v1::AccessTuple;
use decoder::transactions::error::TransactionError;
use reth_primitives::{AccessListItem, Address, H256};

impl TryFrom<&AccessTuple> for AccessListItem {
    type Error = TransactionError;

    fn try_from(tuple: &AccessTuple) -> Result<Self, Self::Error> {
        let address: Address = Address::from_slice(tuple.address.as_slice());
        let storage_keys = tuple
            .storage_keys
            .iter()
            .map(|key| {
                let key_bytes: [u8; 32] = key
                    .as_slice()
                    .try_into()
                    .map_err(|_| TransactionError::InvalidStorageKey(hex::encode(key.clone())))?;
                Ok(H256::from(key_bytes))
            })
            .collect::<Result<Vec<H256>, TransactionError>>()?;

        Ok(AccessListItem {
            address,
            storage_keys,
        })
    }
}
