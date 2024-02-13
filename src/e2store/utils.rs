use crate::pb::acme::verifiable_block::v1::BigInt;
use decoder::transactions::error::TransactionError;
use reth_primitives::U128;

pub fn encode_bigint(big_int: BigInt) -> Vec<u8> {
    let mut bytes = big_int.bytes;
    bytes.reverse();
    if bytes.len() < 32 {
        bytes.append(&mut vec![0; 32 - bytes.len()]);
    }

    bytes
}

impl TryFrom<BigInt> for u128 {
    type Error = TransactionError;

    fn try_from(value: BigInt) -> Result<Self, Self::Error> {
        let slice = value.bytes.as_slice();
        let n = U128::try_from_be_slice(slice)
            .ok_or(TransactionError::InvalidBigInt(hex::encode(slice)))?;
        Ok(u128::from_le_bytes(n.to_le_bytes()))
    }
}
