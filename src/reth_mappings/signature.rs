use crate::pb::acme::verifiable_block::v1::Transaction;
use decoder::transactions::error::TransactionError;
use reth_primitives::{Signature, U256};

impl TryFrom<&Transaction> for Signature {
    type Error = TransactionError;

    fn try_from(trace: &Transaction) -> Result<Self, Self::Error> {
        let r_bytes: [u8; 32] = trace
            .r
            .as_slice()
            .try_into()
            .map_err(|_| TransactionError::MissingValue)?;
        let r = U256::from_be_bytes(r_bytes);

        let s_bytes: [u8; 32] = trace
            .s
            .as_slice()
            .try_into()
            .map_err(|_| TransactionError::MissingValue)?;
        let s = U256::from_be_bytes(s_bytes);

        let odd_y_parity = get_y_parity(trace)?;

        Ok(Signature { r, s, odd_y_parity })
    }
}

fn get_y_parity(trace: &Transaction) -> Result<bool, TransactionError> {
    let v: u8 = if trace.v.is_empty() { 0 } else { trace.v[0] };

    if v == 0 || v == 1 {
        Ok(v == 1)
    } else if v == 27 || v == 28 {
        Ok(v - 27 == 1)
    } else if v == 37 || v == 38 {
        Ok(v - 37 == 1)
    } else {
        Err(TransactionError::MissingValue)
    }
}
