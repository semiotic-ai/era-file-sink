use crate::pb::acme::verifiable_block::v1::{AccessTuple, BigInt, Transaction};
use decoder::transactions::error::TransactionError;
use decoder::transactions::tx_type::map_tx_type;
use reth_primitives::{
    AccessList, AccessListItem, Address, Bytes, ChainId, Signature, Transaction as RethTransaction,
    TransactionKind, TransactionSigned, TxEip1559, TxEip2930, TxLegacy, TxType, H256,
};
use std::str::FromStr;

impl TryFrom<&Transaction> for TransactionSigned {
    type Error = TransactionError;

    fn try_from(trace: &Transaction) -> Result<Self, Self::Error> {
        let transaction = RethTransaction::try_from(trace)?;
        let signature = Signature::try_from(trace)?;

        let hash = H256::from_str(&hex::encode(trace.hash.as_slice()))
            .map_err(|_| TransactionError::MissingCall)?;

        let tx_signed = TransactionSigned {
            transaction,
            signature,
            hash,
        };

        Ok(tx_signed)
    }
}

impl TryFrom<&Transaction> for RethTransaction {
    type Error = TransactionError;

    fn try_from(trace: &Transaction) -> Result<Self, Self::Error> {
        let tx_type = map_tx_type(&trace.r#type)?;

        let nonce = trace.nonce;
        let trace_gas_price = trace
            .gas_price
            .clone()
            .unwrap_or_else(|| BigInt { bytes: vec![0] });
        let gas_price = trace_gas_price.try_into()?;
        let gas_limit = trace.gas_limit;

        let to = get_tx_kind(trace)?;

        let chain_id = 1;

        let trace_value = trace
            .value
            .clone()
            .unwrap_or_else(|| BigInt { bytes: vec![0] });
        let value = trace_value.try_into()?;
        let input = Bytes::from(trace.input.as_slice());

        let transaction: RethTransaction = match tx_type {
            TxType::Legacy => {
                let v: u8 = if trace.v.is_empty() { 0 } else { trace.v[0] };

                let chain_id: Option<ChainId> = if v == 27 || v == 28 { None } else { Some(1) };

                RethTransaction::Legacy(TxLegacy {
                    chain_id,
                    nonce,
                    gas_price,
                    gas_limit,
                    to,
                    value,
                    input,
                })
            }
            TxType::EIP2930 => {
                let access_list = compute_access_list(&trace.access_list)?;

                RethTransaction::Eip2930(TxEip2930 {
                    chain_id,
                    nonce,
                    gas_price,
                    gas_limit,
                    to,
                    value,
                    access_list,
                    input,
                })
            }
            TxType::EIP1559 => {
                let access_list = compute_access_list(&trace.access_list)?;
                let trace_max_fee_per_gas = trace
                    .max_fee_per_gas
                    .clone()
                    .unwrap_or_else(|| BigInt { bytes: vec![0] });
                let max_fee_per_gas = trace_max_fee_per_gas.try_into()?;

                let trace_max_priority_fee_per_gas = trace
                    .max_priority_fee_per_gas
                    .clone()
                    .unwrap_or_else(|| BigInt { bytes: vec![0] });
                let max_priority_fee_per_gas = trace_max_priority_fee_per_gas.try_into()?;

                RethTransaction::Eip1559(TxEip1559 {
                    chain_id,
                    nonce,
                    gas_limit,
                    max_fee_per_gas,
                    max_priority_fee_per_gas,
                    to,
                    value,
                    access_list,
                    input,
                })
            }
        };

        Ok(transaction)
    }
}

fn get_tx_kind(trace: &Transaction) -> Result<TransactionKind, TransactionError> {
    let to = &trace.to;
    if to.is_empty() {
        Ok(TransactionKind::Create)
    } else {
        let address = Address::from_slice(trace.to.as_slice());
        Ok(TransactionKind::Call(address))
    }
}

fn compute_access_list(access_list: &[AccessTuple]) -> Result<AccessList, TransactionError> {
    let access_list_items: Vec<AccessListItem> = access_list
        .iter()
        .map(AccessListItem::try_from)
        .collect::<Result<Vec<AccessListItem>, TransactionError>>(
    )?;

    Ok(AccessList(access_list_items))
}
