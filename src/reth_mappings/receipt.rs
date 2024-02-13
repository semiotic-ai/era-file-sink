use crate::pb::acme::verifiable_block::v1::Transaction;
use decoder::receipts::error::ReceiptError;
use decoder::transactions::tx_type::map_tx_type;
use reth_primitives::{Bloom, Log, Receipt, ReceiptWithBloom};

impl TryFrom<Transaction> for ReceiptWithBloom {
    type Error = ReceiptError;

    fn try_from(trace: Transaction) -> Result<Self, Self::Error> {
        let success = map_success(&trace.status);
        let tx_type = map_tx_type(&trace.r#type)?;
        let trace_receipt = match &trace.receipt {
            Some(receipt) => receipt,
            None => return Err(ReceiptError::MissingReceipt),
        };
        let logs: Vec<Log> = map_logs(&trace_receipt.logs)?;
        let cumulative_gas_used = trace_receipt.cumulative_gas_used;

        let receipt = Receipt {
            success,
            tx_type,
            logs,
            cumulative_gas_used,
        };

        let bloom = map_bloom(&trace_receipt.logs_bloom)?;

        Ok(Self { receipt, bloom })
    }
}

fn map_success(status: &i32) -> bool {
    *status == 1
}

fn map_bloom(slice: &[u8]) -> Result<Bloom, ReceiptError> {
    if slice.len() == 256 {
        let array: [u8; 256] = slice
            .try_into()
            .expect("Slice length doesn't match array length");
        Ok(Bloom(array))
    } else {
        Err(ReceiptError::InvalidBloom(hex::encode(slice)))
    }
}

fn map_logs(logs: &[crate::pb::acme::verifiable_block::v1::Log]) -> Result<Vec<Log>, ReceiptError> {
    logs.iter().map(Log::try_from).collect()
}
