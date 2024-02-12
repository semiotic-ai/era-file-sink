use crate::e2store::utils::encode_bigint;
use crate::e2store::{E2Store, E2StoreType, BYZANTIUM_HARDFORK};
use crate::pb::acme::verifiable_block::v1::{TransactionReceipt, VerifiableBlock};
use decoder::receipts::error::ReceiptError;
use reth_primitives::{BlockBody as RethBlockBody, Header, ReceiptWithBloom, TransactionSigned};
use std::io::Write;

pub struct EraBuilder<W: Write> {
    writer: W,
    bytes_written: u64,
    indexes: Vec<u64>,
    pub(crate) starting_number: i64,
}

impl<W: Write> EraBuilder<W> {
    pub fn new(writer: W) -> Self {
        Self {
            writer,
            bytes_written: 0,
            indexes: Vec::new(),
            starting_number: -1,
        }
    }

    pub fn add(&mut self, block: VerifiableBlock) -> Result<(), anyhow::Error> {
        if self.starting_number == -1 {
            let version = E2Store {
                type_: E2StoreType::Version,
                length: 0,
                reserved: 0,
                data: Vec::new(),
            };
            let version = version.into_bytes();

            self.writer.write_all(&version)?;
            self.bytes_written += version.len() as u64;
            self.starting_number = block.number as i64;
        }

        self.indexes.push(self.bytes_written);
        let header = block.header.clone().ok_or(anyhow::anyhow!("No header"))?;
        let block_header = Header::try_from(&header)?;
        let total_difficulty = header
            .total_difficulty
            .ok_or(anyhow::anyhow!("No total difficulty"))?;
        let header = E2Store::try_from(block_header)?;
        let header = header.into_bytes();
        self.writer.write_all(&header)?;
        self.bytes_written += header.len() as u64;

        let transactions = if block.number == 0 {
            Vec::new()
        } else {
            block.transactions
        };

        let reth_body = RethBlockBody {
            transactions: transactions
                .clone()
                .into_iter()
                .map(|tx| TransactionSigned::try_from(&tx.clone()).unwrap())
                .collect(),
            ommers: block
                .uncles
                .clone()
                .into_iter()
                .map(|uncle| Header::try_from(&uncle.clone()).unwrap())
                .collect(),
            withdrawals: None,
        };

        let body = E2Store::try_from(reth_body)?.into_bytes();

        self.writer.write_all(&body)?;
        self.bytes_written += body.len() as u64;
        let receipts = if block.number < BYZANTIUM_HARDFORK {
            let receipts_vec = transactions
                .iter()
                .map(|transaction| {
                    transaction
                        .receipt
                        .clone()
                        .ok_or(anyhow::anyhow!("No receipt"))
                })
                .collect::<Result<Vec<TransactionReceipt>, anyhow::Error>>()?;
            E2Store::try_from(receipts_vec)?
        } else {
            let receipts_vec = transactions
                .iter()
                .map(|transaction| ReceiptWithBloom::try_from(transaction.clone()))
                .collect::<Result<Vec<ReceiptWithBloom>, ReceiptError>>()?;
            E2Store::try_from(receipts_vec)?
        };

        let receipts = receipts.into_bytes();

        self.writer.write_all(&receipts)?;
        self.bytes_written += receipts.len() as u64;

        let total_difficulty = encode_bigint(total_difficulty);
        let total_difficulty = E2Store {
            type_: E2StoreType::TotalDifficulty,
            length: total_difficulty.len() as u32,
            reserved: 0,
            data: total_difficulty,
        };
        let total_difficulty = total_difficulty.into_bytes();
        self.writer.write_all(&total_difficulty)?;
        self.bytes_written += total_difficulty.len() as u64;

        Ok(())
    }

    pub fn finalize(&mut self, header_accumulator: Vec<u8>) -> Result<(), anyhow::Error> {
        let header_accumulator = E2Store {
            type_: E2StoreType::Accumulator,
            length: header_accumulator.len() as u32,
            reserved: 0,
            data: header_accumulator,
        };

        let header_accumulator = header_accumulator.into_bytes();
        self.writer.write_all(&header_accumulator)?;
        self.bytes_written += header_accumulator.len() as u64;

        let count = self.indexes.len();
        let length = 16 + 8 * count;
        let mut buf = vec![0; length];
        let indexes_out = buf.as_mut_slice();
        indexes_out[0..8].copy_from_slice(&(self.starting_number as u64).to_le_bytes());

        let base: i64 = self.bytes_written as i64 + 3 * 8; // skip e2store header (type, length) and start block
        for (idx, offset) in self.indexes.iter().enumerate() {
            let relative: u64 = (*offset as i64 - base - idx as i64 * 8) as u64;
            let start_idx = 8 + idx * 8;
            indexes_out[start_idx..start_idx + 8].copy_from_slice(&relative.to_le_bytes());
        }

        indexes_out[length - 8..].copy_from_slice(&(count as u64).to_le_bytes());

        let indexes_out = E2Store {
            type_: E2StoreType::BlockIndex,
            length: length as u32,
            reserved: 0,
            data: indexes_out.to_vec(),
        };

        let indexes_out = indexes_out.into_bytes();
        self.writer.write_all(&indexes_out)?;
        self.bytes_written += indexes_out.len() as u64;
        Ok(())
    }

    pub fn reset(&mut self, writer: W) {
        self.bytes_written = 0;
        self.indexes = Vec::new();
        self.starting_number = -1;
        self.writer = writer;
    }

    pub fn len(&self) -> usize {
        self.indexes.len()
    }
}
