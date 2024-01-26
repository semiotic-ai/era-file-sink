pub mod rlp_impl;
pub mod snap_utils;

use crate::pb::acme::verifiable_block::v1::{BigInt, BlockHeader, Transaction, TransactionReceipt, VerifiableBlock};
use prost::Message;
use rlp::{Encodable, RlpStream};
use std::io::Write;
use crate::e2store::snap_utils::snap_encode;

#[derive(Debug)]
pub enum E2StoreType {
    CompressedHeader = 0x03,
    CompressedBody = 0x04,
    CompressedReceipts = 0x05,
    TotalDifficulty = 0x06,
    Accumulator = 0x07,
    Version = 0x3265,
    BlockIndex = 0x3266
}

impl TryInto<E2StoreType> for u16 {
    type Error = anyhow::Error;

    fn try_into(self) -> Result<E2StoreType, Self::Error> {
        match self {
            0x03 => Ok(E2StoreType::CompressedHeader),
            0x04 => Ok(E2StoreType::CompressedBody),
            0x05 => Ok(E2StoreType::CompressedReceipts),
            0x06 => Ok(E2StoreType::TotalDifficulty),
            0x07 => Ok(E2StoreType::Accumulator),
            0x3265 => Ok(E2StoreType::Version),
            0x3266 => Ok(E2StoreType::BlockIndex),
            _ => Err(anyhow::anyhow!("Wrong Type"))
        }
    }
}

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

    pub fn add(self: &mut Self, block: VerifiableBlock) -> Result<(), anyhow::Error> {
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

        let block_header = block.header.clone().ok_or(anyhow::anyhow!("No header"))?;
        let total_difficulty = block_header
            .total_difficulty
            .clone()
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

        let body = BlockBody {
            transactions: transactions.clone(),
            uncles: block.uncles.clone(),
        };

        let body = E2Store::try_from(body)?.into_bytes();

        self.writer.write_all(&body)?;
        self.bytes_written += body.len() as u64;

        let receipts = transactions
            .iter()
            .map(|transaction| {
                transaction
                    .receipt
                    .clone()
                    .ok_or(anyhow::anyhow!("No receipt"))
            })
            .collect::<Result<Vec<TransactionReceipt>, anyhow::Error>>()?;
        let receipts = E2Store::try_from(receipts)?;
        let receipts = receipts.into_bytes();

        self.writer.write_all(&receipts)?;
        self.bytes_written += receipts.len() as u64;

        let mut total_difficulty = encode_bigint(total_difficulty);
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

    pub fn finalize(self: &mut Self, header_accumulator: Vec<u8>) -> Result<(), anyhow::Error> {
        let header_accumulator = E2Store {
            type_: E2StoreType::Accumulator,
            length: header_accumulator.len() as u32,
            reserved: 0,
            data: header_accumulator,
        };

        let header_accumulator = header_accumulator.into_bytes();
        self.writer.write(&header_accumulator)?;
        self.bytes_written += header_accumulator.len() as u64;

        // let mut indexes_out = Vec::new();
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
        self.writer.write(&indexes_out)?;
        self.bytes_written += indexes_out.len() as u64;
        Ok(())
    }

    pub fn reset(self: &mut Self, writer: W) {
        self.bytes_written = 0;
        self.indexes = Vec::new();
        self.starting_number = -1;
        self.writer = writer;
    }

    pub fn len(&self) -> usize {
        self.indexes.len()
    }
}

#[derive(Debug)]
pub struct E2Store {
    pub(crate) type_: E2StoreType,
    pub(crate) length: u32,
    pub(crate) reserved: u16,
    pub(crate) data: Vec<u8>,
}

pub struct BlockBody {
    transactions: Vec<Transaction>,
    uncles: Vec<BlockHeader>,
}

impl E2Store {
    pub fn into_bytes(self) -> Vec<u8> {
        let mut vec = Vec::new();
        vec.extend_from_slice(&(self.type_ as u16).to_le_bytes());
        vec.extend_from_slice(&self.length.to_le_bytes());
        vec.extend_from_slice(&self.reserved.to_le_bytes());
        vec.extend_from_slice(&self.data);
        vec
    }
}

impl TryFrom<BlockHeader> for E2Store {
    type Error = anyhow::Error;

    fn try_from(block_header: BlockHeader) -> Result<Self, Self::Error> {
        // block_header.
        let bytes = block_header.rlp_bytes();

        // Snappy compression
        let data = snap_encode(bytes.as_ref())?;

        Ok(E2Store {
            type_: E2StoreType::CompressedHeader,
            length: data.len() as u32,
            reserved: 0,
            data,
        })
    }
}

impl TryFrom<BlockBody> for E2Store {
    type Error = anyhow::Error;

    fn try_from(block_body: BlockBody) -> Result<Self, Self::Error> {
        let bytes = block_body.rlp_bytes();

        let data = snap_encode(bytes.as_ref())?;

        Ok(E2Store {
            type_: E2StoreType::CompressedBody,
            length: data.len() as u32,
            reserved: 0,
            data,
        })
    }
}

impl TryFrom<Vec<TransactionReceipt>> for E2Store {
    type Error = anyhow::Error;

    fn try_from(receipts: Vec<TransactionReceipt>) -> Result<Self, Self::Error> {
        let mut rlp_encoded = RlpStream::new();
        rlp_encoded.append_list(receipts.as_slice());

        let bytes = rlp_encoded.out();

        let data = snap_encode(bytes.as_ref())?;

        Ok(E2Store {
            type_: E2StoreType::CompressedReceipts,
            length: data.len() as u32,
            reserved: 0,
            data,
        })
    }
}

pub fn encode_bigint(big_int: BigInt) -> Vec<u8> {
    let mut bytes = big_int.bytes;
    bytes.reverse();
    if bytes.len() < 32 {
        bytes.append(&mut vec![0; 32 - bytes.len()]);
    }

    bytes
}
