pub mod rlp_impl;
pub mod snap_utils;

use crate::e2store::snap_utils::snap_encode;
use crate::pb::acme::verifiable_block::v1::{
    AccessTuple, BigInt, BlockHeader, Log as BlockLog, Transaction, TransactionReceipt,
    VerifiableBlock,
};
use bytes::BytesMut;
use decoder::{
    headers::error,
    receipts::error::ReceiptError,
    transactions::{error::TransactionError, tx_type::map_tx_type},
};
use reth_primitives::{
    AccessList, AccessListItem, Address, BlockBody as RethBlockBody, Bloom, Bytes, ChainId, Header,
    Log, Receipt, ReceiptWithBloom, Signature, Transaction as RethTransaction, TransactionKind,
    TransactionSigned, TxEip1559, TxEip2930, TxLegacy, TxType, H256, U128,
};
use reth_rlp::Encodable as RethEncodable;
use revm_primitives::U256;
use rlp::{Encodable, RlpStream};
use std::{io::Write, str::FromStr};

const BYZANTIUM_HARDFORK: u64 = 4_370_000;

#[derive(Debug)]
pub enum E2StoreType {
    CompressedHeader = 0x03,
    CompressedBody = 0x04,
    CompressedReceipts = 0x05,
    TotalDifficulty = 0x06,
    Accumulator = 0x07,
    Version = 0x3265,
    BlockIndex = 0x3266,
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
            _ => Err(anyhow::anyhow!("Wrong Type")),
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
        let receipts: E2Store;
        if block.number < BYZANTIUM_HARDFORK {
            let receipts_vec = transactions
                .iter()
                .map(|transaction| {
                    transaction
                        .receipt
                        .clone()
                        .ok_or(anyhow::anyhow!("No receipt"))
                })
                .collect::<Result<Vec<TransactionReceipt>, anyhow::Error>>()?;
            receipts = E2Store::try_from(receipts_vec)?;
        } else {
            let receipts_vec = transactions
                .iter()
                .map(|transaction| ReceiptWithBloom::try_from(transaction.clone()))
                .collect::<Result<Vec<ReceiptWithBloom>, ReceiptError>>()?;
            receipts = E2Store::try_from(receipts_vec)?;
        }

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

impl TryFrom<Header> for E2Store {
    type Error = anyhow::Error;

    fn try_from(header: Header) -> Result<Self, Self::Error> {
        let mut bytes = BytesMut::new();
        header.encode(&mut bytes);

        let data = snap_encode(&bytes)?;

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

impl TryFrom<RethBlockBody> for E2Store {
    type Error = anyhow::Error;

    fn try_from(block_body: RethBlockBody) -> Result<Self, Self::Error> {
        let mut bytes = BytesMut::new();
        block_body.encode(&mut bytes);

        let data = snap_encode(&bytes)?;

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

impl TryFrom<Vec<ReceiptWithBloom>> for E2Store {
    type Error = anyhow::Error;

    fn try_from(receipts: Vec<ReceiptWithBloom>) -> Result<Self, Self::Error> {
        let mut bytes = BytesMut::new();
        receipts.encode(&mut bytes);
        let data = snap_encode(bytes.as_ref())?;

        Ok(E2Store {
            type_: E2StoreType::CompressedReceipts,
            length: data.len() as u32,
            reserved: 0,
            data,
        })
    }
}

impl TryFrom<Transaction> for ReceiptWithBloom {
    type Error = ReceiptError;

    fn try_from(trace: Transaction) -> Result<Self, Self::Error> {
        let success = map_success(&trace.status)?;
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

pub fn encode_bigint(big_int: BigInt) -> Vec<u8> {
    let mut bytes = big_int.bytes;
    bytes.reverse();
    if bytes.len() < 32 {
        bytes.append(&mut vec![0; 32 - bytes.len()]);
    }

    bytes
}

fn map_success(status: &i32) -> Result<bool, ReceiptError> {
    Ok(*status == 1)
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

pub(crate) fn map_logs(logs: &[BlockLog]) -> Result<Vec<Log>, ReceiptError> {
    logs.iter().map(Log::try_from).collect()
}
impl TryFrom<&BlockLog> for Log {
    type Error = ReceiptError;

    fn try_from(log: &BlockLog) -> Result<Self, Self::Error> {
        let slice: [u8; 20] = log
            .address
            .as_slice()
            .try_into()
            .map_err(|_| ReceiptError::InvalidAddress(hex::encode(log.address.clone())))?;

        let address = Address::from(slice);
        let topics = map_topics(&log.topics)?;
        let data = Bytes::from(log.data.as_slice());

        Ok(Self {
            address,
            topics,
            data,
        })
    }
}

fn map_topics(topics: &[Vec<u8>]) -> Result<Vec<H256>, ReceiptError> {
    topics.iter().map(map_topic).collect()
}

fn map_topic(topic: &Vec<u8>) -> Result<H256, ReceiptError> {
    let slice: [u8; 32] = topic
        .as_slice()
        .try_into()
        .map_err(|_| ReceiptError::InvalidTopic(hex::encode(topic)))?;
    Ok(H256::from(slice))
}

impl TryFrom<&Transaction> for RethTransaction {
    type Error = TransactionError;

    fn try_from(trace: &Transaction) -> Result<Self, Self::Error> {
        let tx_type = map_tx_type(&trace.r#type)?;

        let nonce = trace.nonce;
        let trace_gas_price = match trace.gas_price.clone() {
            Some(gas_price) => gas_price,
            None => BigInt { bytes: vec![0] },
        };
        let gas_price = trace_gas_price.try_into()?;
        let gas_limit = trace.gas_limit;

        let to = get_tx_kind(trace)?;

        let chain_id = 1;

        let trace_value = match trace.value.clone() {
            Some(value) => value,
            None => BigInt { bytes: vec![0] },
        };
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
                let trace_max_fee_per_gas = match trace.max_fee_per_gas.clone() {
                    Some(max_fee_per_gas) => max_fee_per_gas,
                    None => BigInt { bytes: vec![0] },
                };
                let max_fee_per_gas = trace_max_fee_per_gas.try_into()?;

                let trace_max_priority_fee_per_gas = match trace.max_priority_fee_per_gas.clone() {
                    Some(max_priority_fee_per_gas) => max_priority_fee_per_gas,
                    None => BigInt { bytes: vec![0] },
                };
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

pub fn get_tx_kind(trace: &Transaction) -> Result<TransactionKind, TransactionError> {
    let to = &trace.to;
    if to.is_empty() {
        Ok(TransactionKind::Create)
    } else {
        let address = Address::from_slice(trace.to.as_slice());
        Ok(TransactionKind::Call(address))
    }
}

pub(crate) fn compute_access_list(
    access_list: &[AccessTuple],
) -> Result<AccessList, TransactionError> {
    let access_list_items: Vec<AccessListItem> = access_list
        .iter()
        .map(AccessListItem::try_from)
        .collect::<Result<Vec<AccessListItem>, TransactionError>>(
    )?;

    Ok(AccessList(access_list_items))
}

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

impl TryFrom<BigInt> for u128 {
    type Error = TransactionError;

    fn try_from(value: BigInt) -> Result<Self, Self::Error> {
        let slice = value.bytes.as_slice();
        let n = U128::try_from_be_slice(slice)
            .ok_or(TransactionError::InvalidBigInt(hex::encode(slice)))?;
        Ok(u128::from_le_bytes(n.to_le_bytes()))
    }
}

impl TryFrom<&Transaction> for TransactionSigned {
    type Error = TransactionError;

    fn try_from(trace: &Transaction) -> Result<Self, Self::Error> {
        let transaction = RethTransaction::try_from(trace)?;
        let signature = Signature::try_from(trace)?;
        let hash = H256::from_str(&hex::encode(trace.hash.as_slice()))
            .map_err(|_| TransactionError::MissingCall)?;
        let tx_signed = TransactionSigned {
            transaction: transaction.clone(),
            signature: signature.clone(),
            hash,
        };
        Ok(tx_signed)
    }
}

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
        )
        .try_into()?;
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
