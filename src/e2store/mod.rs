use std::io::Write;
use rlp::RlpStream;
use snap::raw::max_compress_len;
use crate::pb::acme::verifiable_block::v1::{BlockHeader, Transaction, TransactionReceipt, VerifiableBlock};

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
                type_: 0x3265,
                length: 0,
                reserved: 0,
                data: Vec::new(),
            };
            let version = version.into_bytes();

            self.writer.write_all(&version)?;
            self.bytes_written += version.len() as u64;
            self.starting_number = block.number as i64;
        }

        let block_header = block.header.clone().ok_or(anyhow::anyhow!("No header"))?;
        let total_difficulty = block_header.total_difficulty.clone()
            .ok_or(anyhow::anyhow!("No total difficulty"))?;
        let header = E2Store::try_from(block_header)?;
        let header = header.into_bytes();
        self.writer.write_all(&header)?;
        self.bytes_written += header.len() as u64;

        let body = BlockBody {
            transactions: block.transactions.clone(),
            uncles: block.uncles.clone(),
        };
        let body = E2Store::try_from(body)?.into_bytes();
        self.writer.write_all(&body)?;
        self.bytes_written += body.len() as u64;

        let receipts = block.transactions.iter().map(|transaction| {
            transaction.receipt.clone().ok_or(anyhow::anyhow!("No receipt"))
        }).collect::<Result<Vec<TransactionReceipt>, anyhow::Error>>()?;
        let receipts = E2Store::try_from(receipts)?;
        let receipts = receipts.into_bytes();

        self.writer.write_all(&receipts)?;
        self.bytes_written += receipts.len() as u64;

        // let total_difficulty = block_header.total_difficulty.ok_or(anyhow::anyhow!("No total difficulty"))?;
        let total_difficulty = total_difficulty.bytes;
        let total_difficulty = E2Store {
            type_: 0x06,
            length: total_difficulty.len() as u32,
            reserved: 0,
            data: total_difficulty,
        };
        let total_difficulty = total_difficulty.into_bytes();
        self.writer.write_all(&total_difficulty)?;
        self.bytes_written += total_difficulty.len() as u64;

        self.indexes.push(self.bytes_written);
        Ok(())
    }

    pub fn finalize(self: &mut Self, header_accumulator: Vec<u8>) -> Result<(), anyhow::Error> {
        let header_accumulator = E2Store {
            type_: 0x07,
            length: header_accumulator.len() as u32,
            reserved: 0,
            data: header_accumulator,
        };

        let header_accumulator = header_accumulator.into_bytes();
        self.writer.write(&header_accumulator)?;
        self.bytes_written += header_accumulator.len() as u64;

        let mut indexes_out = Vec::new();

        let starting_number = self.starting_number.to_le_bytes();
        indexes_out.push(starting_number);

        let base: i64 = self.bytes_written as i64 + 3*8; // skip e2store header (type, length) and start block
        for (idx, offset) in self.indexes.iter().enumerate() {
            let relative: i64 = *offset as i64 - base - idx as i64 * 8;
            indexes_out.push(relative.to_le_bytes());
        }

        indexes_out.push(self.indexes.len().to_le_bytes());

        let indexes_out = E2Store {
            type_: 0x3266,
            length: indexes_out.len() as u32,
            reserved: 0,
            data: indexes_out.into_iter().flatten().collect(),
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

pub struct E2Store {
    type_: u16,
    length: u32,
    reserved: u16,
    data: Vec<u8>,
}

pub struct BlockBody {
    transactions: Vec<Transaction>,
    uncles: Vec<BlockHeader>
}

impl E2Store {
    pub fn into_bytes(self) -> Vec<u8> {
        let mut vec = Vec::new();
        vec.extend_from_slice(&self.type_.to_le_bytes());
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
        let mut rlp_encoded = RlpStream::new();
        rlp_encoded.append(&block_header.parent_hash);
        rlp_encoded.append(&block_header.uncle_hash);
        rlp_encoded.append(&block_header.coinbase);
        rlp_encoded.append(&block_header.state_root);
        rlp_encoded.append(&block_header.transactions_root);
        rlp_encoded.append(&block_header.receipt_root);
        rlp_encoded.append(&block_header.logs_bloom);
        rlp_encoded.append(&block_header.difficulty.unwrap().bytes.as_slice());
        rlp_encoded.append(&block_header.number.to_le_bytes().as_slice());
        rlp_encoded.append(&block_header.gas_limit.to_le_bytes().as_slice());
        rlp_encoded.append(&block_header.gas_used.to_le_bytes().as_slice());
        rlp_encoded.append(&block_header.timestamp
            .ok_or(anyhow::anyhow!("Missing timestamp"))?.seconds.to_le_bytes().as_slice());
        rlp_encoded.append(&block_header.extra_data);
        rlp_encoded.append(&block_header.mix_hash);
        rlp_encoded.append(&block_header.nonce);

        let bytes = rlp_encoded.out();

        // Snappy compression
        let mut encoder = snap::raw::Encoder::new();
        let mut data = vec![0; max_compress_len(bytes.len())];
        encoder.compress(&bytes, &mut data)?;

        Ok(E2Store {
            type_: 0x03,
            length: data.len() as u32,
            reserved: 0,
            data,
        })
    }
}

impl TryFrom<BlockBody> for E2Store {
    type Error = anyhow::Error;

    fn try_from(value: BlockBody) -> Result<Self, Self::Error> {

        let mut rlp_encoded = RlpStream::new();

        for transaction in value.transactions.iter() {
            // TODO: might need to be a list of txs, each rlp encoded
            rlp_encoded.append(&transaction.to);
            rlp_encoded.append(&transaction.nonce.to_le_bytes().as_slice());
            // rlp_encoded.append(&transaction.gas_price.clone()
            //     .ok_or(anyhow::anyhow!("Missing gas price"))?.bytes.as_slice());

            match transaction.gas_price.clone() {
                Some(gas_price) => {
                    rlp_encoded.append(&gas_price.bytes.as_slice());
                },
                None => {
                    rlp_encoded.append(&[0u8].as_slice());
                    println!("Missing gas price")
                }
            }

            rlp_encoded.append(&transaction.gas_limit.to_le_bytes().as_slice());
            // rlp_encoded.append(&transaction.value.clone()
            //     .ok_or(anyhow::anyhow!("Missing tx value"))?.bytes.as_slice());

            match transaction.value.clone() {
                Some(value) => {
                    rlp_encoded.append(&value.bytes.as_slice());
                },
                None => {
                    rlp_encoded.append(&[0u8].as_slice());
                    println!("Missing tx value")
                }
            }

            rlp_encoded.append(&transaction.input);
            rlp_encoded.append(&transaction.v.as_slice());
            rlp_encoded.append(&transaction.r.as_slice());
            rlp_encoded.append(&transaction.s.as_slice());
            rlp_encoded.append(&transaction.r#type.to_le_bytes().as_slice());

            transaction.access_list.iter().for_each(|access| {
                rlp_encoded.append(&access.address);
                rlp_encoded.append(&access.storage_keys.iter().flatten().map(|ptr| ptr.clone()).collect::<Vec<u8>>());
            });
        };

        for uncle in value.uncles.iter() {
            let uncle = E2Store::try_from(uncle.clone())?;
            rlp_encoded.append(&uncle.into_bytes());
        }

        let bytes = rlp_encoded.out();
        let mut encoder = snap::raw::Encoder::new();

        let mut data = vec![0; max_compress_len(bytes.len())];
        encoder.compress(&bytes, &mut data)?;

        Ok(E2Store {
            type_: 0x04,
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

        receipts.iter().for_each(|receipt| {
            // TODO: Check if this is correct, might need to be a list of receipts
            rlp_encoded.append(&receipt.state_root);
            rlp_encoded.append(&receipt.cumulative_gas_used.to_le_bytes().as_slice());
            rlp_encoded.append(&receipt.logs_bloom);

            receipt.logs.iter().for_each(|log| { // TODO: Check
                rlp_encoded.append(&log.address);
                rlp_encoded.append(&log.topics.iter().flatten().map(|ptr| ptr.clone()).collect::<Vec<u8>>());
                rlp_encoded.append(&log.data);
            });
        });

        let bytes = rlp_encoded.out();
        let mut encoder = snap::raw::Encoder::new();

        let mut data = vec![0; max_compress_len(bytes.len())];
        encoder.compress(&bytes, &mut data)?;

        Ok(E2Store {
            type_: 0x05,
            length: data.len() as u32,
            reserved: 0,
            data,
        })

    }
}


