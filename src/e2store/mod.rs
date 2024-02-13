pub(crate) mod builder;
mod utils;

use crate::pb::acme::verifiable_block::v1::{BlockHeader, TransactionReceipt};
use crate::snap::snap_encode;
use bytes::BytesMut;
use reth_primitives::{BlockBody as RethBlockBody, Header, ReceiptWithBloom};
use reth_rlp::Encodable as RethEncodable;
use rlp::{Encodable, RlpStream};

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

#[derive(Debug)]
pub struct E2Store {
    pub(crate) type_: E2StoreType,
    pub(crate) length: u32,
    pub(crate) reserved: u16,
    pub(crate) data: Vec<u8>,
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
