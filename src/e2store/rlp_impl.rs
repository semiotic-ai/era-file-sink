use crate::e2store::BlockBody;
use crate::pb::acme::verifiable_block::v1::{
    AccessTuple, BigInt, BlockHeader, Log, Transaction, TransactionReceipt,
};
use rlp::{Encodable, RlpStream};

impl Encodable for BlockHeader {
    fn rlp_append(&self, s: &mut RlpStream) {
        s.begin_unbounded_list()
            .append(&self.parent_hash)
            .append(&self.uncle_hash)
            .append(&self.coinbase)
            .append(&self.state_root)
            .append(&self.transactions_root)
            .append(&self.receipt_root)
            .append(&self.logs_bloom)
            .append(&self.difficulty.clone().expect("Missing difficulty").bytes)
            .append(&encode_number(self.number))
            .append(&encode_number(self.gas_limit))
            .append(&encode_number(self.gas_used))
            .append(&encode_number(
                self.timestamp.clone().expect("Missing timestamp").seconds as u64,
            ))
            .append(&self.extra_data)
            .append(&self.mix_hash)
            .append(&self.nonce.to_be_bytes().as_slice())
            .finalize_unbounded_list();
    }
}

fn encode_number(n: u64) -> Vec<u8> {
    let leading_zeros = n.leading_zeros();
    let bytes = n.to_be_bytes();
    bytes[(leading_zeros / 8) as usize..].to_vec()
}

impl Encodable for BlockBody {
    fn rlp_append(&self, s: &mut RlpStream) {
        s.begin_unbounded_list()
            .append_list(&self.transactions)
            .append_list(&self.uncles)
            .finalize_unbounded_list();
    }
}

impl Encodable for Transaction {
    fn rlp_append(&self, s: &mut RlpStream) {
        if self.r#type == 2 {
            s.append(&encode_number(2u64));
        } else if self.r#type == 1 {
            s.append(&encode_number(1u64));
        }
        s.begin_unbounded_list();
        if self.r#type == 1 || self.r#type == 2 {
            s.append(&encode_number(1u64));
        }
        s.append(&encode_number(self.nonce));

        match self.gas_price.clone() {
            Some(gas_price) => {
                s.append(&gas_price.bytes.as_slice()); // TODO: check if this is correct
            }
            None => {
                let gas_price = BigInt { bytes: vec![0] };
                s.append(&gas_price.bytes.as_slice());
            }
        }

        s.append(&encode_number(self.gas_limit)).append(&self.to);

        match self.value.clone() {
            Some(value) => {
                s.append(&value.bytes.as_slice());
            }
            None => {
                let value = BigInt { bytes: vec![0] };
                s.append(&value.bytes.as_slice());
            }
        }

        s.append(&self.input);
        if self.r#type == 1 || self.r#type == 2 {
            s.append_list(&self.access_list);
        }
        s.append(&trim_left(self.v.as_slice()))
            .append(&trim_left(self.r.as_slice()))
            .append(&trim_left(self.s.as_slice()));

        s.finalize_unbounded_list();
    }
}

impl Encodable for TransactionReceipt {
    fn rlp_append(&self, s: &mut RlpStream) {
        s.begin_unbounded_list()
            .append(&self.state_root)
            .append(&encode_number(self.cumulative_gas_used))
            .append(&self.logs_bloom)
            .append_list(&self.logs)
            .finalize_unbounded_list();
    }
}

impl Encodable for Log {
    fn rlp_append(&self, s: &mut RlpStream) {
        let topics = self
            .topics
            .iter()
            .map(|topic| topic.as_slice())
            .collect::<Vec<&[u8]>>();
        s.begin_unbounded_list()
            .append(&self.address)
            .append_list::<&[u8], &[u8]>(topics.as_slice())
            .append(&self.data)
            .finalize_unbounded_list();
    }
}

impl Encodable for AccessTuple {
    fn rlp_append(&self, s: &mut RlpStream) {
        let storage_keys = self
            .storage_keys
            .iter()
            .map(|key| key.as_slice())
            .collect::<Vec<&[u8]>>();
        s.begin_unbounded_list()
            .append(&self.address)
            .append_list::<&[u8], &[u8]>(storage_keys.as_slice())
            .finalize_unbounded_list();
    }
}

fn trim_left(bytes: &[u8]) -> &[u8] {
    let mut idx = 0;
    for byte in bytes {
        if *byte != 0 {
            break;
        }
        idx += 1;
    }

    &bytes[idx..]
}
