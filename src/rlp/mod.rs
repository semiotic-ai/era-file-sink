use crate::pb::acme::verifiable_block::v1::{BlockHeader, Log, TransactionReceipt};
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
