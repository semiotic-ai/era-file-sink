use decoder::receipts::error::ReceiptError;
use reth_primitives::{Address, Bytes, Log, H256};

impl TryFrom<&crate::pb::acme::verifiable_block::v1::Log> for Log {
    type Error = ReceiptError;

    fn try_from(log: &crate::pb::acme::verifiable_block::v1::Log) -> Result<Self, Self::Error> {
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
