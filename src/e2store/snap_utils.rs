use prost::bytes::BufMut;
use std::io::{BufReader, Read, Write};

pub fn snap_encode(decoded_data: &[u8]) -> anyhow::Result<Vec<u8>> {
    let mut encoded_data = Vec::new();
    let writer = encoded_data.writer();
    let mut encoder = snap::write::FrameEncoder::new(writer);

    encoder.write_all(decoded_data)?;

    Ok(encoder.into_inner()?.into_inner())
}

pub fn snap_decode(encoded_data: &[u8]) -> anyhow::Result<Vec<u8>> {
    let mut decoded_data = Vec::new();
    let reader = BufReader::new(encoded_data);
    let mut decoder = snap::read::FrameDecoder::new(reader);

    decoder.read_to_end(&mut decoded_data)?;

    Ok(decoded_data)
}
