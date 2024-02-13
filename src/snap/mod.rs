use bytes::BufMut;
use std::io::Write;

pub fn snap_encode(decoded_data: &[u8]) -> anyhow::Result<Vec<u8>> {
    let encoded_data = Vec::new();
    let writer = encoded_data.writer();
    let mut encoder = snap::write::FrameEncoder::new(writer);

    encoder.write_all(decoded_data)?;

    Ok(encoder.into_inner()?.into_inner())
}
