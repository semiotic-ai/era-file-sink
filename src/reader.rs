use std::io::Read;
use std::process::exit;

mod e2store;
mod pb;

use e2store::{E2Store, E2StoreType};
use e2store::snap_utils::snap_decode;
use crate::pb::acme::verifiable_block::v1::BlockHeader;

struct E2StoreReader {
    file: std::fs::File,
}

impl E2StoreReader {
    pub fn new(file: std::fs::File) -> Self {
        Self {
            file,
        }
    }

    pub fn read(&mut self) -> Result<E2Store, anyhow::Error> {
        let mut buf = [0; 8];
        self.file.read_exact(&mut buf)?;
        let type_ = u16::from_le_bytes(buf[0..2].try_into().unwrap()).try_into()?;
        let length = u32::from_le_bytes(buf[2..6].try_into().unwrap());
        let reserved = u16::from_le_bytes(buf[6..8].try_into().unwrap());
        let mut data = vec![0; length as usize];
        self.file.read_exact(&mut data)?;

        Ok(E2Store {
            type_,
            length,
            reserved,
            data
        })
    }
}

pub fn decompress_store(store: &E2Store) -> Result<Vec<u8>, anyhow::Error> {
    match store.type_ {
        E2StoreType::CompressedHeader | E2StoreType::CompressedBody | E2StoreType::CompressedReceipts => {
            snap_decode(store.data.as_slice())
        }
        _ => {
            Ok(store.data.clone())
        }
    }
}

fn main() {
    let mut args = std::env::args();
    if args.len() < 3 {
        println!("usage: reader <file1> <file2>");
        exit(1);
    }

    args.next();

    let file1 = args.next().unwrap();
    let file2 = args.next().unwrap();

    read_files(&file1, &file2).unwrap();
}

pub fn read_files(file1: &str, file2: &str) -> Result<(), anyhow::Error> {
    let file1 = std::fs::File::open(file1)?;
    let file2 = std::fs::File::open(file2)?;

    let mut reader1 = E2StoreReader::new(file1);
    let mut reader2 = E2StoreReader::new(file2);
    let mut count = 0;

    loop {
        let e2store1 = reader1.read()?;
        let e2store2 = reader2.read()?;

        let data1 = decompress_store(&e2store1)?;
        let data2 = decompress_store(&e2store2)?;

        match e2store1.type_ {
            E2StoreType::CompressedBody => {
                if data1 != data2 {
                    println!("Difference in store type {:?}", e2store1.type_);

                    println!("Len1: {}", data1.len());
                    println!("Len2: {}", data2.len());

                    let diff = data1.len() - data2.len();
                    if diff % 20 != 0 {
                        println!("Difference in len {}", diff);
                        println!("1_ {:?}", data1);
                        println!("2_ {:?}", data2);
                    }

                    // let data1 = rlp_decode_body(data1);
                    // let data2 = rlp_decode_body(data2);
                    //
                    //
                    // for (i, (d1, d2)) in data1.iter().zip(data2.iter()).enumerate() {
                    //     if d1 != d2 {
                    //         println!("Difference in idx {}", i);
                    //         for (j, (dd1, dd2)) in d1.iter().zip(d2.iter()).enumerate() {
                    //             if dd1 != dd2 {
                    //                 println!("Difference in part {}", j);
                    //                 println!("1_ {:?}", dd1);
                    //                 println!("2_ {:?}", dd2);
                    //             }
                    //         }
                    //     }
                    // }
                }
            }
            _ => {
                if data1 != data2 {
                    println!("Difference in store type {:?}", e2store1.type_);
                    // println!("1_ {:?}", data1);
                    // println!("2_ {:?}", data2);
                }
            }
        }

    }

    Ok(())

}

fn rlp_decode(data: Vec<u8>) -> Vec<Vec<u8>>{
    // Simple rlp decoder, just return array of elements of list
    rlp::decode_list(data.as_slice())
}

fn rlp_decode_body(data: Vec<u8>) -> Vec<Vec<Vec<u8>>> {
    let first_byte = data[0];
    let full_len_len = first_byte - 247;
    // let len = u64::from_le_bytes(data[1..1 + len_len as usize].try_into().unwrap());
    let start_idx = 1 + full_len_len as usize;
    let first_byte = data[start_idx];
    let tx_list_len_len = first_byte - 247;

    let mut len_buf = [0; 8];
    len_buf[..tx_list_len_len as usize].copy_from_slice(&data[start_idx + 1..start_idx + 1 + tx_list_len_len as usize]);

    let tx_list_len = u64::from_le_bytes(len_buf);
    let mut res = Vec::new();

    let mut idx = start_idx + 1 + tx_list_len_len as usize;
    let end_idx = start_idx + 1 + tx_list_len_len as usize + tx_list_len as usize;

    loop {
        if idx >= end_idx {
            break;
        }

        let first_byte = data[idx];
        let tx_len_len = first_byte - 247;

        let mut len_buf = [0; 8];
        len_buf[..tx_len_len as usize].copy_from_slice(&data[idx + 1..idx + 1 + tx_len_len as usize]);
        let tx_len = u64::from_le_bytes(len_buf);
        println!("tx_len: {}", tx_len);

        let tx_end = idx + 1 + tx_len_len as usize + tx_len as usize;
        if tx_end > end_idx {
            break;
        }

        let tx_data = data[idx..tx_end].to_vec();

        let tx: Vec<Vec<u8>> = rlp::decode_list(tx_data.as_slice());

        res.push(tx);
        idx = tx_end;
    }

    res

}