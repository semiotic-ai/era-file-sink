use embed_file::embed_string;

pub const EPOCH_SIZE: u64 = 8192;

pub fn read_values() -> Vec<String> {
   embed_string!("assets/acc_values.txt").lines().map(|line| line.to_string()).collect()
}

pub fn get_epoch(block_number: u64) -> u64 {
    block_number / EPOCH_SIZE
}

pub fn get_value_for_block(data: &Vec<String>, block_number: u64) -> Option<&String> {
    data.get(get_epoch(block_number) as usize)
}
