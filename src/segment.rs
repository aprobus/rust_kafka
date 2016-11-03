use std::fs::File;
use std::mem;
use std::path::PathBuf;
use std::path::Path;
use std::io::prelude::*;
use crc::{crc32, Hasher32};

pub struct Segment {
    path: PathBuf,
    pub offset: usize,
    file: Option<File>
}

impl Segment {
    pub fn new(path: &Path, offset: usize) -> Segment {
        let path_buf = path.to_path_buf();
        Segment { path: path_buf, offset: offset, file: None }
    }

    pub fn append(&mut self, buffer: &mut Vec<u8>, payload: &[u8]) {
        if self.file.is_none() {
            let file = File::create(&self.path).unwrap();
            self.file = Some(file);
        }

        let file = self.file.as_mut().unwrap();
        write_payload(file, buffer, payload);
    }

    pub fn close(&mut self) {
        self.file = None;
    }
}

const CRC_OFFSET: usize = 0;     // 0-3
const LEN_OFFSET: usize = 4;     // 4-7
const TYPE_OFFSET: usize = 8;    // 8
const PAYLOAD_OFFSET: usize = 9; // 9 - ??

const NUM_HEADER_BYTES: usize = 9; // crc(4) + length(4) + type(1)

#[derive(Copy, Clone)]
enum ChunkType {
    Full = 0,
    Start = 1,
    Middle = 2,
    End = 3
}

impl ChunkType {
    fn from_byte(x: u8) -> ChunkType {
        match x {
            x if x == ChunkType::Full as u8 => ChunkType::Full,
            x if x == ChunkType::Start as u8 => ChunkType::Start,
            x if x == ChunkType::Middle as u8 => ChunkType::Middle,
            x if x == ChunkType::End as u8 => ChunkType::End,
            _ => panic!("Unknown chunk type"),
        }
    }
}

fn write_payload(file: &mut File, buffer: &mut Vec<u8>, payload: &[u8]) {
    let num_payload_bytes_per_chunk = buffer.capacity() - NUM_HEADER_BYTES;

    // 1. Exact
    // payload_size = 10
    // chunk_size = 10
    //
    // (10 + 10 - 1) / 10
    // 19 / 10
    // 1 chunk
    //
    // 2. Partial
    // payload_size = 12
    // chunk_size = 10
    //
    // (12 + 10 - 1) / 10
    // 21 / 10
    // 2 chunks
    let num_chunks = (payload.len() + num_payload_bytes_per_chunk - 1) / num_payload_bytes_per_chunk;

    let mut chunks_iter = payload.chunks(num_payload_bytes_per_chunk).enumerate();
    while let Some((i, next_chunk)) = chunks_iter.next() {
        write_chunk(file, buffer, next_chunk, i, num_chunks);
    }

    file.flush().expect("Failed to flush");
    file.sync_all().expect("Failed to sync");
}

fn write_chunk(file: &mut File, buffer: &mut Vec<u8>, payload: &[u8], chunk_index: usize, num_chunks: usize) {
    write_u32(buffer, 0, CRC_OFFSET);
    write_u32(buffer, payload.len() as u32, LEN_OFFSET);

    if chunk_index == 0 && num_chunks == 1 {
        buffer[TYPE_OFFSET] = ChunkType::Full as u8;
    } else if chunk_index == 0 {
        buffer[TYPE_OFFSET] = ChunkType::Start as u8;
    } else if chunk_index + 1 == num_chunks {
        buffer[TYPE_OFFSET] = ChunkType::End as u8;
    } else {
        buffer[TYPE_OFFSET] = ChunkType::Middle as u8;
    }

    let mut payload_iter = payload.iter().enumerate();
    while let Some((i, x)) = payload_iter.next() {
        buffer[PAYLOAD_OFFSET + i] = *x;
    }

    for i in (PAYLOAD_OFFSET + payload.len())..(buffer.capacity()) {
        buffer[i] = 0;
    }

    let record_crc = calculate_crc(buffer);
    write_u32(buffer, record_crc, CRC_OFFSET);

    file.write_all(&buffer).expect("Failed to write");
}

fn read_payload(file: &mut File, buffer: &mut Vec<u8>) -> Vec<u8> {
    let mut payload = Vec::new();

    loop {
        let read_result = read_chunk(&mut payload, file, buffer).unwrap();

        match read_result {
            ChunkType::Full | ChunkType::End => break,
            ChunkType::Start | ChunkType::Middle => continue,
        };
    }

    payload
}

fn read_chunk(payload: &mut Vec<u8>, file: &mut File, buffer: &mut Vec<u8>) -> Result<ChunkType, &'static str> {
    if let Result::Err(_) = file.read_exact(buffer) {
        return Result::Err("Unable to read from file")
    }

    let expected_crc: u32 = read_u32(&buffer, CRC_OFFSET).unwrap();
    write_u32(buffer, 0, CRC_OFFSET);
    let actual_crc = calculate_crc(buffer);

    if expected_crc != actual_crc {
        return Result::Err("CRC did not much expected value")
    }

    let chunk_len = read_u32(&buffer, LEN_OFFSET).unwrap() as usize;
    let chunk_type = ChunkType::from_byte(buffer[TYPE_OFFSET]);

    payload.reserve(chunk_len);
    for i in 0..chunk_len {
        payload.push(buffer[PAYLOAD_OFFSET + i]);
    }

    Result::Ok(chunk_type)
}

pub fn read_u32(buffer: &Vec<u8>, index: usize) -> Result<u32, &'static str> {
    let size = mem::size_of::<u32>();

    if index + size > buffer.len() {
        return Result::Err("Not enough readable bytes")
    }

    let mut result: u32 = 0;
    for i in index..(index + size) {
        let next_byte: u32 = buffer[i] as u32;
        result = (result >> 8) | (next_byte << 24);
    }

    Result::Ok(result)
}

pub fn write_u32(buffer: &mut Vec<u8>, x: u32, index: usize) -> Result<(), &'static str> {
    let size = mem::size_of::<u32>();

    if index + size > buffer.len() {
        return Result::Err("Not enough space to write")
    }

    let mut x_remain = x;
    for i in index..(index + size) {
        let byte = x_remain as u8;
        buffer[i] = byte;
        x_remain = x_remain >> 8;
    }

    Result::Ok(())
}

fn calculate_crc(payload: &Vec<u8>) -> u32 {
    let mut digest = crc32::Digest::new(crc32::IEEE);
    digest.write(&payload);
    digest.sum32()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_write_u32 () {
        let mut items: Vec<u8> = vec![0; 4];
        write_u32(&mut items, 1, 0);
        assert_eq!(items, vec!(1, 0, 0, 0));
    }

    #[test]
    fn test_write_u32_with_overflow () {
        let mut items: Vec<u8> = vec![0; 4];
        let result = write_u32(&mut items, 1, 2);

        assert!(result.is_err());
    }

    #[test]
    fn test_read_u32 () {
        let mut items: Vec<u8> = vec![1, 0, 0, 0];
        let result = read_u32(&mut items, 0).unwrap();
        assert_eq!(result, 1);
    }

    #[test]
    fn test_read_u32_with_overflow () {
        let mut items: Vec<u8> = vec![1, 0, 0, 0];
        let result = read_u32(&mut items, 2);
        assert!(result.is_err());
    }

    #[test]
    fn test_write_read_u32_cycle () {
        let mut items: Vec<u8> = vec![0, 0, 0, 0, 0, 0];

        write_u32(&mut items, 45, 2).expect("Should work");

        let result = read_u32(&mut items, 2).unwrap();
        assert_eq!(result, 45);
    }
}
