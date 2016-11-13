use std::fs::File;
use std::mem;
use std::path::PathBuf;
use std::path::Path;
use std::io::prelude::*;
use std::io::SeekFrom;
use crc::{crc32, Hasher32};

pub struct Segment {
    path: PathBuf,
    pub offset: usize,
    buffer_size: usize,
    file: Option<File>,
    write_buffer: Option<Vec<u8>>,
    buffer_offset: usize
}

impl Segment {
    pub fn new(path: &Path, offset: usize, buffer_size: usize) -> Segment {
        let path_buf = path.to_path_buf();
        Segment {
            path: path_buf,
            offset: offset,
            buffer_size: buffer_size,
            file: None,
            write_buffer: None,
            buffer_offset: 0
        }
    }

    pub fn append(&mut self, payload: &[u8]) {
        if self.file.is_none() {
            let file = File::create(&self.path).unwrap();
            self.file = Some(file);
            self.write_buffer = Some(vec![0; self.buffer_size]);
            self.buffer_offset = 0;
        }

        let file = self.file.as_mut().unwrap();
        let mut buffer = self.write_buffer.as_mut().unwrap();
        self.buffer_offset = write_payload(file, &mut buffer, self.buffer_offset, payload);
    }

    pub fn close(&mut self) {
        self.file = None;
        self.write_buffer = None;
    }
}

pub const CRC_OFFSET: usize = 0;     // 0-3
pub const LEN_OFFSET: usize = 4;     // 4-7
pub const TYPE_OFFSET: usize = 8;    // 8
pub const PAYLOAD_OFFSET: usize = 9; // 9 - ??

pub const NUM_HEADER_BYTES: usize = 9; // crc(4) + length(4) + type(1)

#[derive(Copy, Clone)]
pub enum ChunkType {
    Null = 0,
    Full = 1,
    Start = 2,
    Middle = 3,
    End = 4
}

impl ChunkType {
    fn from_byte(x: u8) -> ChunkType {
        match x {
            x if x == ChunkType::Null as u8 => ChunkType::Null,
            x if x == ChunkType::Full as u8 => ChunkType::Full,
            x if x == ChunkType::Start as u8 => ChunkType::Start,
            x if x == ChunkType::Middle as u8 => ChunkType::Middle,
            x if x == ChunkType::End as u8 => ChunkType::End,
            _ => panic!("Unknown chunk type"),
        }
    }
}

fn write_payload(file: &mut File, buffer: &mut Vec<u8>, initial_buffer_offset: usize, payload: &[u8]) -> usize {
    if payload.len() == 0 {
        panic!("Can't handle empty messages");
    }

    let empty_vector = vec![];

    let mut remaining_payload = payload;
    let mut buffer_offset = initial_buffer_offset;
    let mut num_pre_chunks = 0;

    let has_buffer_space = initial_buffer_offset + NUM_HEADER_BYTES < buffer.len();
    let is_buffer_written = initial_buffer_offset > 0;
    if has_buffer_space && is_buffer_written {
        let open_buffer_size = buffer.len() - initial_buffer_offset - NUM_HEADER_BYTES;
        // Last written chunk has room to append additional payload
        file.seek(SeekFrom::Current(-(buffer.len() as i64))).expect("Failed to reset write location");
        num_pre_chunks = 1;

        if remaining_payload.len() <= open_buffer_size {
            // Full write
            buffer_offset = write_chunk(file, buffer, remaining_payload, 0, 1, buffer_offset);
            remaining_payload = &empty_vector;
        } else {
            // Partial write
            let chunk = &remaining_payload[0..open_buffer_size];
            buffer_offset = write_chunk(file, buffer, chunk, 0, 2, buffer_offset); // Num chunks >= 2
            remaining_payload = &remaining_payload[open_buffer_size..remaining_payload.len()];
        }

    }

    if remaining_payload.len() > 0 {
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
        let num_payload_bytes_per_chunk = buffer.capacity() - NUM_HEADER_BYTES;
        let num_chunks = num_pre_chunks + (remaining_payload.len() + num_payload_bytes_per_chunk - 1) / num_payload_bytes_per_chunk;

        let mut chunks_iter = remaining_payload.chunks(num_payload_bytes_per_chunk).enumerate();
        while let Some((i, next_chunk)) = chunks_iter.next() {
            buffer_offset = 0;
            clear_buffer(buffer);

            buffer_offset = write_chunk(file, buffer, next_chunk, i + num_pre_chunks, num_chunks, buffer_offset);
        }
    }

    file.flush().expect("Failed to flush");
    file.sync_all().expect("Failed to sync");

    buffer_offset
}

fn clear_buffer(buffer: &mut Vec<u8>) {
    for i in 0..buffer.len() {
        buffer[i] = 0;
    }
}

fn write_chunk(file: &mut File, buffer: &mut Vec<u8>, payload: &[u8], chunk_index: usize, num_chunks: usize, buffer_offset: usize) -> usize {
    let num_chunk_bytes: usize = payload.len() + NUM_HEADER_BYTES;
    let adjusted_offset = buffer_offset + num_chunk_bytes;

    write_u32(buffer, 0, buffer_offset + CRC_OFFSET);
    write_u32(buffer, payload.len() as u32, buffer_offset + LEN_OFFSET);

    buffer[buffer_offset + TYPE_OFFSET] = if chunk_index == 0 && num_chunks == 1 {
        ChunkType::Full as u8
    } else if chunk_index == 0 {
        ChunkType::Start as u8
    } else if chunk_index + 1 == num_chunks {
        ChunkType::End as u8
    } else {
        ChunkType::Middle as u8
    };

    let mut payload_iter = payload.iter().enumerate();
    while let Some((i, x)) = payload_iter.next() {
        buffer[buffer_offset + PAYLOAD_OFFSET + i] = *x;
    }

    let crc_start = buffer_offset + LEN_OFFSET; // Skip crc
    let record_crc = calculate_crc(&buffer[crc_start..adjusted_offset]);

    write_u32(buffer, record_crc, buffer_offset + CRC_OFFSET);

    file.write_all(&buffer).expect("Failed to write");

    adjusted_offset
}

fn read_payload(file: &mut File, buffer: &mut Vec<u8>) -> Vec<u8> {
    let mut payload = Vec::new();

    loop {
        let read_result = read_chunk(&mut payload, file, buffer).unwrap();

        match read_result {
            ChunkType::Full | ChunkType::End | ChunkType::Null => break,
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

pub fn read_u32(buffer: &[u8], index: usize) -> Result<u32, &'static str> {
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

fn calculate_crc(payload: &[u8]) -> u32 {
    let mut digest = crc32::Digest::new(crc32::IEEE);
    digest.write(payload);
    digest.sum32()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;
    use std::fs;
    use std::fs::File;
    use std::io::Read;

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
        let items: Vec<u8> = vec![1, 0, 0, 0];
        let result = read_u32(&items, 0).unwrap();
        assert_eq!(result, 1);
    }

    #[test]
    fn test_read_u32_with_overflow () {
        let items: Vec<u8> = vec![1, 0, 0, 0];
        let result = read_u32(&items, 2);
        assert!(result.is_err());
    }

    #[test]
    fn test_write_read_u32_cycle () {
        let mut items: Vec<u8> = vec![0, 0, 0, 0, 0, 0];

        write_u32(&mut items, 45, 2).expect("Should work");

        let result = read_u32(&items, 2).unwrap();
        assert_eq!(result, 45);
    }

    fn write_messages_to_segment(path: &Path, buffer_size: usize, messages: &[&[u8]]) -> Vec<u8> {
        fs::remove_file(&path);
        let mut seg = Segment::new(path, 0, buffer_size);

        for message in messages {
            seg.append(&message);
        }

        seg.close();

        let file = File::open(&path).unwrap();
        let segment_bytes = file.bytes().map(|b| b.unwrap()).collect();
        segment_bytes
    }

    fn validate_full_message(segment_bytes: &[u8], message: &[u8], offset: usize) {
        assert_eq!(read_u32(segment_bytes, offset + LEN_OFFSET).unwrap(), message.len() as u32);
        assert_eq!(segment_bytes[offset + TYPE_OFFSET], ChunkType::Full as u8);
        assert_eq!(message, &segment_bytes[(offset + PAYLOAD_OFFSET)..(offset + PAYLOAD_OFFSET + message.len())]);
    }

    #[test]
    fn test_single_append_full_initial() {
        let path = Path::new("./test_data/segments/test_single_append_full_initial");
        let message = vec![0, 1, 2, 3, 4];
        let segment_bytes = write_messages_to_segment(&path, 16, &[&message]);
        assert_eq!(segment_bytes.len(), 16);

        validate_full_message(&segment_bytes, &message, 0);
    }

    #[test]
    fn test_single_append_split() {
        let path = Path::new("./test_data/segments/test_append_split");
        let message = vec![0, 1, 2, 3, 4, 5, 6, 7];

        let segment_bytes = write_messages_to_segment(&path, 16, &[&message]);
        assert_eq!(segment_bytes.len(), 32);

        assert_eq!(read_u32(&segment_bytes, LEN_OFFSET).unwrap(), 7);
        assert_eq!(segment_bytes[TYPE_OFFSET], ChunkType::Start as u8);
        assert_eq!(message[0..7], segment_bytes[PAYLOAD_OFFSET..(PAYLOAD_OFFSET + 7)]);

        assert_eq!(read_u32(&segment_bytes, 16 + LEN_OFFSET).unwrap(), 1);
        assert_eq!(segment_bytes[16 + TYPE_OFFSET], ChunkType::End as u8);
        assert_eq!(message[7], segment_bytes[16 + PAYLOAD_OFFSET]);
    }

    #[test]
    fn test_multi_append_full_initial() {
        let path = Path::new("./test_data/segments/test_multi_append_full_initial");
        let initial_message = vec![42];
        let seconday_message = vec![0, 1, 2, 3, 4];
        let segment_bytes = write_messages_to_segment(&path, 32, &[&initial_message, &seconday_message]);
        assert_eq!(segment_bytes.len(), 32);

        // Initial message
        validate_full_message(&segment_bytes, &initial_message, 0);

        // Seconday message
        let secondary_message_offset = initial_message.len() + NUM_HEADER_BYTES;
        assert_eq!(read_u32(&segment_bytes, secondary_message_offset + LEN_OFFSET).unwrap(), seconday_message.len() as u32);
        assert_eq!(segment_bytes[secondary_message_offset + TYPE_OFFSET], ChunkType::Full as u8);
        let actual_secondary_message = &segment_bytes[secondary_message_offset + PAYLOAD_OFFSET..(secondary_message_offset + PAYLOAD_OFFSET + seconday_message.len())];
        assert_eq!(&seconday_message[0..seconday_message.len()], actual_secondary_message);
    }

    #[test]
    fn test_multi_append_partial_initial() {
        let path = Path::new("./test_data/segments/test_multi_append_partial_initial");
        let initial_message = vec![42]; // 10 bytes
        let seconday_message = vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13]; // 23 bytes
        let segment_bytes = write_messages_to_segment(&path, 32, &[&initial_message, &seconday_message]); // 10 + 23 > 32
        assert_eq!(segment_bytes.len(), 64);

        // Inital message
        validate_full_message(&segment_bytes, &initial_message, 0);

        // Secondary message head
        let head_offset = initial_message.len() + NUM_HEADER_BYTES;
        assert_eq!(read_u32(&segment_bytes, head_offset + LEN_OFFSET).unwrap(), 13);
        assert_eq!(segment_bytes[head_offset + TYPE_OFFSET], ChunkType::Start as u8);
        let actual_secondary_message = &segment_bytes[(head_offset + PAYLOAD_OFFSET)..(head_offset + PAYLOAD_OFFSET + 13)];
        assert_eq!(&seconday_message[0..13], actual_secondary_message);

        // Seconday message tail
        let tail_offset = 32;
        assert_eq!(read_u32(&segment_bytes, tail_offset + LEN_OFFSET).unwrap(), 1);
        assert_eq!(segment_bytes[tail_offset + TYPE_OFFSET], ChunkType::End as u8);
        let actual_secondary_message = &segment_bytes[tail_offset + PAYLOAD_OFFSET..(tail_offset + PAYLOAD_OFFSET + 1)];
        assert_eq!(&seconday_message[13..14], actual_secondary_message);
    }

    #[test]
    fn test_multi_append_none_initial() {
        let path = Path::new("./test_data/segments/test_multi_append_none_initial");
        let initial_message = vec![42];
        let seconday_message = vec![0, 1, 2, 3, 4];
        let segment_bytes = write_messages_to_segment(&path, 16, &[&initial_message, &seconday_message]);
        assert_eq!(segment_bytes.len(), 32);

        // Initial message
        validate_full_message(&segment_bytes, &initial_message, 0);

        // Seconday message
        let secondary_message_offset = 16;
        assert_eq!(read_u32(&segment_bytes, secondary_message_offset + LEN_OFFSET).unwrap(), seconday_message.len() as u32);
        assert_eq!(segment_bytes[secondary_message_offset + TYPE_OFFSET], ChunkType::Full as u8);
        let actual_secondary_message = &segment_bytes[secondary_message_offset + PAYLOAD_OFFSET..(secondary_message_offset + PAYLOAD_OFFSET + seconday_message.len())];
        assert_eq!(&seconday_message[0..seconday_message.len()], actual_secondary_message);
    }
}
