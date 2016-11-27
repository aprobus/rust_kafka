use std::fs::File;
use std::mem;
use std::path::PathBuf;
use std::path::Path;
use std::io::prelude::*;
use std::io::SeekFrom;
use crc::{crc32, Hasher32};
use std::io;

pub struct SegmentIterator {
    file: File,
    buffer: Vec<u8>,
    offset: usize
}

impl SegmentIterator {
    fn new(path: &Path, buffer_size: usize) -> SegmentIterator {
        let segment_file = File::open(path).unwrap();
        let buffer = vec![0; buffer_size];

        SegmentIterator { file: segment_file, buffer: buffer, offset: buffer_size }
    }

    fn is_stale(&self) -> bool {
        ChunkType::from_byte(self.buffer[self.offset + TYPE_OFFSET]) == ChunkType::Null
    }

    fn is_buffer_exhausted(&self) -> bool {
        self.offset + NUM_HEADER_BYTES >= self.buffer.len()
    }

    fn load_buffer(&mut self) -> io::Result<()> {
        let load_result = self.file.read_exact(&mut self.buffer);

        if load_result.is_ok() {
            self.offset = 0;
        }

        load_result
    }

    fn reload_buffer(&mut self) {
        let buffer_size = self.buffer.len() as i64;
        self.file.seek(SeekFrom::Current(-buffer_size)).expect("Failed to reset read location");
        self.file.read_exact(&mut self.buffer).expect("Failed to reread buffer");
    }

    fn ensure_buffer_loaded(&mut self) -> io::Result<()> {
        if self.is_buffer_exhausted() {
            self.load_buffer()
        } else {
            Ok(())
        }
    }

}

impl Iterator for SegmentIterator {
    type Item = Vec<u8>;

    fn next(&mut self) -> Option<Vec<u8>> {
        let mut payload = Vec::new();

        if !self.is_buffer_exhausted() && self.is_stale() {
            self.reload_buffer();
            if self.is_stale() {
                return None;
            }
        }

        loop {
            if self.ensure_buffer_loaded().is_err() {
                return None;
            }

            let (chunk_type, offset) = read_message(&self.buffer, self.offset, &mut payload);
            self.offset = offset;

            match chunk_type {
                ChunkType::Full | ChunkType::End => {
                    break;
                },
                ChunkType::Null => {
                    return None;
                },
                ChunkType::Middle | ChunkType::Start => {
                    continue;
                }
            }
        }

        Some(payload)
    }
}

fn read_message(buffer: &[u8], offset: usize, payload: &mut Vec<u8>) -> (ChunkType, usize) {
    let chunk_type = ChunkType::from_byte(buffer[offset + TYPE_OFFSET]);
    if chunk_type == ChunkType::Null {
        return (chunk_type, offset);
    }

    let payload_size = u32::read_bytes(buffer, offset + LEN_OFFSET).unwrap() as usize;
    let payload_start = offset + PAYLOAD_OFFSET;
    let payload_end = offset + PAYLOAD_OFFSET + payload_size;

    let expected_crc = calculate_crc(&buffer[(offset + LEN_OFFSET)..payload_end]);
    let actual_crc = u32::read_bytes(buffer, offset + CRC_OFFSET).unwrap();
    if expected_crc != actual_crc {
        panic!("Invalid crc");
    }

    payload.reserve(payload_size);
    for i in &buffer[payload_start..payload_end] {
        payload.push(*i);
    }

    (chunk_type, payload_end)
}

pub const FOOTER_MAGIC_OFFSET: usize = 0;        // 0
pub const FOOTER_INDEX_OFFSET: usize = 1;        // 1-8
pub const FOOTER_BUFFER_SIZE_OFFSET: usize = 9;  // 9-16
pub const FOOTER_START_INDEX_OFFSET: usize = 17; // 17-24
pub const FOOTER_NEXT_INDEX_OFFSET: usize = 25;  // 25-32

pub const FOOTER_MAGIC_BYTE: u8 = 42;
pub const FOOTER_BYTE_COUNT: usize = 33;

#[derive(Clone, Debug, PartialEq)]
pub struct SegmentInfo {
    path: PathBuf,
    pub index: usize,
    buffer_size: usize,
    start_offset: usize,
    pub next_offset: usize
}

impl SegmentInfo {
    pub fn new(path: &Path, index: usize, start_offset: usize, buffer_size: usize) -> SegmentInfo {
        let path_buf = path.to_path_buf();

        SegmentInfo {
            path: path_buf,
            index: index,
            buffer_size: buffer_size,
            start_offset: start_offset,
            next_offset: start_offset
        }
    }

    pub fn from_file(path: &Path) -> SegmentInfo {
        let mut file = File::open(path).unwrap();
        file.seek(SeekFrom::End(-(FOOTER_BYTE_COUNT as i64))).expect("Failed to seek to footer");

        let mut footer_bytes = vec![0; FOOTER_BYTE_COUNT];
        file.read_exact(&mut footer_bytes).expect("Failed to read footer");

        if u8::read_bytes(&footer_bytes, FOOTER_MAGIC_OFFSET).unwrap() != FOOTER_MAGIC_BYTE {
            panic!("Magic byte is missing!");
        }

        let path_buf = path.to_path_buf();
        SegmentInfo {
            path: path.to_path_buf(),
            index: u64::read_bytes(&footer_bytes, FOOTER_INDEX_OFFSET).unwrap() as usize,
            buffer_size: u64::read_bytes(&footer_bytes, FOOTER_BUFFER_SIZE_OFFSET).unwrap() as usize,
            start_offset: u64::read_bytes(&footer_bytes, FOOTER_START_INDEX_OFFSET).unwrap() as usize,
            next_offset: u64::read_bytes(&footer_bytes, FOOTER_NEXT_INDEX_OFFSET).unwrap() as usize
        }
    }

    pub fn iter(&self) -> SegmentIterator {
        SegmentIterator::new(&self.path, self.buffer_size)
    }
}

pub struct SegmentWriter {
    segment_info: SegmentInfo,
    file: File,
    write_buffer: Vec<u8>,
    buffer_offset: usize,
    num_payload_bytes_per_chunk: usize
}

impl SegmentWriter {
    pub fn new(segment_info: SegmentInfo) -> SegmentWriter {
        let file = File::create(&segment_info.path).unwrap();
        let write_buffer = vec![0; segment_info.buffer_size];

        let num_payload_bytes_per_chunk = segment_info.buffer_size - NUM_HEADER_BYTES;

        SegmentWriter {
            file: file,
            buffer_offset: 0,
            write_buffer: write_buffer,
            segment_info: segment_info,
            num_payload_bytes_per_chunk: num_payload_bytes_per_chunk
        }
    }

    pub fn append(&mut self, payload: &[u8]) {
        self.write_payload(payload);
        self.segment_info.next_offset += 1;
    }

    fn buffer_payload_capacity(&self) -> usize {
        let num_used_bytes = self.buffer_offset + NUM_HEADER_BYTES;

        if num_used_bytes >= self.write_buffer.len() {
            0
        } else {
            self.write_buffer.len() - num_used_bytes
        }
    }

    fn is_buffer_full(&self) -> bool {
        self.buffer_payload_capacity() == 0
    }

    fn is_buffer_hungry(&self) -> bool {
        self.buffer_payload_capacity() > 0
    }

    fn is_buffer_clean(&self) -> bool {
        self.buffer_offset == 0
    }

    fn is_buffer_dirty(&self) -> bool {
        !self.is_buffer_clean()
    }

    fn seek_buffer_start(&mut self) -> io::Result<()> {
        if self.is_buffer_dirty() {
            self.file.seek(SeekFrom::Current(-(self.write_buffer.len() as i64))).and_then(|_| Result::Ok(()))
        } else {
            Result::Ok(())
        }
    }

    fn num_chunks(&self, payload: &[u8]) -> usize {
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
        (payload.len() + self.num_payload_bytes_per_chunk - 1) / self.num_payload_bytes_per_chunk
    }

    fn clear_buffer(&mut self) {
        for i in 0..self.write_buffer.len() {
            self.write_buffer[i] = 0;
        }

        self.buffer_offset = 0;
    }

    fn flush(&mut self) {
        self.file.flush().expect("Failed to flush");
        self.file.sync_all().expect("Failed to sync");
    }

    fn write(&mut self) {
        self.file.write_all(&self.write_buffer).expect("Failed to write");
    }

    fn write_payload(&mut self, payload: &[u8]) {
        if payload.len() == 0 {
            panic!("Can't handle empty messages");
        }

        let empty_vector = vec![];

        let mut remaining_payload = payload;
        let mut num_pre_chunks = 0;

        if self.is_buffer_hungry() && self.is_buffer_dirty() {
            let open_buffer_size = self.buffer_payload_capacity();
            // Last written chunk has room to append additional payload
            self.seek_buffer_start();
            num_pre_chunks = 1;

            if remaining_payload.len() <= open_buffer_size {
                // Full write
                self.write_chunk(remaining_payload, 0, 1);
                remaining_payload = &empty_vector;
            } else {
                // Partial write
                let chunk = &remaining_payload[0..open_buffer_size];
                self.write_chunk(chunk, 0, 2); // Num chunks >= 2
                remaining_payload = &remaining_payload[open_buffer_size..remaining_payload.len()];
            }
        }

        if remaining_payload.len() > 0 {
            let num_chunks = num_pre_chunks + self.num_chunks(remaining_payload);

            let mut chunks_iter = remaining_payload.chunks(self.num_payload_bytes_per_chunk).enumerate();
            while let Some((i, next_chunk)) = chunks_iter.next() {
                self.clear_buffer();

                self.write_chunk(next_chunk, i + num_pre_chunks, num_chunks);
            }
        }

        self.flush();
    }

    fn write_chunk(&mut self, payload: &[u8], chunk_index: usize, num_chunks: usize) {
        let num_chunk_bytes: usize = payload.len() + NUM_HEADER_BYTES;
        let chunk_end = self.buffer_offset + num_chunk_bytes;

        (payload.len() as u32).write_bytes(&mut self.write_buffer, self.buffer_offset + LEN_OFFSET);

        self.write_buffer[self.buffer_offset + TYPE_OFFSET] = if chunk_index == 0 && num_chunks == 1 {
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
            self.write_buffer[self.buffer_offset + PAYLOAD_OFFSET + i] = *x;
        }

        let crc_start = self.buffer_offset + LEN_OFFSET; // Skip crc
        let record_crc = calculate_crc(&self.write_buffer[crc_start..chunk_end]);

        record_crc.write_bytes(&mut self.write_buffer, self.buffer_offset + CRC_OFFSET);

        self.write();

        self.buffer_offset = chunk_end
    }

    pub fn segment_info_snapshot(&self) -> SegmentInfo {
        self.segment_info.clone()
    }

    fn write_footer(&mut self) {
        let mut footer = vec![0; FOOTER_BYTE_COUNT];
        self.append_footer(&mut footer);
        self.file.write_all(&footer).expect("Failed to write");

        self.flush();
    }

    fn append_footer(&self, buffer: &mut Vec<u8>) {
        let info = &self.segment_info;
        (FOOTER_MAGIC_BYTE as u8).write_bytes(buffer, FOOTER_MAGIC_OFFSET);
        (info.index as u64).write_bytes(buffer, FOOTER_INDEX_OFFSET);
        (info.buffer_size as u64).write_bytes(buffer, FOOTER_BUFFER_SIZE_OFFSET);
        (info.start_offset as u64).write_bytes(buffer, FOOTER_START_INDEX_OFFSET);
        (info.next_offset as u64).write_bytes(buffer, FOOTER_NEXT_INDEX_OFFSET);
    }
}

impl Drop for SegmentWriter {
    fn drop(&mut self) {
        self.write_footer();
    }
}

pub const CRC_OFFSET: usize = 0;     // 0-3
pub const LEN_OFFSET: usize = 4;     // 4-7
pub const TYPE_OFFSET: usize = 8;    // 8
pub const PAYLOAD_OFFSET: usize = 9; // 9 - ??

pub const NUM_HEADER_BYTES: usize = 9; // crc(4) + length(4) + type(1)

#[derive(Copy, Clone, PartialEq)]
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

pub trait Persistable<T> {
    fn write_bytes(self, buffer: &mut Vec<u8>, index: usize) -> Result<(), &'static str>;
    fn read_bytes(buffer: &[u8], index: usize) -> Result<T, &'static str>;
}

impl Persistable<u32> for u32 {
    fn write_bytes(self, buffer: &mut Vec<u8>, index: usize) -> Result<(), &'static str> {
        if index + 4 > buffer.len() {
            return Result::Err("Not enough space to write");
        }

        for i in 0..4 {
            let next_byte = (self >> (i << 3)) as u8;
            buffer[index + i] = next_byte;
        }

        Result::Ok(())
    }

    fn read_bytes(buffer: &[u8], index: usize) -> Result<u32, &'static str> {
        if index + 4 > buffer.len() {
            return Result::Err("Not enough readable bytes")
        }

        let mut result = 0u32;
        for i in 0..4 {
            let next_byte = buffer[index + i] as u32;

            result |= next_byte << (i << 3);
        }

        Result::Ok(result)
    }
}

impl Persistable<u64> for u64 {
    fn write_bytes(self, buffer: &mut Vec<u8>, index: usize) -> Result<(), &'static str> {
        if index + 8 > buffer.len() {
            return Result::Err("Not enough space to write");
        }

        for i in 0..8 {
            let next_byte = (self >> (i << 3)) as u8;
            buffer[index + i] = next_byte;
        }


        Result::Ok(())
    }

    fn read_bytes(buffer: &[u8], index: usize) -> Result<u64, &'static str> {
        if index + 8 > buffer.len() {
            return Result::Err("Not enough readable bytes")
        }

        let mut result = 0u64;
        for i in 0..8 {
            let next_byte = buffer[index + i] as u64;

            result |= next_byte << (i << 3);
        }

        Result::Ok(result)
    }
}

impl Persistable<u8> for u8 {
    fn write_bytes(self, buffer: &mut Vec<u8>, index: usize) -> Result<(), &'static str> {
        if index + 1 > buffer.len() {
            return Result::Err("Not enough space to write");
        }

        for i in 0..1 {
            let next_byte = (self >> (i << 3)) as u8;
            buffer[index + i] = next_byte;
        }

        Result::Ok(())
    }

    fn read_bytes(buffer: &[u8], index: usize) -> Result<u8, &'static str> {
        if index + 1 > buffer.len() {
            return Result::Err("Not enough readable bytes")
        }

        let mut result = 0u8;
        for i in 0..1 {
            let next_byte = buffer[index + i] as u8;

            result |= next_byte << (i << 3);
        }

        Result::Ok(result)
    }
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

    fn write_messages_to_segment(path: &Path, buffer_size: usize, messages: &[&[u8]]) -> (Vec<u8>, SegmentInfo) {
        fs::remove_file(&path);

        let segment_info = {
            let segment_info = SegmentInfo::new(path, 0, 0, buffer_size);
            let mut seg_writer = SegmentWriter::new(segment_info);

            for message in messages {
                seg_writer.append(&message);
            }

            seg_writer.segment_info_snapshot()
        };

        let file = File::open(&path).unwrap();
        let mut segment_bytes: Vec<u8> = file.bytes().map(|b| b.unwrap()).collect();
        let num_payload_bytes = segment_bytes.len() - FOOTER_BYTE_COUNT;
        segment_bytes.truncate(num_payload_bytes);

        (segment_bytes, segment_info)
    }

    fn validate_full_message(segment_bytes: &[u8], message: &[u8], offset: usize) {
        assert_eq!(u32::read_bytes(segment_bytes, offset + LEN_OFFSET).unwrap(), message.len() as u32);
        assert_eq!(segment_bytes[offset + TYPE_OFFSET], ChunkType::Full as u8);
        assert_eq!(message, &segment_bytes[(offset + PAYLOAD_OFFSET)..(offset + PAYLOAD_OFFSET + message.len())]);
    }

    #[test]
    fn test_single_append_full_initial() {
        let path = Path::new("./test_data/segments/test_single_append_full_initial");
        let message = vec![0, 1, 2, 3, 4];
        let (segment_bytes, segment_info) = write_messages_to_segment(&path, 16, &[&message]);
        assert_eq!(segment_bytes.len(), 16);

        validate_full_message(&segment_bytes, &message, 0);
        assert_eq!(segment_info, SegmentInfo::from_file(&path));
        assert_eq!(segment_info.index, 0);
        assert_eq!(segment_info.start_offset, 0);
        assert_eq!(segment_info.next_offset, 1);
    }

    #[test]
    fn test_single_append_split() {
        let path = Path::new("./test_data/segments/test_append_split");
        let message = vec![0, 1, 2, 3, 4, 5, 6, 7];

        let (segment_bytes, _) = write_messages_to_segment(&path, 16, &[&message]);
        assert_eq!(segment_bytes.len(), 32);

        assert_eq!(u32::read_bytes(&segment_bytes, LEN_OFFSET).unwrap(), 7);
        assert_eq!(segment_bytes[TYPE_OFFSET], ChunkType::Start as u8);
        assert_eq!(message[0..7], segment_bytes[PAYLOAD_OFFSET..(PAYLOAD_OFFSET + 7)]);

        assert_eq!(u32::read_bytes(&segment_bytes, 16 + LEN_OFFSET).unwrap(), 1);
        assert_eq!(segment_bytes[16 + TYPE_OFFSET], ChunkType::End as u8);
        assert_eq!(message[7], segment_bytes[16 + PAYLOAD_OFFSET]);
    }

    #[test]
    fn test_multi_append_full_initial() {
        let path = Path::new("./test_data/segments/test_multi_append_full_initial");
        let initial_message = vec![42];
        let seconday_message = vec![0, 1, 2, 3, 4];
        let (segment_bytes, _) = write_messages_to_segment(&path, 32, &[&initial_message, &seconday_message]);
        assert_eq!(segment_bytes.len(), 32);

        // Initial message
        validate_full_message(&segment_bytes, &initial_message, 0);

        // Seconday message
        let secondary_message_offset = initial_message.len() + NUM_HEADER_BYTES;
        assert_eq!(u32::read_bytes(&segment_bytes, secondary_message_offset + LEN_OFFSET).unwrap(), seconday_message.len() as u32);
        assert_eq!(segment_bytes[secondary_message_offset + TYPE_OFFSET], ChunkType::Full as u8);
        let actual_secondary_message = &segment_bytes[secondary_message_offset + PAYLOAD_OFFSET..(secondary_message_offset + PAYLOAD_OFFSET + seconday_message.len())];
        assert_eq!(&seconday_message[0..seconday_message.len()], actual_secondary_message);
    }

    #[test]
    fn test_multi_append_partial_initial() {
        let path = Path::new("./test_data/segments/test_multi_append_partial_initial");
        let initial_message = vec![42]; // 10 bytes
        let seconday_message = vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13]; // 23 bytes
        let (segment_bytes, _) = write_messages_to_segment(&path, 32, &[&initial_message, &seconday_message]); // 10 + 23 > 32
        assert_eq!(segment_bytes.len(), 64);

        // Inital message
        validate_full_message(&segment_bytes, &initial_message, 0);

        // Secondary message head
        let head_offset = initial_message.len() + NUM_HEADER_BYTES;
        assert_eq!(u32::read_bytes(&segment_bytes, head_offset + LEN_OFFSET).unwrap(), 13);
        assert_eq!(segment_bytes[head_offset + TYPE_OFFSET], ChunkType::Start as u8);
        let actual_secondary_message = &segment_bytes[(head_offset + PAYLOAD_OFFSET)..(head_offset + PAYLOAD_OFFSET + 13)];
        assert_eq!(&seconday_message[0..13], actual_secondary_message);

        // Seconday message tail
        let tail_offset = 32;
        assert_eq!(u32::read_bytes(&segment_bytes, tail_offset + LEN_OFFSET).unwrap(), 1);
        assert_eq!(segment_bytes[tail_offset + TYPE_OFFSET], ChunkType::End as u8);
        let actual_secondary_message = &segment_bytes[tail_offset + PAYLOAD_OFFSET..(tail_offset + PAYLOAD_OFFSET + 1)];
        assert_eq!(&seconday_message[13..14], actual_secondary_message);
    }

    #[test]
    fn test_multi_append_none_initial() {
        let path = Path::new("./test_data/segments/test_multi_append_none_initial");
        let initial_message = vec![42];
        let seconday_message = vec![0, 1, 2, 3, 4];
        let (segment_bytes, _) = write_messages_to_segment(&path, 16, &[&initial_message, &seconday_message]);
        assert_eq!(segment_bytes.len(), 32);

        // Initial message
        validate_full_message(&segment_bytes, &initial_message, 0);

        // Seconday message
        let secondary_message_offset = 16;
        assert_eq!(u32::read_bytes(&segment_bytes, secondary_message_offset + LEN_OFFSET).unwrap(), seconday_message.len() as u32);
        assert_eq!(segment_bytes[secondary_message_offset + TYPE_OFFSET], ChunkType::Full as u8);
        let actual_secondary_message = &segment_bytes[secondary_message_offset + PAYLOAD_OFFSET..(secondary_message_offset + PAYLOAD_OFFSET + seconday_message.len())];
        assert_eq!(&seconday_message[0..seconday_message.len()], actual_secondary_message);
    }

    #[test]
    fn test_iter_closed_segment() {
        let path = Path::new("./test_data/segments/test_iter_closed_segment");
        let initial_message = vec![42]; // 10 bytes
        let seconday_message = vec![0, 1, 2, 3, 4]; // 14 bytes
        let (_, segment) = write_messages_to_segment(&path, 16, &[&initial_message, &seconday_message]);

        let mut iter = segment.iter();

        assert_eq!(iter.next(), Some(initial_message)); // Full message
        assert_eq!(iter.next(), Some(seconday_message)); // Split message
        assert_eq!(iter.next(), None);
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn test_iter_open_segment() {
        let path = Path::new("./test_data/segments/test_iter_open_segment");
        let first_message = vec![42]; // 10 bytes
        let second_message = vec![0, 1, 2, 3, 4]; // 14 bytes
        let third_message = vec![56]; // 10 bytes

        fs::remove_file(&path);

        let segment_info = SegmentInfo::new(path, 0, 0, 32);
        let mut segment = SegmentWriter::new(segment_info);
        segment.append(&first_message);

        let mut iter = segment.segment_info.iter();
        let read_one = iter.next(); // Read *before* next message is written

        segment.append(&second_message); // Written *after* iter buffer has been filled

        let read_two = iter.next();
        let read_three = iter.next();

        assert_eq!(read_one, Some(first_message)); // Full message
        assert_eq!(read_two, Some(second_message)); // Split message
        assert_eq!(read_three, None);

        segment.append(&third_message);
        let read_four = iter.next();
        assert_eq!(read_four, Some(third_message));
    }
}
