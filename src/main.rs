extern crate crc;

use std::fs::File;
use std::mem;
use std::io::prelude::*;
use crc::{crc32, Hasher32};

fn main() {
    {
        let mut f = File::create("foo.txt").unwrap();
        let payload: Vec<u8> = vec!(8, 1, 3);
        write_payload(&mut f, &payload);
    }

    {
        let mut buffer = Vec::with_capacity(256);
        for _ in 0..buffer.capacity() {
            buffer.push(0);
        }

        let mut f = File::open("foo.txt").unwrap();
        let payload = read_payload(&mut f, &mut buffer);
        println!("Resulting payload: {:?}", payload);
    }

    println!("Hello world");
}

fn write_payload(file: &mut File, payload: &Vec<u8>) {
    let len_bytes: [u8; 8] = unsafe { std::mem::transmute(payload.len()) };
    println!("Writing length ({}): {:?}", payload.len(), len_bytes);

    let record_crc = calculate_crc(payload.len(), payload);
    let crc_bytes: [u8; 4] = unsafe { std::mem::transmute(record_crc) };
    println!("Digest ({:?}): {}", crc_bytes, record_crc);

    file.write_all(&crc_bytes).expect("Failed to write length");
    file.write_all(&len_bytes).expect("Failed to write length");
    file.write_all(&payload).expect("Failed to write");

    file.flush().expect("Failed to flush");
    file.sync_all().expect("Failed to sync");
}

fn read_payload(file: &mut File, buffer: &mut Vec<u8>) -> Result<Vec<u8>, &'static str> {
    let read_size = file.read(buffer).unwrap();

    if read_size < 12 {
        panic!("Read less than crc(4) + length(8) bytes!");
    }

    let crc_end = mem::size_of::<u32>();
    let len_end = crc_end + mem::size_of::<usize>();

    //TODO: Little Endian vs Big Endian safety

    let expected_crc: u32 = read_u32(&buffer, 0, crc_end);
    let payload_len = read_usize(&buffer, crc_end, len_end);

    let mut payload = Vec::with_capacity(payload_len);
    for i in len_end..read_size {
        payload.push(buffer[i]);
    }
    while payload.len() < payload_len {
        let read_size = file.read(buffer).unwrap();
        for i in 0..read_size {
            payload.push(buffer[i]);
        }
    }

    let actual_crc = calculate_crc(payload_len, &payload);

    if actual_crc == expected_crc {
        Result::Ok(payload)
    } else {
        println!("Mismatched crc: {} vs {}", actual_crc, expected_crc);
        Result::Err("Mismatched crc")
    }
}

fn read_u32(buffer: &Vec<u8>, start: usize, end: usize) -> u32 {
    let mut result: u32 = 0;
    for i in start..end {
        let next_byte: u32 = buffer[i] as u32;
        result = (result >> 8) | (next_byte << 24);
    }

    result
}

fn read_usize(buffer: &Vec<u8>, start: usize, end: usize) -> usize {
    let mut result: usize = 0;
    for i in start..end {
        let next_byte: usize = (buffer[i] as usize) << 56;
        result = (result >> 8) | next_byte;
    }
    result
}

fn calculate_crc(length: usize, payload: &Vec<u8>) -> u32 {
    let mut digest = crc32::Digest::new(crc32::IEEE);

    let len_bytes: [u8; 8] = unsafe { std::mem::transmute(length) };
    digest.write(&len_bytes);

    digest.write(&payload);
    digest.sum32()
}
