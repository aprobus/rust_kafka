#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(unused_imports)]
#![allow(unused_must_use)]

extern crate crc;
extern crate rand;

mod segment;
mod topic;
mod kafka;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
    }
}
