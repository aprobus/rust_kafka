use std::path::Path;
use std::path::PathBuf;
use std::fs::{self, DirEntry};
use std::fs::File;
use std::io;

use segment::Segment;
use segment::write_payload;

pub struct Topic {
    dir: PathBuf,
    segments: Vec<Segment>,
    buffer: Vec<u8>
}

impl Topic {
    pub fn new(path: &Path) -> io::Result<Topic> {
        let path_buf = path.to_path_buf();
        let mut segments = Vec::new();

        for entry in try!(fs::read_dir(&path_buf)) {
            let entry = try!(entry);
            let path = entry.path();

            if !path.is_file() {
                continue;
            }

            if let Some(file_name_str) = path.file_name().and_then(|n| n.to_str()) {
                if file_name_str.starts_with("segment_") {
                    let offset = file_name_str.replace("segment_", "").parse::<usize>().unwrap();

                    println!("Found segment file: {:?}, and offset {}", file_name_str, offset);

                    let segment = Segment::new(&path, offset);
                    segments.push(segment);
                }
            }
        }

        let buffer = vec![0; 256];

        let topic = Topic { dir: path_buf, segments: segments, buffer: buffer };
        Ok(topic)
    }

    pub fn produce(&mut self, message: &[u8]) -> Result<(), &'static str> {
        let next_offset = self.segments.last().map(|segment| segment.offset + 1).unwrap_or(0);

        let mut path = PathBuf::from(&self.dir);
        path.push(format!("segment_{:09}", next_offset));

        let segment = Segment::new(&path, next_offset);
        self.segments.push(segment);

        let mut file = File::create(path).unwrap();

        write_payload(&mut file, &mut self.buffer, message);

        println!("Next offset: {}", next_offset);
        Ok(())
    }
}
