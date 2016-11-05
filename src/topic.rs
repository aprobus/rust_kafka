use std::path::Path;
use std::path::PathBuf;
use std::fs::{self, DirEntry};
use std::fs::File;
use std::io;

use segment::Segment;

pub struct Topic {
    dir: PathBuf,
    segments: Vec<Segment>,
    buffer: Vec<u8>,
    current_segment: Option<Segment>
}

impl Topic {
    pub fn new(path: &Path) -> io::Result<Topic> {
        let path_buf = path.to_path_buf();

        println!("Creating dir: {:?}", &path_buf);
        try!(fs::create_dir_all(&path_buf));

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

        let buffer = vec![0; 512];

        let topic = Topic { dir: path_buf, segments: segments, buffer: buffer, current_segment: None };
        Ok(topic)
    }

    pub fn produce(&mut self, message: &[u8]) -> Result<(), &'static str> {
        if self.current_segment.is_none() {
            let next_offset = self.segments.last().map(|segment| segment.offset + 1).unwrap_or(0);

            let mut path = PathBuf::from(&self.dir);
            path.push(format!("segment_{:09}", next_offset));

            let segment = Segment::new(&path, next_offset);
            self.current_segment = Some(segment);
        }

        let mut segment = self.current_segment.as_mut().unwrap();
        segment.append(&mut self.buffer, message);

        Ok(())
    }

    pub fn close(&mut self) {
        if let Some(segment) = self.current_segment.as_mut() {
            segment.close();
        }
    }
}
