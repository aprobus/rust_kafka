use std::path::Path;
use std::path::PathBuf;
use std::collections::LinkedList;
use std::rc::Rc;
use std::fs::{self, DirEntry};
use std::fs::File;
use std::io;

use segment::SegmentInfo;
use segment::SegmentWriter;
use segment::SegmentIterator;

pub struct TopicIterator {
    segments: LinkedList<Rc<SegmentInfo>>,
    segment_iter: Option<SegmentIterator>
}

impl TopicIterator {
    fn new(segments: LinkedList<Rc<SegmentInfo>>) -> TopicIterator {
        TopicIterator { segments: segments, segment_iter: None }
    }
}

impl Iterator for TopicIterator {
    type Item = Vec<u8>;

    fn next(&mut self) -> Option<Vec<u8>> {
        let message = self.segment_iter.as_mut().and_then(|iter| iter.next());

        if let Some(value) = message {
            Some(value)
        } else {
            if let Some(segment) = self.segments.pop_front().as_ref() {
                self.segment_iter = Some(segment.iter());
                self.segment_iter.as_mut().and_then(|iter| iter.next())
            } else {
                None
            }
        }
    }
}

pub struct Topic {
    dir: PathBuf,
    segments: Vec<Rc<SegmentInfo>>,
    open_segment: Option<SegmentWriter>,
    buffer_size: usize
}

impl Topic {
    pub fn new(path: &Path, buffer_size: usize) -> io::Result<Topic> {
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

                    let segment = Rc::new(SegmentInfo::new(&path, offset, buffer_size));
                    segments.push(segment);
                }
            }
        }

        let topic = Topic { dir: path_buf, segments: segments, open_segment: None, buffer_size: buffer_size };
        Ok(topic)
    }

    pub fn produce(&mut self, message: &[u8]) -> Result<(), &'static str> {
        if self.open_segment.is_none() {
            let next_offset = self.segments.last().map(|segment| segment.index + 1).unwrap_or(0);

            let mut path = PathBuf::from(&self.dir);
            path.push(format!("segment_{:09}", next_offset));

            let segment_info = SegmentInfo::new(&path, next_offset, self.buffer_size);
            self.open_segment = Some(SegmentWriter::new(segment_info));
        }

        let mut segment = self.open_segment.as_mut().unwrap();
        segment.append(message);

        Ok(())
    }

    pub fn close(&mut self) {
        if let Some(segment_info) = self.open_segment.take().map(|segment| Rc::new(segment.segment_info_snapshot())) {
            self.segments.push(segment_info);
        }
    }

    pub fn iter(&self) -> TopicIterator {
        let mut segments = LinkedList::new();

        for segment in &self.segments {
            segments.push_back(segment.clone());
        }

        TopicIterator::new(segments)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;
    use std::fs;
    use std::fs::File;
    use std::io::Read;

    #[test]
    fn test_topic_iter() {
        let path = Path::new("./test_data/topics/test_topic_iter");
        fs::remove_dir_all(&path);

        let mut topic = Topic::new(&path, 64).unwrap();

        let message_1 = vec![0, 1];
        let message_2 = vec![1, 2];

        topic.produce(&message_1);
        topic.close();

        topic.produce(&message_2);
        topic.close();

        let mut iter = topic.iter();
        let actual_messages = vec!(iter.next(), iter.next(), iter.next());
        assert_eq!(actual_messages[0], Some(message_1));
        assert_eq!(actual_messages[1], Some(message_2));
        assert_eq!(actual_messages[2], None);
    }
}
