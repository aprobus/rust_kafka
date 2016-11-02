use std::collections::HashMap;
use std::path::Path;
use std::path::PathBuf;
use std::fs::{self, DirEntry};
use std::io;

use topic::Topic;

struct Kafka {
    dir: PathBuf,
    topics: HashMap<String, Topic>
}

impl Kafka {
    fn new(dir: &Path) -> Kafka {
        let topics = HashMap::new();
        Kafka { dir: dir.to_path_buf(), topics: topics }
    }

    fn open(&mut self) -> io::Result<()> {
        for entry in try!(fs::read_dir(&self.dir)) {
            let entry = try!(entry);
            let path = entry.path();

            if path.is_dir() {
                let topic_name = path.file_name().unwrap().to_str().unwrap().to_string();
                println!("Found topic: {:?}", topic_name);
                let topic = Topic::new(&path).unwrap();
                self.topics.insert(topic_name, topic);
            }
        }

        Ok(())
    }

    fn produce(&mut self, topic: &str, message: &[u8]) -> Result<(), &'static str> {
        if let Some(topic) = self.topics.get_mut(topic) {
            return topic.produce(message);
        } else {
            return Err("Topic not found")
        }
    }

    fn seek(&self, topic: &str) -> Result<(), &'static str> {
        Result::Ok(())
    }

    fn consume(&self, topic: &str) -> Option<Vec<u8>> {
        Option::None
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use super::*;
    use super::Kafka;

    #[test]
    fn test_open () {
        let path = Path::new("./test_data/test_open");
        let mut kafka = Kafka::new(&path);
        assert!(kafka.open().is_ok());

        let topics: Vec<&String> = kafka.topics.keys().collect();
        assert_eq!(topics, vec!["foo"]);
    }

    #[test]
    fn test_produce () {
        let path = Path::new("./test_data/test_produce");
        let mut kafka = Kafka::new(&path);
        assert!(kafka.open().is_ok());

        let result = kafka.produce("foo", &vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
        assert!(result.is_ok());
    }
}
