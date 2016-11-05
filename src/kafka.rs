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
    fn new(dir: &Path) -> io::Result<Kafka> {
        try!(fs::create_dir_all(&dir));

        let topics = HashMap::new();
        let kafka = Kafka { dir: dir.to_path_buf(), topics: topics };
        Ok(kafka)
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

    fn close(&mut self) {
        for topic in self.topics.values_mut() {
            topic.close();
        }
    }

    fn produce(&mut self, topic_name: &str, message: &[u8]) -> Result<(), &'static str> {
        let base_dir = &self.dir;
        let topic = self.topics.entry(topic_name.to_string()).or_insert_with(|| {
            let mut path = PathBuf::from(base_dir);
            path.push(topic_name);

            return Topic::new(&path).unwrap();
        });

        return topic.produce(message);
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
    use std::fs;

    #[test]
    fn test_open () {
        let path = Path::new("./test_data/test_open");
        let mut kafka = Kafka::new(&path).unwrap();
        assert!(kafka.open().is_ok());

        let topics: Vec<&String> = kafka.topics.keys().collect();
        assert_eq!(topics, vec!["foo"]);
    }

    #[test]
    fn test_produce () {
        fs::remove_dir_all("./test_data/test_produce/foo");

        let path = Path::new("./test_data/test_produce");

        let mut kafka = Kafka::new(&path).unwrap();
        assert!(kafka.open().is_ok());

        let result = kafka.produce("foo", &vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
        assert!(result.is_ok());

        let second_result = kafka.produce("foo", &vec![10, 11, 12, 13, 14, 15, 16, 17, 18, 19]);
        assert!(second_result.is_ok());
    }
}
