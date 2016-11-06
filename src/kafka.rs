use std::collections::HashMap;
use std::path::Path;
use std::path::PathBuf;
use std::fs::{self, DirEntry};
use std::io;

use topic::Topic;

const BUFFER_SIZE: usize = 512;

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
                let topic = Topic::new(&path, BUFFER_SIZE).unwrap();
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

            return Topic::new(&path, BUFFER_SIZE).unwrap();
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
    use std::time::{Duration, SystemTime};

    use std::io;

    use rand::Rng;
    use rand;

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

    #[test]
    #[ignore]
    fn test_produce_throughput_perf () {
        let path = Path::new("./test_data/test_produce_throughput_perf");
        let mut kafka = init_kafka_for_test(&path);

        let start_time = SystemTime::now();

        let test_duration = Duration::from_secs(60);;
        let mut num_messages_produced = 0;
        let test_message_size = 256;
        let mut message = vec![0; test_message_size];

        let mut rng = rand::thread_rng();
        loop {
            if start_time.elapsed().unwrap() > test_duration {
                break;
            }

            for i in 0..test_message_size {
                message[i] = rng.gen::<u8>();
            }

            let result = kafka.produce("foo", &message);
            assert!(result.is_ok());
            num_messages_produced += 1;
        }

        println!("Produced produced: {}", num_messages_produced);

        // Message Size: 256
        // Duration: 60 seconds
        //
        // | Write Size | Min Writes | Max Writes |
        // | ---------- | ---------- | ---------- |
        // | 512        | 65910      | 67659      |
        // | 516        | 56566      | 57408      |
    }

    #[test]
    #[ignore]
    fn test_produce_size_perf () {
        let path = Path::new("./test_data/test_produce_size_perf");
        let mut kafka = init_kafka_for_test(&path);

        let test_num_produces = 40000;
        let test_message_size = 256;
        let mut message = vec![0; test_message_size];

        let mut rng = rand::thread_rng();
        for _ in 0..test_num_produces {
            for i in 0..test_message_size {
                message[i] = rng.gen::<u8>();
            }

            let result = kafka.produce("foo", &message);
            assert!(result.is_ok());
        }

        let disk_size = calculate_dir_size(&path).unwrap();
        println!("Size: {}", disk_size);

        // Message Size: 256
        // Messages: 40,000
        //
        // | Write Size | Min Size | Max Size |
        // | ---------- | -------- | -------- |
        // | 512        | 19.5M    | 19.5M    |
    }

    fn init_kafka_for_test(path: &Path) -> Kafka {
        fs::remove_dir_all(path);

        let path = Path::new(path);

        let mut kafka = Kafka::new(&path).unwrap();
        assert!(kafka.open().is_ok());
        kafka
    }

    fn calculate_dir_size(dir: &Path) -> io::Result<u64> {
        let mut dir_size = 0;

        if dir.is_dir() {
            for entry in try!(fs::read_dir(dir)) {
                let entry = try!(entry);
                let path = entry.path();
                if path.is_dir() {
                    dir_size += try!(calculate_dir_size(&path));
                } else {
                    dir_size += path.metadata().unwrap().len();
                }
            }
        }
        Ok(dir_size)
    }
}
