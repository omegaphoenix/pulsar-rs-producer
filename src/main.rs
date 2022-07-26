mod config;
use pulsar::producer::ProducerOptions;
use pulsar::proto::CompressionType;
use pulsar::{Authentication, Pulsar, TokioExecutor};
use serde::Deserialize;
use std::fs::File;
use std::io::{self, BufRead};
use std::path::Path;
use std::time::Duration;

pub async fn delay_ms(ms: usize) {
    tokio::time::sleep(Duration::from_millis(ms as u64)).await;
}

#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Config {
    pub pulsar: PulsarConfig,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct PulsarConfig {
    pub hostname: String,
    pub port: u16,
    pub tenant: String,
    pub namespace: String,
    pub topic: String,
    pub token: Option<String>,
    /// File to read messages from to send
    pub filename: String,
}

async fn get_pulsar_client(config: Config) -> Result<Pulsar<TokioExecutor>, pulsar::Error> {
    let addr = format!(
        "pulsar+ssl://{}:{}",
        config.pulsar.hostname, config.pulsar.port
    );
    let mut builder = Pulsar::builder(addr, TokioExecutor);

    let authentication = Authentication {
        name: "token".to_string(),
        data: config.pulsar.token.unwrap().into_bytes(),
    };
    builder = builder.with_auth(authentication);

    builder.build().await
}

// The output is wrapped in a Result to allow matching on errors
// Returns an Iterator to the Reader of the lines of the file.
fn read_lines<P>(filename: P) -> io::Result<io::Lines<io::BufReader<File>>>
where
    P: AsRef<Path>,
{
    let file = File::open(filename)?;
    Ok(io::BufReader::new(file).lines())
}

#[tokio::main]
async fn main() {
    env_logger::init();

    let config: Config = config::load().expect("Unable to load config");
    let namespace = config.pulsar.namespace.clone();
    let topic = config.pulsar.topic.clone();
    let input_filename = config.pulsar.filename.clone();
    let pulsar_client = get_pulsar_client(config)
        .await
        .expect("Failed to build pulsar client");

    let full_topic_name = format!("persistent://public/{}/{}", &namespace, &topic);

    let mut producer = pulsar_client
        .producer()
        .with_topic(full_topic_name)
        .with_name("test_producer")
        .with_options(ProducerOptions {
            compression: Some(CompressionType::Zstd),
            ..ProducerOptions::default()
        })
        .build()
        .await
        .expect("Failed to create producer");

    let mut count = 0;
    if let Ok(lines) = read_lines(input_filename) {
        // Consumes the iterator, returns an (Optional) String
        for line in lines {
            if let Ok(message) = line {
                producer
                    .send(message)
                    .await
                    .expect("Failed to send message");
                count += 1;

                if count % 100 == 0 {
                    log::info!("Sent {} messages", count);
                }
            }
        }
    }
    log::info!("Sent {} messages", count);
}
