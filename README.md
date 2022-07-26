# pulsar-rs producer example

Example for using the pulsar-rs crate to write to a topic.
Read lines from a file to send as messages using zstd compression.

## To Run
1. `cp config.sample.toml config.toml`
1. Fill in the pulsar hostname, port, tenant, namespace, topic, token, and filename in the `config.toml`
1. `RUST_LOG=info cargo run`
