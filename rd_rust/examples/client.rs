#[macro_use]
extern crate log;
extern crate rd;
extern crate pretty_env_logger;

use rd::RDClient;
use std::time::Instant;

fn main() {
    pretty_env_logger::init_timed();

    let client = RDClient::connect("tcp://localhost:5555")
        .expect("Couldn't connect to the broker");

    let count = 50_000;

    let start = Instant::now();
    let mut i = 0;
    while i < count {
        let id_s = &i.to_string();
        let id = id_s.as_bytes();
        let request = String::from("Hello World");
        client.send(&id, &request).expect("Couldn't send a request");
        i += 1;
    }

    let mut j = 0;
    while j < count {
        client.recv().expect("Couldn't receive a message");
        j += 1;
    }
    let duration = start.elapsed();
    info!(
        "Sending out and receiving {:?} messages took {:?}.{:?}s",
        count,
        duration.as_secs(),
        duration.subsec_millis())
}
