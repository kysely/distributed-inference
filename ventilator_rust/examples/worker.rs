extern crate pretty_env_logger;
extern crate ventilator;
extern crate rand;
extern crate zmq;
#[macro_use]
extern crate log;

use ventilator::Worker;
use rand::prelude::*;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration};

// const RESPONSE: &'static str = "{\"y\":[43.9310612026,43.9310612026]}";


// fn main() {
//     pretty_env_logger::init_timed();

//     let mut worker = Worker::connect("tcp://localhost:5555", "tcp://localhost:5556")
//         .expect("Couldn't connect the worker");

//     loop {
//         let payload = worker.recv().expect("Couldn't receive msg");
//         worker.reply(payload).expect("Couldn't reply");
//     }

// }


fn main() {
    pretty_env_logger::init_timed();

    let id = rand::thread_rng().gen_range(1000, 10000);

    let msgs_count = Arc::new(Mutex::new(0));
    let msgs_count_thread = msgs_count.clone();
    // Monitoring thread
    thread::spawn(move || loop {
        thread::sleep(Duration::new(1, 0));
        let mut count = msgs_count_thread.lock().unwrap();
        info!("[W {:?}] Current throughput {:?} msgs", id, *count);
        *count = 0;
    });

    // Main worker thread
    let ctx = zmq::Context::new();
    let from_vent_url = "tcp://localhost:5555";
    let to_vent_url = "tcp://localhost:5556";

    let from_vent = ctx.socket(zmq::PULL).unwrap();
    from_vent.connect(from_vent_url).unwrap();
    from_vent.set_rcvhwm(1000000).unwrap();

    let to_vent = ctx.socket(zmq::PUSH).unwrap();
    to_vent.connect(to_vent_url).unwrap();
    to_vent.set_linger(0).unwrap();
    to_vent.set_sndhwm(1000000).unwrap();

    info!(
        "[W {:?}] Running worker {:?} -> {:?}",
        id, from_vent_url, to_vent_url
    );
    loop {
        // TODO: do something more than echo
        let payload = from_vent
            .recv_multipart(0)
            .expect("Couldn't receive a request");
        debug!("[W {:?}] New payload: {:?}", id, payload);

        let (last_part, first_parts) = payload.split_last().unwrap();
        for part in first_parts.iter() {
            to_vent.send(part, zmq::SNDMORE).expect("Couldn't send payload");
        }
        to_vent.send(last_part, 0).expect("Couldn't send payload");

        *msgs_count.lock().unwrap() += 1;
    }
}
