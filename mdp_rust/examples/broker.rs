extern crate majordomo;
extern crate pretty_env_logger;
use majordomo::MajordomoBroker;

fn main() {
    pretty_env_logger::init_timed();

    let mut broker = MajordomoBroker::new("tcp://*:5555", 1000).expect("Couldn't bind the broker");
    broker.start().expect("Error during broker activity...");
}
