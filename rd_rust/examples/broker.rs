extern crate rd;
extern crate pretty_env_logger;
use rd::RDBroker;

fn main() {
    pretty_env_logger::init_timed();

    let mut broker = RDBroker::new("tcp://*:5555", "tcp://*:5556").expect("Couldn't bind the broker");
    broker.start().expect("Error during broker activity...");
}
