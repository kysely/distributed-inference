extern crate pretty_env_logger;
extern crate ventilator;

use ventilator::Worker;

fn main() {
    pretty_env_logger::init_timed();

    let mut worker = Worker::connect("tcp://localhost:5555", "tcp://localhost:5556")
        .expect("Couldn't connect the worker");
    
    Worker::monitor(worker.msg_count.clone(), worker.id);

    loop {
        let payload = worker.recv().expect("Couldn't receive msg");
        worker.reply(payload).expect("Couldn't reply");
        worker.inc();
    }
}