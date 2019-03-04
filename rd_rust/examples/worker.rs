extern crate rd;
extern crate pretty_env_logger;
use rd::RDWorker;

fn main() {
    pretty_env_logger::init_timed();
    
    let mut worker = RDWorker::connect("tcp://localhost:5556")
        .expect("Couldn't connect to the broker");

    loop {
        let msg = worker.recv().expect("Error during waiting for request");
        worker.reply(msg).expect("Couldn't send a reply");
    }
}
