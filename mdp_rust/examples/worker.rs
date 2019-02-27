extern crate majordomo;
extern crate pretty_env_logger;
use majordomo::MajordomoWorker;

fn main() {
    pretty_env_logger::init_timed();
    
    let mut worker = MajordomoWorker::connect("tcp://localhost:5555", "echo", 2000)
        .expect("Couldn't connect to the broker");

    loop {
        let (c_addr, id, payload) = worker.recv().expect("Couldn't receive a request");
        worker.reply(c_addr, id, payload).expect("Couldn't send a reply");
    }
}
