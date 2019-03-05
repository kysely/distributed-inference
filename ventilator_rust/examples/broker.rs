extern crate pretty_env_logger;
extern crate zmq;
#[macro_use]
extern crate log;

use std::thread;

fn main() {
    pretty_env_logger::init_timed();

    let listen_url = "tcp://*:5554";
    let to_workers_url = "tcp://*:5555";
    let from_workers_url = "tcp://*:5556";
    let return_url = "tcp://*:5557";

    // THREAD: client -> workers
    thread::spawn(move || {
        let ctx_thread = zmq::Context::new();

        // PULL from client
        let pull = ctx_thread.socket(zmq::PULL).unwrap();
        pull.set_rcvhwm(1000000).unwrap();
        pull.bind(listen_url).unwrap();

        // PUSH
        let to_workers = ctx_thread.socket(zmq::PUSH).unwrap();
        to_workers.set_linger(0).unwrap();
        to_workers.set_sndhwm(1000000).unwrap();
        to_workers.bind(to_workers_url).unwrap();

        loop {
            let payload = pull.recv_msg(0).unwrap();
            if pull.get_rcvmore().unwrap() {
                to_workers.send_msg(payload, zmq::SNDMORE).unwrap();
            } else {
                to_workers.send_msg(payload, 0).unwrap();
            }
        }
    });

    // THREAD: workers -> clients
    let ctx = zmq::Context::new();
    // PULL
    let from_workers = ctx.socket(zmq::PULL).unwrap();
    from_workers.set_rcvhwm(1000000).unwrap();
    from_workers.bind(from_workers_url).unwrap();

    // ROUTER to client
    let router = ctx.socket(zmq::ROUTER).unwrap();
    router.set_linger(0).unwrap();
    router.set_sndhwm(1000000).unwrap();
    router.bind(return_url).unwrap();

    loop {
        let payload = from_workers.recv_msg(0).unwrap();
        if from_workers.get_rcvmore().unwrap() {
            router.send_msg(payload, zmq::SNDMORE).unwrap();
        } else {
            router.send_msg(payload, 0).unwrap();
        }
    }
}
