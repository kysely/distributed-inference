extern crate pretty_env_logger;
extern crate ventilator;
extern crate rand;
extern crate zmq;
#[macro_use]
extern crate log;

use std::sync::{Arc, Mutex};
use ventilator::Ventilator;
use std::time::{Duration, Instant};
use std::thread;

// fn main() {
//     pretty_env_logger::init_timed();

//     let mut vent = Ventilator::new("tcp://localhost:5555", "tcp://localhost:5556")
//         .expect("Couldn't create vent");
//     vent.start().expect("Error during ventilating");
// }
fn main() {
    pretty_env_logger::init_timed();

    let ctx = zmq::Context::new();
    let ctx_thread = ctx.clone();
    let listen_url = "tcp://*:5554";
    let to_workers_url = "tcp://*:5555";
    let from_workers_url = "tcp://*:5556";
    let return_url = "tcp://*:5557";

    // PULL
    let mut from_workers = ctx.socket(zmq::PULL).unwrap();
    from_workers.bind(from_workers_url).unwrap();
    from_workers.set_rcvhwm(1000000).unwrap();

    // ROUTER to client
    let mut router = ctx.socket(zmq::ROUTER).unwrap();
    router.bind(return_url).unwrap();
    router.set_linger(0).unwrap();
    router.set_sndhwm(1000000).unwrap();

    // THREAD: client -> workers
    thread::spawn(move || {
        let wmsgs_count = Arc::new(Mutex::new(0));
        let wmsgs_count_thread = wmsgs_count.clone();
        // Monitoring thread
        thread::spawn(move || loop {
            thread::sleep(Duration::new(1, 0));
            let mut count = wmsgs_count_thread.lock().unwrap();
            info!("-> W {:?} msgs", *count);
            *count = 0;
        });

        // PULL from client
        let mut pull = ctx_thread.socket(zmq::PULL).unwrap();
        pull.bind(listen_url).unwrap();
        pull.set_rcvhwm(1000000).unwrap();

        // PUSH
        let mut to_workers = ctx_thread.socket(zmq::PUSH).unwrap();
        to_workers.bind(to_workers_url).unwrap();
        to_workers.set_linger(0).unwrap();
        to_workers.set_sndhwm(1000000).unwrap();

        loop {
            let payload = pull.recv_multipart(0).expect("Couldn't PULL from client");

            let (last_part, first_parts) = payload.split_last().unwrap();
            for part in first_parts.iter() {
                to_workers
                    .send(part, zmq::SNDMORE)
                    .expect("Couldn't send payload");
            }
            to_workers.send(last_part, 0).expect("Couldn't send payload");
            *wmsgs_count.lock().unwrap() += 1;
        }
    });


    // THREAD: workers -> clients
    let cmsgs_count = Arc::new(Mutex::new(0));
    let cmsgs_count_thread = cmsgs_count.clone();
    // Monitoring thread
    thread::spawn(move || loop {
        thread::sleep(Duration::new(1, 0));
        let mut count = cmsgs_count_thread.lock().unwrap();
        info!("-> C {:?} msgs", *count);
        *count = 0;
    });

    loop {
        let payload = from_workers.recv_multipart(0).expect("Couldn't PULL from workers");

        let (last_part, first_parts) = payload.split_last().unwrap();
        for part in first_parts.iter() {
            router
                .send(part, zmq::SNDMORE)
                .expect("Couldn't send payload");
        }
        router.send(last_part, 0).expect("Couldn't send payload");
        *cmsgs_count.lock().unwrap() += 1;
    }
}


// fn main() {
//     pretty_env_logger::init_timed();

//     let ctx = zmq::Context::new();
//     // let ctx_push = ctx.clone();
//     let listen_url = "tcp://*:5554";
//     let to_workers_url = "tcp://*:5555";
//     let from_workers_url = "tcp://*:5556";

//     // PUSH
//     let to_workers = ctx.socket(zmq::PUSH).unwrap();
//     to_workers.bind(to_workers_url).unwrap();
//     to_workers.set_linger(0).unwrap();
//     to_workers.set_sndhwm(1000000).unwrap();

//     // PULL
//     let from_workers = ctx.socket(zmq::PULL).unwrap();
//     from_workers.bind(from_workers_url).unwrap();
//     from_workers.set_rcvhwm(1000000).unwrap();


//     // ROUTER
//     let router = ctx.socket(zmq::ROUTER).unwrap();
//     router.bind(listen_url).unwrap();
//     router.set_linger(0).unwrap();
//     router.set_rcvhwm(1000000).unwrap();
//     router.set_sndhwm(1000000).unwrap();

//     loop {
//         let mut items = [
//             router.as_poll_item(zmq::POLLIN),
//             from_workers.as_poll_item(zmq::POLLIN)
//         ];

//         match zmq::poll(&mut items[0..], 1000) {
//             Err(_) => {},
//             Ok(0) => {},
//             Ok(_) => {
//                 if items[0].is_readable() {
//                     // route to worker
//                     let payload = router.recv_multipart(0).unwrap();
//                     println!("{:?}", payload);
//                     let (last_part, first_parts) = payload.split_last().unwrap();
//                     for part in first_parts.iter() {
//                         to_workers
//                             .send(part, zmq::SNDMORE)
//                             .expect("Couldn't send payload");
//                     }
//                     to_workers.send(last_part, 0).expect("Couldn't send payload");
//                 } else if items[1].is_readable() {
//                     // worket to client
//                     let payload = from_workers.recv_multipart(0).unwrap();
//                     let (last_part, first_parts) = payload.split_last().unwrap();
//                     for part in first_parts.iter() {
//                         router
//                             .send(part, zmq::SNDMORE)
//                             .expect("Couldn't send payload");
//                     }
//                     router.send(last_part, 0).expect("Couldn't send payload");
//                 }
//             }
//         }
//     }
// }

// fn main() {
//     pretty_env_logger::init_timed();

//     let ctx = zmq::Context::new();
//     let ctx_push = ctx.clone();
//     let to_workers_url = "tcp://*:5555";
//     let from_workers_url = "tcp://*:5556";

//     let start = Instant::now();
//     // PUSH thread
//     thread::spawn(move || {
//         let to_workers = ctx_push.socket(zmq::PUSH).unwrap();
//         to_workers.bind(to_workers_url).unwrap();
//         to_workers.set_linger(0).unwrap();
//         to_workers.set_sndhwm(1000000).unwrap();
//         let payload = vec![
//             Vec::from("\x00\x10\x00\x13\x13\x16"),
//             Vec::from("Hello world"),
//         ];
//         let count = 1_000_000;
//         let mut i = 0;
//         while i < count {
//             let (last_part, first_parts) = payload.split_last().unwrap();
//             for part in first_parts.iter() {
//                 to_workers
//                     .send(part, zmq::SNDMORE)
//                     .expect("Couldn't send payload");
//             }
//             to_workers.send(last_part, 0).expect("Couldn't send payload");
//             i += 1;
//         }
//     });

//     // PULL thread
//     let from_workers = ctx.socket(zmq::PULL).unwrap();
//     from_workers.bind(from_workers_url).expect("Couldn't bind the PULL sckt");
//     from_workers.set_rcvhwm(1000000).unwrap();
//     let count = 1_000_000;
//     let mut i = 0;
//     while i < count {
//         from_workers.recv_multipart(0).unwrap();
//         i += 1;
//     }
//     let duration = start.elapsed();
//     info!("Sending and receiving {:?} msgs took {:?}", count, duration);
// }
