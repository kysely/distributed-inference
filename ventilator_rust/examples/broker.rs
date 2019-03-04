extern crate pretty_env_logger;
extern crate zmq;
#[macro_use]
extern crate log;

use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

fn main() {
    pretty_env_logger::init_timed();

    let ctx = zmq::Context::new();
    // let ctx_thread = ctx.clone();
    let listen_url = "tcp://*:5554";
    let to_workers_url = "tcp://*:5555";
    let from_workers_url = "tcp://*:5556";
    let return_url = "tcp://*:5557";

    // PULL
    let from_workers = ctx.socket(zmq::PULL).unwrap();
    from_workers.set_rcvhwm(1000000).unwrap();
    from_workers.bind(from_workers_url).unwrap();

    // ROUTER to client
    let router = ctx.socket(zmq::ROUTER).unwrap();
    router.set_linger(0).unwrap();
    router.set_sndhwm(1000000).unwrap();
    router.bind(return_url).unwrap();

    // THREAD: client -> workers
    thread::spawn(move || {
        let ctx_thread = zmq::Context::new();
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
        let pull = ctx_thread.socket(zmq::PULL).unwrap();
        pull.set_rcvhwm(1000000).unwrap();
        pull.bind(listen_url).unwrap();

        // PUSH
        let to_workers = ctx_thread.socket(zmq::PUSH).unwrap();
        to_workers.set_linger(0).unwrap();
        to_workers.set_sndhwm(1000000).unwrap();
        to_workers.bind(to_workers_url).unwrap();

        loop {
            loop {
                let payload = pull.recv_msg(0).expect("Couldn't PULL from client");

                if pull.get_rcvmore().unwrap() {
                    to_workers.send_msg(payload, zmq::SNDMORE).expect("Could't send payload");
                } else {
                    to_workers.send_msg(payload, 0).expect("Could't send payload");
                    break;
                }
            }

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
        loop {
            let payload = from_workers.recv_msg(0).expect("Couldn't PULL from workers");

            if from_workers.get_rcvmore().unwrap() {
                // println!("{:?}", &payload);
                router.send_msg(payload, zmq::SNDMORE).expect("Could't send payload");
            } else {
                // println!("{:?}", &payload);
                router.send_msg(payload, 0).expect("Could't send payload");
                // println!("------------------");
                break;
            }
        }
        *cmsgs_count.lock().unwrap() += 1;
    }
}
// fn main() {
//     pretty_env_logger::init_timed();

//     let ctx = zmq::Context::new();
//     let ctx_thread = ctx.clone();
//     let listen_url = "tcp://*:5554";
//     let to_workers_url = "tcp://*:5555";
//     let from_workers_url = "tcp://*:5556";
//     let return_url = "tcp://*:5557";

//     // PULL
//     let from_workers = ctx.socket(zmq::PULL).unwrap();
//     from_workers.bind(from_workers_url).unwrap();
//     from_workers.set_rcvhwm(1000000).unwrap();

//     // ROUTER to client
//     let router = ctx.socket(zmq::ROUTER).unwrap();
//     router.bind(return_url).unwrap();
//     router.set_linger(0).unwrap();
//     router.set_sndhwm(1000000).unwrap();

//     // THREAD: client -> workers
//     thread::spawn(move || {
//         let wmsgs_count = Arc::new(Mutex::new(0));
//         let wmsgs_count_thread = wmsgs_count.clone();
//         // Monitoring thread
//         thread::spawn(move || loop {
//             thread::sleep(Duration::new(1, 0));
//             let mut count = wmsgs_count_thread.lock().unwrap();
//             info!("-> W {:?} msgs", *count);
//             *count = 0;
//         });

//         // PULL from client
//         let pull = ctx_thread.socket(zmq::PULL).unwrap();
//         pull.bind(listen_url).unwrap();
//         pull.set_rcvhwm(1000000).unwrap();

//         // PUSH
//         let to_workers = ctx_thread.socket(zmq::PUSH).unwrap();
//         to_workers.bind(to_workers_url).unwrap();
//         to_workers.set_linger(0).unwrap();
//         to_workers.set_sndhwm(1000000).unwrap();

//         loop {
//             let payload = pull.recv_multipart(0).expect("Couldn't PULL from client");

//             let (last_part, first_parts) = payload.split_last().unwrap();
//             for part in first_parts.iter() {
//                 to_workers
//                     .send(part, zmq::SNDMORE)
//                     .expect("Couldn't send payload");
//             }
//             to_workers
//                 .send(last_part, 0)
//                 .expect("Couldn't send payload");
//             *wmsgs_count.lock().unwrap() += 1;
//         }
//     });

//     // THREAD: workers -> clients
//     let cmsgs_count = Arc::new(Mutex::new(0));
//     let cmsgs_count_thread = cmsgs_count.clone();
//     // Monitoring thread
//     thread::spawn(move || loop {
//         thread::sleep(Duration::new(1, 0));
//         let mut count = cmsgs_count_thread.lock().unwrap();
//         info!("-> C {:?} msgs", *count);
//         *count = 0;
//     });

//     loop {
//         let payload = from_workers
//             .recv_multipart(0)
//             .expect("Couldn't PULL from workers");

//         let (last_part, first_parts) = payload.split_last().unwrap();
//         for part in first_parts.iter() {
//             router
//                 .send(part, zmq::SNDMORE)
//                 .expect("Couldn't send payload");
//         }
//         router.send(last_part, 0).expect("Couldn't send payload");
//         *cmsgs_count.lock().unwrap() += 1;
//     }
// }
