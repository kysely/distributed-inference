extern crate bytes;
#[macro_use]
extern crate log;
extern crate rand;
extern crate zmq;

use rand::prelude::*;
use std::time::{Duration, Instant};

pub const PROTOCOL: &[u8] = b"PP01";
pub const HEADER_DELIMITER: u8 = b'\n';
pub const REQUEST: &[u8] = b"\x01";
pub const REPLY: &[u8] = b"\x02";
pub const READY: &[u8] = b"\x03";
pub const PING: &[u8] = b"\x04";
pub const PONG: &[u8] = b"\x05";
pub const DISCONNECT: &[u8] = b"\x06";

pub enum Error {
    InvalidProtocol = 1,
}

pub fn parse_header(header: &Vec<u8>) -> Result<(Vec<u8>, Vec<u8>), Error> {
    let header_parts = header.split(|b| b == &HEADER_DELIMITER);

    let mut cmd: Vec<u8> = Vec::new();
    let mut route: Vec<u8> = Vec::new();

    for (i, part) in header_parts.enumerate() {
        // PROTOCOL | CMD | ?TARGET
        match i {
            0 => {
                if !part.starts_with(PROTOCOL) {
                    return Err(Error::InvalidProtocol);
                }
            }
            1 => cmd = Vec::from(part),
            2 => route = Vec::from(part),
            _ => {}
        }
    }

    return Ok((cmd, route));
}

// CLIENT ======================================================================
pub struct VentilatorClient {
    id: u32,
    id_b: Vec<u8>,
    broker_url: String,
    return_url: String,
    ctx: zmq::Context,
    to_broker: zmq::Socket,
    from_broker: zmq::Socket,
}

impl VentilatorClient {
    pub fn connect(broker_url: &str, return_url: &str) -> Result<Self, zmq::Error> {
        let ctx = zmq::Context::new();
        let t_b = ctx.socket(zmq::PUSH)?;
        let f_b = ctx.socket(zmq::DEALER)?;
        let id = rand::thread_rng().gen_range(1000, 10000);
        let id_s = id.to_string();

        let mut connection = Self {
            id: id,
            id_b: Vec::from(id_s),
            broker_url: broker_url.to_string(),
            return_url: return_url.to_string(),
            ctx: ctx,
            to_broker: t_b,
            from_broker: f_b,
        };

        connection.connect_to_broker()?;
        Ok(connection)
    }

    fn connect_to_broker(&mut self) -> Result<(), zmq::Error> {
        self.from_broker = self.ctx.socket(zmq::DEALER)?;
        self.from_broker.set_identity(self.id.to_string().as_bytes())?;
        self.from_broker.set_linger(0)?;
        self.from_broker.set_rcvhwm(100000)?;
        self.from_broker.connect(&self.return_url)?;
        // self.from_broker.set_subscribe(self.id.to_string().as_bytes())?;
        self.from_broker.send_str("REGISTER", 0)?;
        info!("C{:?} > New RECV socket at {:?}", self.id, self.return_url);

        self.to_broker = self.ctx.socket(zmq::PUSH)?;
        self.to_broker.set_linger(0)?;
        self.to_broker.set_sndhwm(100000)?;
        self.to_broker.connect(&self.broker_url)?;
        info!("C{:?} > New SEND socket at {:?}", self.id, self.broker_url);
        Ok(())
    }

    pub fn send(&self, id: &[u8], request: &str) -> Result<(), zmq::Error> {
        self.to_broker.send(&self.id_b, zmq::SNDMORE)?;
        self.to_broker.send(id, zmq::SNDMORE)?;
        self.to_broker.send(request.as_bytes(), 0)?;
        Ok(())
    }

    pub fn recv(&self) -> Result<Vec<Vec<u8>>, zmq::Error> {
        loop {
            let msg = self.from_broker.recv_multipart(0)?;
            return Ok(msg.to_vec())
        }
    }
}


// impl VentilatorClient {
//     pub fn connect(broker_url: &str) -> Result<Self, zmq::Error> {
//         let ctx = zmq::Context::new();
//         let socket = ctx.socket(zmq::DEALER)?;

//         let mut connection = Self {
//             id: rand::thread_rng().gen_range(1000, 10000),
//             broker_url: broker_url.to_string(),
//             ctx: ctx,
//             socket: socket,
//         };

//         connection.connect_to_broker()?;
//         Ok(connection)
//     }

//     fn connect_to_broker(&mut self) -> Result<(), zmq::Error> {
//         self.socket = self.ctx.socket(zmq::DEALER)?;
//         self.socket.set_identity(self.id.to_string().as_bytes())?;
//         self.socket.set_linger(0)?;
//         self.socket.set_sndhwm(100000)?;
//         self.socket.connect(&self.broker_url)?;
//         info!("C{:?} > New socket to broker at {:?}", self.id, self.broker_url);
//         Ok(())
//     }

//     pub fn send(&self, id: &[u8], request: &str) -> Result<(), zmq::Error> {
//         self.socket.send(id, zmq::SNDMORE)?;
//         self.socket.send(request.as_bytes(), 0)?;
//         Ok(())
//     }

//     pub fn recv(&self) -> Result<Vec<Vec<u8>>, zmq::Error> {
//         loop {
//             let msg = self.socket.recv_multipart(0)?;

//             // let header = &msg[0];
//             // let payload = &msg[1..];

//             // let (cmd, route) = match parse_header(header) {
//             //     Ok(r) => r,
//             //     Err(_) => {
//             //         error!("C{:?} > Invalid protocol {:?}, discarding msg", self.id, header);
//             //         continue; // also disc?
//             //     }
//             // };

//             // debug!("C{:?} > Received CMD {:?} | ROUTE {:?}", self.id, cmd, route);
//             return Ok(msg.to_vec())
//         }
//     }
// }


// WORKER ======================================================================
pub struct Worker {
    id: u32,
    input_url: String,
    output_url: String,
    ctx: zmq::Context,
    input: zmq::Socket,
    output: zmq::Socket,
}

impl Worker {
    pub fn connect(input_url: &str, output_url: &str) -> Result<Self, zmq::Error> {
        let ctx = zmq::Context::new();
        let i_s = ctx.socket(zmq::PULL)?;
        let o_s = ctx.socket(zmq::PUSH)?;

        let mut connection = Self {
            id: rand::thread_rng().gen_range(1000, 10000),
            input_url: input_url.to_string(),
            output_url: output_url.to_string(),
            ctx: ctx,
            input: i_s,
            output: o_s,
        };

        connection.connect_to_broker()?;
        Ok(connection)
    }

    fn connect_to_broker(&mut self) -> Result<(), zmq::Error> {
        self.input = self.ctx.socket(zmq::PULL)?;
        self.input.connect(&self.input_url)?;
        self.input.set_rcvhwm(500000)?;

        self.output = self.ctx.socket(zmq::PUSH)?;
        self.output.connect(&self.output_url)?;
        self.output.set_linger(0)?;
        self.input.set_sndhwm(500000)?;

        info!("W{:?} > New worker sockets | {:?} -> {:?}", self.id, self.input_url, self.output_url);
        Ok(())
    }

    pub fn recv(&mut self) -> Result<Vec<Vec<u8>>, zmq::Error> {
        loop {
            match self.input.poll(zmq::POLLIN, 4000) {
                Err(e) => return Err(e),
                Ok(0) => {
                    warn!("W{:?} > Timeout, reconnecting to broker...", self.id);
                    self.connect_to_broker()?;
                },
                Ok(_) => {
                    let msg = self.input.recv_multipart(0)?;
                    if msg.len() == 1 {
                        debug!("W{:?} > heartbeat", self.id);
                    }
                    // if msg.len() < 3 {
                    //     error!("W{:?} > Invalid message", msg);
                    // }

                    // TODO: unpack c_addr, id, payload?
                    return Ok(msg)
                }
            }
        }
    }

    pub fn recv_nopoll(&mut self) -> Result<Vec<Vec<u8>>, zmq::Error> {
        let msg = self.input.recv_multipart(0)?;
        // TODO: unpack c_addr, id, payload?
        return Ok(msg)
    }

    pub fn reply(&self, payload: Vec<Vec<u8>>) -> Result<(), zmq::Error> {
        let (last_part, first_parts) = payload.split_last().unwrap();

        for part in first_parts.iter() {
            self.output.send(part, zmq::SNDMORE)?;
        }
        self.output.send(last_part, 0)?;

        Ok(())
    }
}

// VENTILATOR / BROKER =========================================================
pub struct Ventilator {
    to_vent_url: String,
    from_vent_url: String,
    from_vent: zmq::Socket,
    to_vent: zmq::Socket,
    _ctx: zmq::Context,
}

impl Ventilator {
    pub fn new(to_vent_url: &str, from_vent_url: &str) -> Result<Self, zmq::Error> {
        let ctx = zmq::Context::new();
        let fv_s = ctx.socket(zmq::PULL)?;
        let tv_s = ctx.socket(zmq::PUSH)?;

        let mut broker = Self {
            from_vent_url: from_vent_url.to_string(),
            to_vent_url: to_vent_url.to_string(),
            from_vent: fv_s,
            to_vent: tv_s,
            _ctx: ctx,
        };

        broker.bind()?;
        Ok(broker)
    }

    fn bind(&mut self) -> Result<(), zmq::Error> {
        self.to_vent = self._ctx.socket(zmq::PUSH)?;
        self.to_vent.bind(&self.to_vent_url)?;
        self.to_vent.set_linger(0)?;
        self.to_vent.set_sndhwm(500000)?;

        self.from_vent = self._ctx.socket(zmq::PULL)?;
        self.from_vent.bind(&self.from_vent_url)?;
        self.to_vent.set_rcvhwm(500000)?;
        info!("Broker active at ___ | VENT -> {:?} | VENT <- {:?}", self.to_vent_url, self.from_vent_url);
        Ok(())
    }

    pub fn send(&self, payload: &Vec<Vec<u8>>) -> Result<(), zmq::Error> {
        let (last_part, first_parts) = payload.split_last().unwrap();

        for part in first_parts.iter() {
            self.to_vent.send(part, zmq::SNDMORE)?;
        }
        self.to_vent.send(last_part, 0)?;

        Ok(())
    }

    pub fn recv(&self) -> Result<Vec<Vec<u8>>, zmq::Error> {
        Ok(self.from_vent.recv_multipart(0)?)
    }

    // -------------------------------------------------------------------------
    pub fn start(&mut self) -> Result<(), zmq::Error> {

        Ok(())
        // loop {
        //     match self.socket.poll(zmq::POLLIN, self.timeout) {
        //         Err(e) => return Err(e),
        //         Ok(0) => { /* timeout */ },
        //         Ok(_) => {
        //             let msg = self.socket.recv_multipart(0)?;

        //             let addr = &msg[0];
        //             let header = &msg[1];
        //             let payload = &msg[2..];

        //             // Parse header
        //             let (cmd, route) = match parse_header(header) {
        //                 Ok(r) => r,
        //                 Err(_) => {
        //                     error!("Invalid protocol {:?}, discarding msg", header);
        //                     continue; // also disc?
        //                 }
        //             };

        //             // Identify message type
        //             if cmd == REQUEST {
        //                 self.handle_request(addr, payload)?;
        //             } else if cmd == REPLY {
        //                 self.handle_reply(addr, &route, payload)?;
        //             } else if cmd == PING {
        //                 self.handle_ping(addr)?;
        //             } else if cmd == PONG {
        //                 self.handle_pong(addr)?;
        //             } else if cmd == READY {
        //                 self.handle_ready(addr)?;
        //             } else if cmd == DISCONNECT {
        //                 self.delete_worker(addr, false)?;
        //             } else {
        //                 error!("Invalid CMD {:?} (ROUTE {:?})", cmd, route);
        //                 // TODO: send disc?
        //             }

        //         }
        //     }
        // }
    }
}
