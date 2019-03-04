extern crate bytes;
#[macro_use]
extern crate log;
extern crate rand;
extern crate zmq;

use rand::prelude::*;

pub const PROTOCOL: &[u8] = b"MD01";
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
pub struct RDClient {
    broker_url: String,
    ctx: zmq::Context,
    socket: zmq::Socket,
}

impl RDClient {
    pub fn connect(broker_url: &str) -> Result<Self, zmq::Error> {
        let ctx = zmq::Context::new();
        let socket = ctx.socket(zmq::DEALER)?;

        let mut connection = Self {
            broker_url: broker_url.to_string(),
            ctx: ctx,
            socket: socket,
        };

        connection.connect_to_broker()?;
        Ok(connection)
    }

    fn connect_to_broker(&mut self) -> Result<(), zmq::Error> {
        self.socket = self.ctx.socket(zmq::DEALER)?;
        self.socket.set_linger(0)?;
        self.socket.set_sndhwm(100000)?;
        self.socket.connect(&self.broker_url)?;
        info!("New socket to broker at {:?}", self.broker_url);
        Ok(())
    }

    pub fn send(&self, id: &[u8], request: &str) -> Result<(), zmq::Error> {
        debug!("Sending msg id {:?}", id);
        self.socket.send(id, zmq::SNDMORE)?;
        self.socket.send(request.as_bytes(), 0)?;
        Ok(())
    }

    pub fn recv(&self) -> Result<Vec<Vec<u8>>, zmq::Error> {
        let msg = self.socket.recv_multipart(0)?;
        return Ok(msg)
    }
}


// WORKER ======================================================================
pub struct RDWorker {
    id: u32,
    broker_url: String,
    ctx: zmq::Context,
    socket: zmq::Socket,
    expect_pong: bool,
    connects: u64,
}

impl RDWorker {
    pub fn connect(broker_url: &str) -> Result<Self, zmq::Error> {
        let ctx = zmq::Context::new();
        let socket = ctx.socket(zmq::DEALER)?;

        let mut connection = Self {
            id: rand::thread_rng().gen_range(1000, 10000),
            broker_url: broker_url.to_string(),
            ctx: ctx,
            socket: socket,
            expect_pong: false,
            connects: 0,
        };

        connection.connect_to_broker()?;
        Ok(connection)
    }

    fn connect_to_broker(&mut self) -> Result<(), zmq::Error> {
        self.socket = self.ctx.socket(zmq::DEALER)?;
        self.socket.set_linger(0)?;
        self.socket.set_sndhwm(100000)?;
        self.socket.connect(&self.broker_url)?;

        self.connects += 1;
        self.expect_pong = false;
        info!("[W {:?}] New socket to broker at {:?}", self.id, self.broker_url);
        self.send_ready()?;
        Ok(())
    }

    pub fn recv(&mut self) -> Result<Vec<Vec<u8>>, zmq::Error> {
        let msg = self.socket.recv_multipart(0)?;

        if msg.len() == 1 {
            // control message
            let payload = &msg[0];

            if payload == &PING {
                self.send_pong()?;
            } else if payload == &PONG {
                //
            }

            return Ok(self.recv()?);
        }

        Ok(msg)
    }

    // SOCKET FUNCTIONS --------------------------------------------------------
    pub fn reply(&self, payload: Vec<Vec<u8>>) -> Result<(), zmq::Error> {
        let (last_part, first_parts) = payload.split_last().unwrap();

        for part in first_parts.iter() {
            self.socket.send(part, zmq::SNDMORE)?;
        }
        self.socket.send(last_part, 0)?;

        Ok(())
    }

    fn send_pong(&self) -> Result<(), zmq::Error> {
        debug!("[W {:?}] Sending PONG", self.id);
        self.socket.send(PONG, 0)?;
        Ok(())
    }

    fn send_ready(&self) -> Result<(), zmq::Error> {
        debug!("[W {:?}] Sending READY", self.id);
        self.socket.send(READY, 0)?;
        Ok(())
    }
}

// BROKER ======================================================================
pub struct RDBroker {
    frontend_url: String,
    backend_url: String,
    frontend: zmq::Socket,
    backend: zmq::Socket,
    _ctx: zmq::Context,
}

impl RDBroker {
    pub fn new(frontend_url: &str, backend_url: &str) -> Result<Self, zmq::Error> {
        let ctx = zmq::Context::new();
        let f = ctx.socket(zmq::ROUTER)?;
        let b = ctx.socket(zmq::DEALER)?;

        let broker = Self {
            frontend_url: frontend_url.to_string(),
            backend_url: backend_url.to_string(),
            frontend: f,
            backend: b,
            _ctx: ctx,
        };

        broker.bind()?;
        Ok(broker)
    }

    fn bind(&self) -> Result<(), zmq::Error> {
        self.frontend.set_linger(0)?;
        self.frontend.set_rcvhwm(1000000)?;
        self.frontend.set_sndhwm(1000000)?;
        self.frontend.bind(&self.frontend_url)?;

        self.backend.set_linger(0)?;
        self.backend.set_rcvhwm(1000000)?;
        self.backend.set_sndhwm(1000000)?;
        self.backend.bind(&self.backend_url)?;
        info!("Broker active | {:?} -> {:?}", &self.frontend_url, &self.backend_url);
        Ok(())
    }

    // -------------------------------------------------------------------------
    pub fn start(&mut self) -> Result<(), zmq::Error> {
        loop {
            let mut items = [
                self.frontend.as_poll_item(zmq::POLLIN),
                self.backend.as_poll_item(zmq::POLLIN),
            ];

            match zmq::poll(&mut items, -1) {
                Err(e) => return Err(e),
                Ok(0) => { /* timeout (unreachable due to -1) */ },
                Ok(_) => {
                    if items[0].is_readable() {
                        // msg on frontend
                        let msg = self.frontend.recv_multipart(0)?;
                        self.send_to_workers(msg)?;
                    } else {
                        // msg on backend
                        let msg = self.backend.recv_multipart(0)?;
                        if msg.len() == 1 {
                            // control msg
                            continue;
                        }
                        self.send_to_clients(msg)?;
                    }
                }
            }
        }
    }

    fn send_to_clients(&self, payload: Vec<Vec<u8>>) -> Result<(), zmq::Error> {
        let (last_part, first_parts) = payload.split_last().unwrap();

        for part in first_parts.iter() {
            self.frontend.send(part, zmq::SNDMORE)?;
        }
        self.frontend.send(last_part, 0)?;

        Ok(())
    }

    fn send_to_workers(&self, payload: Vec<Vec<u8>>) -> Result<(), zmq::Error> {
        let (last_part, first_parts) = payload.split_last().unwrap();

        for part in first_parts.iter() {
            self.backend.send(part, zmq::SNDMORE)?;
        }
        self.backend.send(last_part, 0)?;

        Ok(())
    }
}
