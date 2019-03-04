extern crate bytes;
#[macro_use]
extern crate log;
extern crate rand;
extern crate zmq;

use rand::prelude::*;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

// CLIENT ======================================================================
pub struct VentilatorClient {
    id: u32,
    id_b: Vec<u8>,
    to_broker_url: String,
    from_broker_url: String,
    ctx: zmq::Context,
    to_broker: zmq::Socket,
    from_broker: zmq::Socket,
}

impl VentilatorClient {
    pub fn connect(to_broker_url: &str, from_broker_url: &str) -> Result<Self, zmq::Error> {
        let ctx = zmq::Context::new();
        let t_b = ctx.socket(zmq::PUSH)?;
        let f_b = ctx.socket(zmq::DEALER)?;
        let id = rand::thread_rng().gen_range(1000, 10000);
        let id_s = id.to_string();

        let mut connection = Self {
            id: id,
            id_b: Vec::from(id_s),
            to_broker_url: to_broker_url.to_string(),
            from_broker_url: from_broker_url.to_string(),
            ctx: ctx,
            to_broker: t_b,
            from_broker: f_b,
        };

        connection.connect_to_broker()?;
        Ok(connection)
    }

    fn connect_to_broker(&mut self) -> Result<(), zmq::Error> {
        self.from_broker = self.ctx.socket(zmq::DEALER)?;
        self.from_broker
            .set_identity(self.id.to_string().as_bytes())?;
        self.from_broker.set_linger(0)?;
        self.from_broker.set_rcvhwm(100000)?;
        self.from_broker.connect(&self.from_broker_url)?;
        self.from_broker.send_str("HI", 0)?; // send msg to broker so that ROUTER knows our identity
        info!(
            "C{:?} > New RECV socket at {:?}",
            self.id, self.from_broker_url
        );

        self.to_broker = self.ctx.socket(zmq::PUSH)?;
        self.to_broker.set_linger(0)?;
        self.to_broker.set_sndhwm(100000)?;
        self.to_broker.connect(&self.to_broker_url)?;
        info!(
            "C{:?} > New SEND socket at {:?}",
            self.id, self.to_broker_url
        );
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
            return Ok(msg.to_vec());
        }
    }
}

// WORKER ======================================================================
pub struct Worker {
    pub id: u32,
    input_url: String,
    output_url: String,
    ctx: zmq::Context,
    input: zmq::Socket,
    output: zmq::Socket,
    pub msg_count: Arc<Mutex<i64>>,
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
            msg_count: Arc::new(Mutex::new(0)),
        };

        connection.connect_to_broker()?;
        Ok(connection)
    }

    pub fn monitor(counter: Arc<Mutex<i64>>, id: u32) {
        thread::spawn(move || loop {
            thread::sleep(Duration::from_secs(1));
            let mut count = counter.lock().unwrap();
            info!("[W {:?}] Current throughput {:?} msgs", id, *count);
            *count = 0;
        });
    }

    pub fn inc(&self) {
        *self.msg_count.lock().unwrap() += 1;
    }

    fn connect_to_broker(&mut self) -> Result<(), zmq::Error> {
        self.input = self.ctx.socket(zmq::PULL)?;
        self.input.connect(&self.input_url)?;
        self.input.set_rcvhwm(1000000)?;

        self.output = self.ctx.socket(zmq::PUSH)?;
        self.output.connect(&self.output_url)?;
        self.output.set_linger(0)?;
        self.input.set_sndhwm(1000000)?;

        info!(
            "[W {:?}] New worker sockets | {:?} -> {:?}",
            self.id, self.input_url, self.output_url
        );
        Ok(())
    }

    pub fn recv(&mut self) -> Result<Vec<Vec<u8>>, zmq::Error> {
        let payload = self.input.recv_multipart(0)?;
        debug!("[W {:?}] New payload: {:?}", self.id, payload);
        Ok(payload)
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
