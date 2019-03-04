extern crate bytes;
#[macro_use]
extern crate log;
extern crate rand;
extern crate zmq;

use rand::prelude::*;
use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};

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
pub struct MajordomoClient {
    broker_url: String,
    ctx: zmq::Context,
    socket: zmq::Socket,
    timeout: i64,
}

impl MajordomoClient {
    pub fn connect(broker_url: &str, timeout: i64) -> Result<Self, zmq::Error> {
        let ctx = zmq::Context::new();
        let socket = ctx.socket(zmq::DEALER)?;

        let mut connection = Self {
            broker_url: broker_url.to_string(),
            ctx: ctx,
            socket: socket,
            timeout: timeout
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

    pub fn send(&self, service: &str, id: &[u8], request: &str) -> Result<(), zmq::Error> {
        debug!("Sending msg id {:?}", id);
        
        self.socket.send(&[PROTOCOL, REQUEST, service.as_bytes()].join(&HEADER_DELIMITER), zmq::SNDMORE)?;
        self.socket.send(id, zmq::SNDMORE)?;
        self.socket.send(request.as_bytes(), 0)?;
        
        Ok(())
    }

    pub fn recv(&self) -> Result<Vec<Vec<u8>>, zmq::Error> {
        loop {
            let msg = self.socket.recv_multipart(0)?;

            let header = &msg[0];
            let payload = &msg[1..];

            let (cmd, route) = match parse_header(header) {
                Ok(r) => r,
                Err(_) => {
                    error!("Invalid protocol {:?}, discarding msg", header);
                    continue; // also disc?
                }
            };

            debug!("Received CMD {:?} | ROUTE {:?}", cmd, route);
            return Ok(payload.to_vec())
        }
    }
}


// WORKER ======================================================================
pub struct MajordomoWorker {
    id: u32,
    broker_url: String,
    service_name: String,
    ctx: zmq::Context,
    socket: zmq::Socket,
    timeout: i64,
    expect_pong: bool,
    connects: u64,
}

impl MajordomoWorker {
    pub fn connect(broker_url: &str, service_name: &str, timeout: i64) -> Result<Self, zmq::Error> {
        let ctx = zmq::Context::new();
        let socket = ctx.socket(zmq::DEALER)?;

        let mut connection = Self {
            id: rand::thread_rng().gen_range(1000, 10000),
            broker_url: broker_url.to_string(),
            service_name: service_name.to_string(),
            ctx: ctx,
            socket: socket,
            timeout: timeout,
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
        info!("{:?} > New socket to broker at {:?}", self.id, self.broker_url);
        self.send_ready()?;
        Ok(())
    }

    pub fn recv(&mut self) -> Result<(Vec<u8>, Vec<u8>, Vec<Vec<u8>>), zmq::Error> {
        loop {
            match self.socket.poll(zmq::POLLIN, self.timeout) {
                Err(e) => return Err(e),
                Ok(0) => {
                    debug!("{:?} > Timeout {:?}ms", self.id, self.timeout);
                    if !self.expect_pong {
                        self.send_ping()?;
                    } else {
                        warn!("{:?} > No PONG reply within timeout, reconnecting...", self.id);
                        self.connect_to_broker()?;
                    }
                }
                Ok(_) => {
                    // Incoming events on socket
                    self.expect_pong = false;

                    let msg = self.socket.recv_multipart(0)?;
                    // debug!("Received {:?}", msg);

                    let header = &msg[0];
                    let payload = &msg[1..];

                    // Parse header
                    let (cmd, route) = match parse_header(header) {
                        Ok(r) => r,
                        Err(_) => {
                            error!("{:?} > Invalid protocol {:?}, discarding msg", self.id, header);
                            continue; // also disc?
                        }
                    };

                    // Identify message type
                    if cmd == REQUEST {
                        if payload.len() < 2 || route.is_empty() {
                            error!("{:?} > Invalid request, payload len {:?}, route {:?}", self.id, payload.len(), route);
                            continue;
                        }
                        debug!("{:?} > Received REQUEST", self.id);
                        debug!("{:?} > Received request id {:?} from {:?}", self.id, route, payload[0]);
                        // return to API
                        return Ok((route.to_vec(), payload[0].to_vec(), payload[1..].to_vec()))
                    } else if cmd == PING {
                        debug!("{:?} > Received PING", self.id);
                        self.send_pong()?;
                    } else if cmd == PONG {
                        // ignore?
                    } else if cmd == DISCONNECT {
                        warn!("{:?} > Received DISCONNECT from broker", self.id);
                        self.connect_to_broker()?;
                    } else {
                        error!("{:?} > Invalid CMD {:?} (TARGET {:?})", self.id, cmd, route);
                        // TODO: send disc?
                    }

                }
            }
        }
    }

    // SOCKET FUNCTIONS --------------------------------------------------------
    pub fn reply(&self, route: Vec<u8>, id: Vec<u8>, payload: Vec<Vec<u8>>) -> Result<(), zmq::Error> {
        self.socket.send(&[PROTOCOL, REPLY, &route].join(&HEADER_DELIMITER), zmq::SNDMORE)?;
        self.socket.send(&id, zmq::SNDMORE)?;

        let (last_part, first_parts) = payload.split_last().unwrap();

        for part in first_parts.iter() {
            self.socket.send(part, zmq::SNDMORE)?;
        }
        self.socket.send(last_part, 0)?;

        Ok(())
    }

    fn send_control_msg(&self, header: &[u8]) -> Result<(), zmq::Error> {
        self.socket.send(header, 0)?;
        Ok(())
    }

    fn send_ready(&self) -> Result<(), zmq::Error> {
        debug!("{:?} > Sending READY to broker", self.id);
        self.send_control_msg(
            &[PROTOCOL, READY, self.service_name.as_bytes()].join(&HEADER_DELIMITER),
        )?;
        Ok(())
    }

    fn send_ping(&mut self) -> Result<(), zmq::Error> {
        debug!("{:?} > Sending PING to broker", self.id);
        self.send_control_msg(&[PROTOCOL, PING].join(&HEADER_DELIMITER))?;
        self.expect_pong = true;
        Ok(())
    }

    fn send_pong(&self) -> Result<(), zmq::Error> {
        debug!("{:?} > Sending PONG to broker", self.id);
        self.send_control_msg(&[PROTOCOL, PONG].join(&HEADER_DELIMITER))?;
        Ok(())
    }

}

// BROKER ======================================================================
#[derive(Debug)]
pub struct Worker {
    service_name: Vec<u8>,
    last_activity: Instant,
    expect_pong: bool,
    pong_by: Instant,
}

#[derive(Debug)]
pub struct Service {
    workers: VecDeque<Vec<u8>>,
}

pub struct MajordomoBroker {
    url: String,
    timeout: i64,
    timeout_d: Duration,
    socket: zmq::Socket,
    _ctx: zmq::Context,
    services: HashMap<Vec<u8>, Service>,
    workers: HashMap<Vec<u8>, Worker>,
}

impl MajordomoBroker {
    pub fn new(url: &str, timeout: u64) -> Result<Self, zmq::Error> {
        let ctx = zmq::Context::new();
        let socket = ctx.socket(zmq::ROUTER)?;

        let broker = Self {
            url: url.to_string(),
            timeout: timeout as i64,
            timeout_d: Duration::from_millis(timeout),
            socket: socket,
            _ctx: ctx,
            services: HashMap::new(),
            workers: HashMap::new(),
        };

        broker.bind()?;
        Ok(broker)
    }

    fn bind(&self) -> Result<(), zmq::Error> {
        self.socket.set_linger(0)?;
        self.socket.bind(&self.url)?;
        self.socket.set_rcvhwm(100000)?;
        self.socket.set_sndhwm(100000)?;
        info!("Broker active at {:?}", &self.url);
        Ok(())
    }

    // -------------------------------------------------------------------------
    pub fn start(&mut self) -> Result<(), zmq::Error> {
        loop {
            match self.socket.poll(zmq::POLLIN, self.timeout) {
                Err(e) => return Err(e),
                Ok(0) => { /* timeout */ },
                Ok(_) => {
                    let msg = self.socket.recv_multipart(0)?;

                    let addr = &msg[0];
                    let header = &msg[1];
                    let payload = &msg[2..];

                    // Parse header
                    let (cmd, route) = match parse_header(header) {
                        Ok(r) => r,
                        Err(_) => {
                            error!("Invalid protocol {:?}, discarding msg", header);
                            continue; // also disc?
                        }
                    };

                    // Identify message type
                    if cmd == REQUEST {
                        self.handle_request(addr, &route, payload)?;
                    } else if cmd == REPLY {
                        self.handle_reply(addr, &route, payload)?;
                    } else if cmd == PING {
                        self.handle_ping(addr)?;
                    } else if cmd == PONG {
                        self.handle_pong(addr)?;
                    } else if cmd == READY {
                        self.handle_ready(addr, &route)?;
                    } else if cmd == DISCONNECT {
                        self.delete_worker(addr, false)?;
                    } else {
                        error!("Invalid CMD {:?} (ROUTE {:?})", cmd, route);
                        // TODO: send disc?
                    }

                }
            }

            self.purge_workers();
        }
    }

    // INTERNAL STATE FXs ------------------------------------------------------
    fn delete_worker(&mut self, w_addr: &Vec<u8>, send_dcnt: bool) -> Result<(), zmq::Error> {
        if send_dcnt {
            self.send_disconnect(w_addr)?;
        }

        // Remove worker with matching address
        match self.workers.remove(w_addr) {
            Some(_) => {
                debug!("Deleted worker {:?}", w_addr);
            }
            None => {
                warn!("Weird, couldn't delete worker {:?} from workers", w_addr);
            }
        }

        // will be Some(s_name) if service should be deleted later
        let mut delete_service: Option<Vec<u8>> = None;

        // Look for w_addr in some of the services
        for (svc_name, service) in self.services.iter_mut() {
            if service.workers.contains(w_addr) {
                // find & delete `w_addr` from service.workers
                let to_del = service
                    .workers
                    .iter()
                    .enumerate()
                    .find(|w| w.1 == w_addr)
                    .unwrap();

                match service.workers.remove(to_del.0) {
                    Some(_) => {}
                    None => warn!("Weird, couldn't delete worker {:?} from service", w_addr),
                }

                // set `delete_service` to Some if no more workers
                if service.workers.is_empty() {
                    warn!("Service {:?} has no more workers, removing...", svc_name);
                    delete_service = Some(svc_name.to_vec());
                }
            }
        }

        match delete_service {
            Some(svc_name) => {
                self.services.remove(&svc_name).unwrap();
            }
            None => {}
        }
        Ok(())
    }

    fn purge_workers(&mut self) {
        let now = Instant::now();
        let mut workers_to_ping: VecDeque<Vec<u8>> = VecDeque::new();
        let mut workers_to_delete: VecDeque<Vec<u8>> = VecDeque::new();

        for (addr, worker) in self.workers.iter_mut() {
            if now.duration_since(worker.last_activity) > self.timeout_d {
                if worker.expect_pong == true && worker.pong_by < now {
                    workers_to_delete.push_back(addr.to_vec());
                } else {
                    worker.expect_pong = true;
                    worker.pong_by = now + self.timeout_d;
                    workers_to_ping.push_back(addr.to_vec());
                }
            }
        }

        for addr in workers_to_ping {
            self.send_ping(&addr).unwrap();
        }

        for addr in workers_to_delete {
            debug!("Deleting expired worker {:?}", addr);
            self.delete_worker(&addr, false).unwrap(); // safe unwrap due to `false`
        }
    }

    fn get_next_worker(&mut self, service_name: &Vec<u8>) -> Option<Vec<u8>> {
        self.purge_workers();

        // None if no service or worker present
        match self.services.get_mut(service_name) {
            Some(svc) => {
                if svc.workers.is_empty() {
                    return None;
                }

                // rotate first -> last
                let next_w = svc.workers.pop_front().unwrap();
                svc.workers.push_back(next_w);
                return Some(svc.workers.back().unwrap().to_vec());
            }
            None => return None,
        }
    }

    fn worker_activity(&mut self, w_addr: &Vec<u8>) {
        match self.workers.get_mut(w_addr) {
            Some(worker) => {
                worker.last_activity = Instant::now();
                worker.expect_pong = false;
            }
            None => warn!(
                "Weird, can't update activity for wrkr {:?}, unknwon",
                w_addr
            ),
        }
    }

    // COMMAND HANDLERS --------------------------------------------------------
    fn handle_request(
        &mut self,
        addr: &Vec<u8>,
        route: &Vec<u8>,
        payload: &[Vec<u8>],
    ) -> Result<(), zmq::Error> {
        if payload.len() < 2 || route.is_empty() { /* TODO: invalid req */ }

        match self.get_next_worker(route) {
            Some(w_addr) => {
                let header = &[PROTOCOL, REQUEST, addr].join(&HEADER_DELIMITER);
                debug!("Routing request header {:?}", header);
                self.send_data_msg(&w_addr, header, payload)?;
            }
            None => warn!("No service or worker found for {:?}", route),
        }
        Ok(())
    }

    fn handle_reply(
        &mut self,
        addr: &Vec<u8>,
        route: &Vec<u8>,
        payload: &[Vec<u8>],
    ) -> Result<(), zmq::Error> {
        if !self.workers.contains_key(addr) {
            self.send_disconnect(addr)?;
            return Ok(());
        }
        if payload.len() < 2 || route.is_empty() { /* TODO: invalid rep */ }

        let svc_name = &self.workers.get(addr).unwrap().service_name;

        let header = &[PROTOCOL, REPLY, svc_name].join(&HEADER_DELIMITER);

        self.send_data_msg(route, header, payload)?;
        self.worker_activity(addr);
        Ok(())
    }

    fn handle_ping(&mut self, addr: &Vec<u8>) -> Result<(), zmq::Error> {
        if !self.workers.contains_key(addr) {
            self.send_disconnect(addr)?;
            return Ok(());
        }
        self.send_pong(&addr)?;
        self.worker_activity(addr);
        Ok(())
    }

    fn handle_pong(&mut self, addr: &Vec<u8>) -> Result<(), zmq::Error> {
        if !self.workers.contains_key(addr) {
            self.send_disconnect(addr)?;
            return Ok(());
        }
        self.worker_activity(addr);
        Ok(())
    }

    fn handle_ready(&mut self, addr: &Vec<u8>, route: &Vec<u8>) -> Result<(), zmq::Error> {
        // READY
        // - if reserved service name, disconnect
        // + set worker as waiting
        if route.is_empty() { /* invalid ready */ }

        if self.workers.contains_key(addr) {
            self.delete_worker(addr, true)?;
            return Ok(());
        }

        info!(
            "Registering new worker at {:?} for service {:?}",
            addr, route
        );
        self.workers.insert(
            addr.to_owned(),
            Worker {
                service_name: route.to_owned(),
                last_activity: Instant::now(),
                expect_pong: false,
                pong_by: Instant::now() + self.timeout_d,
            },
        );

        if !self.services.contains_key(route) {
            // Create new Service if none exists yet
            info!("Creating new service {:?}", route);
            self.services.insert(
                route.to_owned(),
                Service {
                    workers: VecDeque::new(),
                },
            );
        }

        // Attach worker address to service
        self.services
            .get_mut(route)
            .unwrap() // safe unwrap due to logic above
            .workers.push_back(addr.to_vec()); // TODO: push back or front? (resolve waiting)
        
        info!("Workers on service {:?}: {:?}", route, self.services.get_mut(route).unwrap());

        self.send_pong(&addr)?; // TODO: ????
        Ok(())
    }

    // SOCKET FUNCTIONS --------------------------------------------------------
    fn send_data_msg(
        &self,
        addr: &Vec<u8>,
        header: &[u8],
        payload: &[Vec<u8>],
    ) -> Result<(), zmq::Error> {
        self.socket.send(addr, zmq::SNDMORE)?;
        self.socket.send(header, zmq::SNDMORE)?;

        let (last_part, first_parts) = payload.split_last().unwrap();

        for part in first_parts.iter() {
            self.socket.send(part, zmq::SNDMORE)?;
        }
        self.socket.send(last_part, 0)?;

        Ok(())
    }

    fn send_control_msg(&self, addr: &Vec<u8>, header: &[u8]) -> Result<(), zmq::Error> {
        self.socket.send(addr, zmq::SNDMORE)?;
        self.socket.send(header, 0)?;
        Ok(())
    }

    fn send_disconnect(&self, addr: &Vec<u8>) -> Result<(), zmq::Error> {
        debug!("Sending DISCONNECT to {:?}", addr);
        self.send_control_msg(addr, &[PROTOCOL, DISCONNECT].join(&HEADER_DELIMITER))?;
        Ok(())
    }

    fn send_ping(&self, addr: &Vec<u8>) -> Result<(), zmq::Error> {
        debug!("Sending PING to {:?}", addr);
        self.send_control_msg(addr, &[PROTOCOL, PING].join(&HEADER_DELIMITER))?;
        Ok(())
    }

    fn send_pong(&self, addr: &Vec<u8>) -> Result<(), zmq::Error> {
        debug!("Sending PONG to {:?}", addr);
        self.send_control_msg(addr, &[PROTOCOL, PONG].join(&HEADER_DELIMITER))?;
        Ok(())
    }
}
