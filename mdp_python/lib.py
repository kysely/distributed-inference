from binascii import hexlify
import logwood
from logwood.handlers.stderr import ColoredStderrHandler
from random import randint
import time
import zmq

# ------------------------------------------------------------------------------
C_CLIENT = b'MDPC01'
W_WORKER = b'MDPW01'
B_BROKER = b'MDPB01'

#  MDP/Server commands, as strings
W_READY      = b'\001'
W_REQUEST    = b'\002'
W_REPLY      = b'\003'
W_HEARTBEAT  = b'\004'
W_DISCONNECT = b'\005'

class BaseComponent:
    def __init__(self, verbose=False):
        LOG_FORMAT = '{timestamp} [{name}] [{level}] {message}'

        logwood.basic_config(
            level = logwood.DEBUG if verbose else logwood.INFO,
            handlers = [ColoredStderrHandler(format=LOG_FORMAT)]
        )

        self.id = '%04X-%04X' % (randint(0, 0x10000), randint(0, 0x10000))
        self.log = logwood.get_logger(f'{self.__class__.__name__} {self.id}')

# CLIENT -----------------------------------------------------------------------
class MajordomoClient(BaseComponent):

    RETRIES = 3
    TIMEOUT = 2.5

    def __init__(self, broker, verbose=False):
        super().__init__(verbose)
        self.broker = broker
        self.ctx = zmq.Context()
        self.poller = zmq.Poller()

        self.client: zmq.Socket = None

        self.reconnect_to_broker()

    def reconnect_to_broker(self):
        """Connect or reconnect to broker"""
        if self.client:
            self.poller.unregister(self.client)
            self.client.close()
        
        self.client = self.ctx.socket(zmq.REQ)
        self.client.linger = 0
        self.client.connect(self.broker)
        self.poller.register(self.client, zmq.POLLIN)

        self.log.info(f'Connecting to broker at {self.broker}...')
    
    def send(self, service, request):
        """Send request to broker and get reply by hook or crook.

        Takes ownership of request message and destroys it when sent.
        Returns the reply message or None if there was no reply.
        """
        if not isinstance(request, list):
            request = [request if isinstance(request, bytes) else request.encode()]
        if not isinstance(service, bytes):
            service = service.encode()
        
        request = [C_CLIENT, service] + request

        self.log.debug(f'Send request to {service}: {request}')

        reply = None
        retries = self.RETRIES

        while retries > 0:
            self.client.send_multipart(request)
            try:
                items = self.poller.poll(self.TIMEOUT*1e3)
            except KeyboardInterrupt:
                break
            
            if items:
                msg = self.client.recv_multipart()
                self.log.debug(f'Received reply{msg}')
                
                assert len(msg) >= 3

                header = msg.pop(0)
                assert C_CLIENT == header

                reply_service = msg.pop(0)
                assert service == reply_service

                reply = msg
                break
            else:
                if retries:
                    self.log.warning('No reply, reconnecting...')
                    self.reconnect_to_broker()
                else:
                    self.log.warning('Permanent error, abandoning')
                    break
                retries -= 1
        
        return reply

    def destroy(self):
        self.ctx.destroy()


# WORKER -----------------------------------------------------------------------
class MajordomoWorker(BaseComponent):

    HB_LIVENESS = 3
    HB_INTERVAL = 2.5

    TIMEOUT = 2.5
    RECONNECT = 2.5

    def __init__(self, broker, service, verbose=False):
        super().__init__(verbose)
        self.broker = broker
        self.service = service
        self.ctx = zmq.Context()
        self.poller = zmq.Poller()

        self.worker: zmq.Socket = None

        self.hb_at: int = 0
        self.liveness: int = 0
        self.expect_reply = False # False only at start
        self.reply_to = None # Return address, if any

        self.reconnect_to_broker()
    
    def reconnect_to_broker(self):
        """Connect or reconnect to broker"""
        if self.worker:
            self.poller.unregister(self.worker)
            self.worker.close()
        
        self.worker = self.ctx.socket(zmq.DEALER)
        self.worker.linger = 0
        self.worker.connect(self.broker)
        self.poller.register(self.worker, zmq.POLLIN)

        self.log.info(f'Connecting to broker at {self.broker}...')
    
        # Register service with broker
        self.send_to_broker(W_READY, self.service, [])
        self.liveness = self.HB_LIVENESS
        self.hb_at = time.time() + self.HB_INTERVAL
    
    def send_to_broker(self, command, option=None, msg=None):
        """Send message to broker.
        If no msg is provided, creates one internally
        """
        if not isinstance(command, bytes):
            command = command.encode()

        if msg is None:
            msg = []
        elif not isinstance(msg, list):
            msg = [msg if isinstance(msg, bytes) else msg.encode()]
        
        if option:
            msg = [option if isinstance(option, bytes) else option.encode()] + msg
        
        msg = [b'', W_WORKER, command] + msg

        self.log.debug(f'Sending {command} to broker: {msg}')

        self.worker.send_multipart(msg)

    def recv(self, reply=None):
        assert reply is not None or not self.expect_reply

        if reply is not None:
            assert self.reply_to is not None
            reply = [self.reply_to, b''] + reply
            self.send_to_broker(W_REPLY, msg=reply)
        
        self.expect_reply = True
        while True:
            try:
                items = self.poller.poll(self.TIMEOUT*1e3)
            except KeyboardInterrupt:
                break
            
            if items:
                msg = self.worker.recv_multipart()
                self.log.debug(f'Received msg from broker: {msg}')
                
                self.liveness = self.HB_LIVENESS

                assert len(msg) >= 3

                empty = msg.pop(0)
                assert empty == b''

                header = msg.pop(0)
                assert header == W_WORKER

                command = msg.pop(0)
                if command == W_REQUEST:
                    # We should pop and save as many addresses as there are
                    # up to a null part, but for now, just save one…
                    self.reply_to = msg.pop(0)

                    # pop empty
                    empty = msg.pop(0)
                    assert empty == b''

                    # We have a request to process
                    return msg
                elif command == W_HEARTBEAT:
                    pass
                elif command == W_DISCONNECT:
                    self.reconnect_to_broker()
                else:
                    self.log.error(f'Invalid input message: {msg}')
            else:
                self.liveness -= 1
                if self.liveness == 0:
                    self.log.info('Disconnected from broker — reconnecting...')
                    
                    try:
                        time.sleep(self.RECONNECT)
                    except KeyboardInterrupt:
                        break
                    
                    self.reconnect_to_broker()
            
            if time.time() > self.hb_at:
                self.send_to_broker(W_HEARTBEAT)
                self.hb_at = time.time() + self.HB_INTERVAL
        
        self.log.warning('Interrupt received, killing worker...')
        return None
    
    def destroy(self):
        self.ctx.destroy(0)


# BROKER -----------------------------------------------------------------------
class Service:
    """a single Service"""
    def __init__(self, name):
        self.name = name
        self.requests = [] # client reqs
        self.waiting = [] # waiting workers


class Worker:
    """a Worker, idle or active"""
    def __init__(self, identity, address, lifetime):
        self.identity = identity
        self.address = address
        self.expiry = time.time() + lifetime
        self.service = None # Owning service, if known


class MajordomoBroker(BaseComponent):
    """
    Majordomo Protocol broker
    A minimal implementation of http:#rfc.zeromq.org/spec:7 and spec:8
    """

    INTERNAL_SERVICE_PREFIX = b'mmi.'
    HB_LIVENESS = 3
    HB_INTERVAL = 2.5
    HB_EXPIRY = HB_INTERVAL * HB_LIVENESS

    def __init__(self, verbose=False):
        super().__init__(verbose)
        
        self.services = {} # known services
        self.workers = {} # known workers
        self.waiting = [] # idle workers

        self.hb_at = time.time() + self.HB_INTERVAL

        self.ctx = zmq.Context()
        self.socket = self.ctx.socket(zmq.ROUTER)
        self.socket.linger = 0
        self.poller = zmq.Poller()
        self.poller.register(self.socket, zmq.POLLIN)

    def mediate(self):
        """Main broker work happens here"""
        while True:
            try:
                items = self.poller.poll(self.HB_INTERVAL*1e3)
            except KeyboardInterrupt:
                break
            
            if items:
                msg = self.socket.recv_multipart()
                self.log.debug(f'Received message: {msg}')
                
                sender = msg.pop(0)
                empty = msg.pop(0)
                assert empty == b''
                header = msg.pop(0)

                if C_CLIENT == header:
                    self.process_client(sender, msg)
                elif W_WORKER == header:
                    self.process_worker(sender, msg)
                else:
                    self.log.error(f'Invalid message {msg}')

            self.purge_workers()
            self.send_heartbeats()
        
    def destroy(self):
        """Disconnect all workers, destroy context."""
        while self.workers:
            self.delete_worker(self.workers.values()[0], True)
        self.ctx.destroy(0)
    
    def process_client(self, sender, msg):
        """Process a request coming from a client."""
        assert len(msg) >= 2 # service name + body
        service = msg.pop(0)

        # Set reply return address to client sender
        msg = [sender, b''] + msg

        if service.startswith(self.INTERNAL_SERVICE_PREFIX):
            self.service_internal(service, msg)
        else:
            self.dispatch(self.require_service(service), msg)
    
    def process_worker(self, sender, msg):
        """Process message sent to us by a worker."""
        assert len(msg) >= 1 # at least a command

        command = msg.pop(0)

        worker_ready = hexlify(sender) in self.workers
        worker = self.require_worker(sender)

        if W_READY == command:
            assert len(msg) >= 1 # at least, a service name
            service = msg.pop(0)
            # Not first command in session or Reserved service name
            if (worker_ready or service.startswith(self.INTERNAL_SERVICE_PREFIX)):
                self.delete_worker(worker, True)
            else:
                # Attach worker to service and mark as idle
                worker.service = self.require_service(service)
                self.worker_waiting(worker)

        elif W_REPLY == command:
            if (worker_ready):
                # Remove & save client return envelope and insert the
                # protocol header and service name, then rewrap envelope.
                client = msg.pop(0)
                empty = msg.pop(0) # ?
                msg = [client, b'', C_CLIENT, worker.service.name] + msg

                self.socket.send_multipart(msg)
                self.worker_waiting(worker)
            else:
                self.delete_worker(worker, True)

        elif W_HEARTBEAT == command:
            if (worker_ready):
                worker.expiry = time.time() + self.HB_EXPIRY
            else:
                self.delete_worker(worker, True)

        elif W_DISCONNECT == command:
            self.delete_worker(worker, False)

        else:
            self.log.error(f'Invalid message from worker: {msg}')
    
    def delete_worker(self, worker, disconnect):
        """Deletes worker from all data structures, and deletes worker."""
        assert worker is not None
        if disconnect:
            self.send_to_worker(worker, W_DISCONNECT, None, None)
        
        if worker.service is not None:
            worker.service.waiting.remove(worker)
        
        self.workers.pop(worker.identity)

    def require_worker(self, address):
        """Finds the worker (creates if necessary)."""
        assert address is not None
        identity = hexlify(address)
        worker = self.workers.get(identity)
        
        if worker is None:
            worker = Worker(identity, address, self.HB_EXPIRY)
            self.workers[identity] = worker
            
            self.log.info(f'Registering new worker {identity}')
        
        return worker

    def require_service(self, name):
        """Locates the service (creates if necessary)."""
        assert name is not None
        service = self.services.get(name, None)

        if service is None:
            service = Service(name)
            self.services[name] = service
        
        return service
    
    def bind(self, endpoint):
        """Bind broker to endpoint, can call this multiple times.

        We use a single socket for both clients and workers.
        """
        self.socket.bind(endpoint)
        self.log.info(f'{B_BROKER} is active at {endpoint}')
    
    def service_internal(self, service, msg):
        """Handle internal service according to 8/MMI specification"""
        returncode = '501'
        if f'{self.INTERNAL_SERVICE_PREFIX}service' == service:
            name = msg[-1]
            returncode = '200' if name in self.services else '404'
        
        msg[-1] = returncode

        # insert the protocol header and service name after the routing envelope ([client, b''])
        msg = msg[:2] + [C_CLIENT, service] + msg[2:]
        self.socket.send_multipart(msg)
    
    def send_heartbeats(self):
        """Send heartbeats to idle workers if it's time"""
        if time.time() > self.hb_at:
            for worker in self.waiting:
                self.send_to_worker(worker, W_HEARTBEAT, None, None)
            
            self.hb_at = time.time() + self.HB_INTERVAL
    
    def purge_workers(self):
        """Look for & kill expired workers.
        Workers are oldest to most recent, so we stop at the first alive worker.
        """
        while self.waiting:
            w = self.waiting[0]
            if w.expiry < time.time():
                self.log.info(f'Deleting expired worker {w.identity}')
                self.delete_worker(w, False)
                self.waiting.pop(0)
            else:
                break
    
    def worker_waiting(self, worker):
        """This worker is now waiting for work."""
        self.waiting.append(worker)
        worker.service.waiting.append(worker)
        worker.expiry = time.time() + self.HB_EXPIRY
        self.dispatch(worker.service, None)
    
    def dispatch(self, service, msg):
        """Dispatch requests to waiting workers as possible"""
        assert service is not None
        if msg is not None: # Queue message if any
            service.requests.append(msg)
        
        self.purge_workers()

        while service.waiting and service.requests:
            msg = service.requests.pop(0)
            worker = service.waiting.pop(0)
            self.waiting.remove(worker)
            self.send_to_worker(worker, W_REQUEST, None, msg)
    
    def send_to_worker(self, worker, command, option, msg=None):
        """Send message to worker.

        If message is provided, sends that message.
        """
        if msg is None:
            msg = []
        elif not isinstance(msg, list):
            msg = [msg]
        
        # Stack routing and protocol envelopes to start of message
        # and routing envelope
        if option is not None:
            msg = [option] + msg
        
        msg = [worker.address, b'', W_WORKER, command] + msg
        
        self.log.debug(f'Sending {command} to worker: {msg}')
        self.socket.send_multipart(msg)





