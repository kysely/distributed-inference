import zmq
import time
from collections import OrderedDict

HB_LIVENESS = 3
HB_INTERVAL = 1.0

LRU_READY = b'\x01'
LRU_HEARTBEAT = b'\x02'

class Worker:
    def __init__(self, address):
        self.address = address
        self.expiry = time.time() + HB_INTERVAL*HB_LIVENESS

class WorkerQueue:
    def __init__(self):
        self.queue = OrderedDict()
    
    def ready(self, worker):
        self.queue.pop(worker.address, None)
        self.queue[worker.address] = worker
    
    def purge(self):
        """Look for & kill expired workers."""
        t = time.time()
        expired = []

        for address,worker in self.queue.items():
            if t > worker.expiry:
                expired.append(address)
        
        for address in expired:
            print(f'[WARNING]: Idle worker {address} expired')
            self.queue.pop(worker.address, None)

    def next(self):
        address, worker = self.queue.popitem(False)
        return address

context = zmq.Context(1)

frontend = context.socket(zmq.ROUTER) # ROUTER
backend = context.socket(zmq.ROUTER) # ROUTER
frontend.bind('tcp://*:5555') # For clients
backend.bind('tcp://*:5556')  # For workers

poll_workers = zmq.Poller()
poll_workers.register(backend, zmq.POLLIN)

poll_both = zmq.Poller()
poll_both.register(frontend, zmq.POLLIN)
poll_both.register(backend, zmq.POLLIN)

workers = WorkerQueue()

hb_at = time.time() + HB_INTERVAL

while True:
    if len(workers.queue) > 0:
        poller = poll_both
    else:
        poller = poll_workers

    socks = dict(poller.poll(HB_INTERVAL*1000))

    # Handle worker activity on backend
    if socks.get(backend) == zmq.POLLIN:
        # Use worker address for LRU routing
        frames = backend.recv_multipart()
        print(f'=B: From backend: {frames}')
        if not frames:
            break
        address = frames[0]
        workers.ready(Worker(address))

        # Everything after the second (delimiter) frame is reply
        # Validate control message, or return reply to client
        msg = frames[1:]
        if len(msg) == 1:
            if msg[0] not in (LRU_READY, LRU_HEARTBEAT):
                print(f'[ERROR]: Invalid message from worker: {msg}')
        else:
            frontend.send_multipart(msg)
        
        if time.time() > hb_at:
            for worker in workers.queue:
                msg = [worker, LRU_HEARTBEAT]
                backend.send_multipart(msg)
            hb_at = time.time() + HB_INTERVAL

    if socks.get(frontend) == zmq.POLLIN:
        #  Get client request, route to first available worker
        frames = frontend.recv_multipart()
        print(f'FFFF: From frontend: {frames}')
        if not frames:
            break

        request = [workers.next()] + frames
        print(f'FFFF: To frontend: {request}')
        backend.send_multipart(request)
    
    workers.purge()