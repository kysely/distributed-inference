from random import randint
import time
import zmq

HB_LIVENESS = 3
HB_INTERVAL = 1.0
INTERVAL_INIT = 1
INTERVAL_MAX = 4

LRU_READY = b'\x01'
LRU_HEARTBEAT = b'\x02'


identity = '%04X-%04X' % (randint(0, 0x10000), randint(0, 0x10000))

def worker_socket(context, poller):
    worker = context.socket(zmq.DEALER)
    worker.setsockopt_string(zmq.IDENTITY, identity)
    poller.register(worker, zmq.POLLIN)
    worker.connect('tcp://localhost:5556')
    print(f'[INFO {identity}] Worker ready...')
    worker.send(LRU_READY)
    return worker


context = zmq.Context(1)
poller = zmq.Poller()

liveness = HB_LIVENESS
interval = INTERVAL_INIT

hb_at = time.time() + HB_INTERVAL

worker = worker_socket(context, poller)
cycles = 0

while True:
    socks = dict(poller.poll(HB_INTERVAL*1000))

    if socks.get(worker) == zmq.POLLIN:
        #  Handle if activity within hb interval
        #  - 3-part envelope + content -> request
        #  - 1-part HEARTBEAT -> heartbeat
        frames = worker.recv_multipart()
        if not frames:
            break # interrupted
        
        if len(frames) == 3:
            cycles += 1
            if cycles > 3 and randint(0, 5) == 0:
                print(f'[INFO {identity}] Simulating crash...')
                break
            if cycles > 3 and randint(0, 5) == 0:
                print(f'[INFO {identity}] Simulating CPU overload...')
                time.sleep(3)
            
            print(f'[INFO {identity}] Normal reply')
            worker.send_multipart(frames)

            liveness = HB_LIVENESS
            time.sleep(1) # do some heavy work

        elif len(frames) == 1 and frames[0] == LRU_HEARTBEAT:
            # We have ready/hb
            # print(f'[INFO {identity}] Queue heartbeat')
            liveness = HB_LIVENESS
        else:
            print(f'[ERROR {identity}] Invalid message: {frames}')
        
        interval = INTERVAL_INIT
    else:
        # HB timed out
        liveness -= 1

        if liveness == 0:
            print(f'[WARNING {identity}] Heartbeat failure, cannot reach queue')
            print(f'[WARNING {identity}] Reconnecting in {interval}s')
            time.sleep(interval)

            if interval < INTERVAL_MAX:
                interval *= 2
            
            poller.unregister(worker)
            worker.setsockopt(zmq.LINGER, 0)
            worker.close()
            worker = worker_socket(context, poller)
            liveness = HB_LIVENESS
    
    # at the end of loop, check whether you should send hb
    if time.time() > hb_at:
        hb_at = time.time() + HB_INTERVAL
        # print(f'[INFO {identity}] Worker heartbeat')
        worker.send(LRU_HEARTBEAT)



print(f'==== Worker {identity} dead =============')