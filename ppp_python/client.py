import zmq

REQUEST_TIMEOUT = 2500
REQUEST_RETRIES = 5
SERVER_ENDPOINT = 'tcp://localhost:5555'

context = zmq.Context(1)

print('Connecting to server…')
client = context.socket(zmq.REQ)
client.connect(SERVER_ENDPOINT)

poll = zmq.Poller()
poll.register(client, zmq.POLLIN)

sequence = 0
retries_left = REQUEST_RETRIES
while retries_left:
    sequence += 1
    request = str(sequence).encode()
    print(f'Sending ({request})')
    client.send(request)

    expect_reply = True
    while expect_reply:
        socks = dict(poll.poll(REQUEST_TIMEOUT))
        if socks.get(client) == zmq.POLLIN:
            reply = client.recv()
            if not reply:
                break
            if int(reply) == sequence:
                print(f'I: Server replied OK ({reply})')
                retries_left = REQUEST_RETRIES
                expect_reply = False
            else:
                print(f'E: Malformed reply from server: {reply}')

        else:
            print('W: No response from server, retrying…')
            # Socket is confused. Close and remove it.
            client.setsockopt(zmq.LINGER, 0)
            client.close()
            poll.unregister(client)
            retries_left -= 1
            if retries_left == 0:
                print('E: Server seems to be offline, abandoning')
                break
            print(f'I: Reconnecting and resending ({request})')
            # Create new connection
            client = context.socket(zmq.REQ)
            client.connect(SERVER_ENDPOINT)
            poll.register(client, zmq.POLLIN)
            client.send(request)

context.term()