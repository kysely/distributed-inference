import sys
import time
from lib import MajordomoClient

def main():
    verbose = '-v' in sys.argv
    client = MajordomoClient('tcp://localhost:5555', verbose)

    count = 0
    start = time.time()
    while count < 20000:
        request = [b'Hello', b'World', b'1']
        try:
            reply = client.send('echo', request)
        except KeyboardInterrupt:
            break
        else:
            if reply is None:
                break
        count += 1
 
    print(f'Requests/replies processed: {count}')
    print(f'Program took {time.time()-start}s')

if __name__ == '__main__':
    main()
