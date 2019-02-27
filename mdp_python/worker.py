import sys
from lib import MajordomoWorker

def main():
    verbose = '-v' in sys.argv
    worker = MajordomoWorker('tcp://localhost:5555', 'echo', verbose)

    reply = None
    while True:
        request = worker.recv(reply)
        if request is None:
            break
        reply = request # echo is complex :D

if __name__ == '__main__':
    main()