import sys
from lib import MajordomoBroker

def main():
    """create and start new broker"""
    verbose = '-v' in sys.argv
    broker = MajordomoBroker(verbose)
    broker.bind('tcp://*:5555')
    broker.mediate()

if __name__ == '__main__':
    main()