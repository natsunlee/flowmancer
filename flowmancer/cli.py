import sys
from .flowmancer import Flowmancer

def main():
    ret = Flowmancer().start()
    sys.exit(ret)