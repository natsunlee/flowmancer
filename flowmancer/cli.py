import sys, asyncio
from .flowmancer import initiate

def main():
    asyncio.run(initiate(sys.argv[1:]))