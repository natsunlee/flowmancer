from argparse import ArgumentParser
from argparse import Namespace

parser = ArgumentParser(description='Flowmancer command line options.')

parser.add_argument("-j", "--jobdef", action="store", dest="jobdef")
parser.add_argument("-r", "--restart", action="store_true", dest="restart", default=False)

def parse_args() -> Namespace:
    return parser.parse_args()