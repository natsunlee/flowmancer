from argparse import ArgumentParser
from argparse import Namespace

parser = ArgumentParser(description='Flowmancer job execution options.')

parser.add_argument("-j", "--jobdef", action="store", dest="jobdef")
parser.add_argument("-r", "--restart", action="store_true", dest="restart", default=False)
parser.add_argument("--skip", action="append", dest="skip", default=[])
parser.add_argument("--run-to", action="store", dest="run_to")
parser.add_argument("--run-from", action="store", dest="run_from")
parser.add_argument("--max-parallel", action="store", type=int, dest="max_parallel")

def parse_args() -> Namespace:
    return parser.parse_args()