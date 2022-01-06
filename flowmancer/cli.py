import os
from argparse import ArgumentParser

parser = ArgumentParser(description='Flowmancer utility for managing jobs.')

parser.add_argument("--snapshots", action="store_true", dest="snapshots", default=False)
parser.add_argument("--snapshots-dir", action="store", dest="snapshots_dir", default="./.flowmancer")
parser.add_argument("--clear-all-snapshots", action="store_true", dest="clear_all_snapshots", default=False)

def main() -> None:
    args = parser.parse_args()
    if not any(vars(args).values()):
        parser.print_help()

    if args.snapshots:
        snapshot_files = os.listdir(args.snapshots_dir)
        for f in snapshot_files:
            print(f)
        if not snapshot_files:
            print(f"No snapshots available in: {args.snapshots_dir}")
    elif args.clear_all_snapshots:
        confirm = (input("Are you SURE you want to clear all snapshots? This cannot be undone (y/n) [n]: ") or "n").strip().lower()
        if confirm == "n":
            print("Cancelling")
        else:
            snapshot_files = os.listdir(args.snapshots_dir)
            for f in snapshot_files:
                os.remove(f"{args.snapshots_dir}/{f}")
            print(f"Removed {len(snapshot_files)} snapshots.")