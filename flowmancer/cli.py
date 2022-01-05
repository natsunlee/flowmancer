import os
from argparse import ArgumentParser

def main():
    parser = ArgumentParser(description='Flowmancer utility for managing jobs.')
    
    parser.add_argument("--snapshots", action="store_true", dest="snapshots", default=False)
    parser.add_argument("--clear-all-snapshots", action="store_true", dest="clear_all_snapshots", default=False)
    
    args = parser.parse_args()
    
    if args.snapshots:
        for f in os.listdir("./.flowmancer"):
            print(f)
    elif args.clear_all_snapshots:
        confirm = (input("Are you SURE you want to clear all snapshots? This cannot be undone (y/n) [n]: ") or "n").strip().lower()
        if confirm == "n":
            print("Cancelling")
        else:
            snapshot_files = os.listdir("./.flowmancer")
            for f in snapshot_files:
                os.remove(f"./.flowmancer/{f}")
            print(f"Removed {len(snapshot_files)} snapshots.")