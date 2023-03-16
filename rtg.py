# Copyright 2021 Optiver Asia Pacific Pty. Ltd.
#
# This file is part of Ready Trader Go.
#
#     Ready Trader Go is free software: you can redistribute it and/or
#     modify it under the terms of the GNU Affero General Public License
#     as published by the Free Software Foundation, either version 3 of
#     the License, or (at your option) any later version.
#
#     Ready Trader Go is distributed in the hope that it will be useful,
#     but WITHOUT ANY WARRANTY; without even the implied warranty of
#     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#     GNU Affero General Public License for more details.
#
#     You should have received a copy of the GNU Affero General Public
#     License along with Ready Trader Go.  If not, see
#     <https://www.gnu.org/licenses/>.
import argparse
import multiprocessing
import pathlib
import subprocess
import sys
import time
import traceback
import os
import shutil
import pandas as pd
import json

import ready_trader_go.exchange
import ready_trader_go.trader
from ready_trader_go.modified_event_source import ModifiedRecordedEventSource

try:
    from ready_trader_go.hud.__main__ import main as hud_main, replay as hud_replay
except ImportError:
    hud_main = hud_replay = None


def no_heads_up_display() -> None:
    print("Cannot run the Ready Trader Go heads-up display. This could\n"
          "mean that the PySide6 module has not been installed. Please\n"
          "see the README.md file for more information.", file=sys.stderr)


def replay(args) -> None:
    """Replay a match from a file."""
    if hud_replay is None:
        no_heads_up_display()
        return

    path: pathlib.Path = args.filename
    if not path.is_file():
        print("'%s' is not a regular file" % str(path), file=sys.stderr)
        return

    hud_replay(path)


def on_error(name: str, error: Exception) -> None:
    print("%s threw an exception: %s" % (name, error), file=sys.stderr)
    traceback.print_exception(type(error), error, error.__traceback__, file=sys.stderr)

def move_trader_files_to_home(args) -> bool:
    for auto_trader in args.autotrader:
        if auto_trader.suffix.lower == ".py" or not os.path.isdir(os.path.join("traders", auto_trader.with_suffix(""))):
            folder_name = os.path.join("traders", auto_trader.with_suffix(""))
            print("Trader folder '{0}' doesn't exist".format(folder_name))
            print("Please create a folder for each trading strategy to run the trading simulation.")
            return False
        else:
            shutil.copy(os.path.join("traders/", auto_trader.with_suffix(""), auto_trader.with_suffix(".py")), auto_trader.with_suffix(".py"))
            # it's very important to NOT write auto_trader.with_suffix("") below here
            # since sometimes we're gonna pass {trader_name}.{hash signature of parameters}
            shutil.copy(os.path.join("traders/", auto_trader.with_suffix(""), auto_trader.name + ".json"), auto_trader.with_suffix(".json"))

    return True
    
def erase_trader_files_from_home(args) -> None:
    for auto_trader in args.autotrader:
        assert os.path.isfile(auto_trader.with_suffix(".py")) and os.path.isfile(auto_trader.with_suffix(".json"))
        os.remove(auto_trader.with_suffix(".py"))
        os.remove(auto_trader.with_suffix(".json"))

def run(args) -> None:
    """Run a match."""

    if not move_trader_files_to_home(args):
        # there was an error moving the files
        # remember the names of the traders we pass have to be without suffix, so pass "activity_lots_trader" and NOT "activity_lots_trader.py"
        return

    # take the exchange settings from the first autotrader
    with open(os.path.join("traders", args.autotrader[0].with_suffix(""), args.autotrader[0].name + ".json"), "r") as file:
        exchange_settings = json.load(file)["Exchange"]
        json.dump(exchange_settings, open("exchange.json", "w"))

    for auto_trader in args.autotrader:
        if not auto_trader.with_suffix(".py").exists():
            print("'%s' does not exist" % auto_trader.with_suffix(".py"), file=sys.stderr)
            return
        if not auto_trader.with_suffix(".json").exists():
            print("'%s': configuration file is missing: %s" % (auto_trader, auto_trader.with_suffix(".json")))
            return

    with multiprocessing.Pool(len(args.autotrader) + 2, maxtasksperchild=1) as pool:
        exchange = pool.apply_async(ready_trader_go.exchange.main,
                                    error_callback=lambda e: on_error("The exchange simulator", e))

        # Give the exchange simulator a chance to start up.
        time.sleep(0.5)
        
        for path_ in args.autotrader:
            path = path_.with_suffix(".py")

            if path.suffix.lower() == ".py":
                pool.apply_async(ready_trader_go.trader.main, (path.with_suffix("").name,),
                                 error_callback=lambda e: on_error("Auto-trader '%s'" % path, e))
            else:
                resolved: pathlib.Path = path.resolve()
                pool.apply_async(subprocess.run, ([resolved],), {"check": True, "cwd": resolved.parent},
                                 error_callback=lambda e: on_error("Auto-trader '%s'" % path, e))

        if hud_main is None:
            no_heads_up_display()
            exchange.get()
        else:
            hud_main(args.host, args.port)

    erase_trader_files_from_home(args)

def test(args) -> None:
    """Run a match and copy all log files to the strategy's folder that's being tested."""

    if not move_trader_files_to_home(args):
        # there was an error moving the files
        return

    for auto_trader in args.autotrader:
        if not auto_trader.with_suffix(".py").exists():
            print("'%s' does not exist" % auto_trader, file=sys.stderr)
            return
        if not auto_trader.with_suffix(".json").exists():
            print("'%s': configuration file is missing: %s" % (auto_trader, auto_trader.with_suffix(".json")))
            return

    with multiprocessing.Pool(len(args.autotrader) + 2, maxtasksperchild=1) as pool:
        exchange = pool.apply_async(ready_trader_go.exchange.main,
                                    error_callback=lambda e: on_error("The exchange simulator", e))

        # Give the exchange simulator a chance to start up.
        time.sleep(0.5)
        
        for path_ in args.autotrader:
            path = path_.with_suffix(".py")

            if path.suffix.lower() == ".py":
                pool.apply_async(ready_trader_go.trader.main, (path.with_suffix("").name,),
                                 error_callback=lambda e: on_error("Auto-trader '%s'" % path, e))
            else:
                resolved: pathlib.Path = path.resolve()
                pool.apply_async(subprocess.run, ([resolved],), {"check": True, "cwd": resolved.parent},
                                 error_callback=lambda e: on_error("Auto-trader '%s'" % path, e))

        # we don't display the HUD
        exchange.get()

    # We're gonna create a new directory to store these logs
    # The log files for each simulation will be stored in a directory called
    # `logs_{$log_number}` (within that trader's folder) where `log_number` is 1 for the first round, 2 for the second, etc
    logs_path = pathlib.Path(os.path.join("traders", args.autotrader[0].with_suffix(""), "logs"))
    if logs_path.exists():
        files_and_dirs = os.listdir(logs_path)
    else:
        files_and_dirs = []

    log_number = 1
    for path in files_and_dirs:
        if path.startswith("logs_"):
            log_number += 1

    output_dir = pathlib.Path(os.path.join("traders", args.autotrader[0].with_suffix(""), "logs", "logs" + "_" + str(log_number) + "_" + args.autotrader[0].name))
    output_dir.mkdir(parents=True, exist_ok=True)

    # move all log files to the path where the first autotrader algorithm (from the argument list) is located
    # No need to store all the match events at the moment
    shutil.move("match_events.csv", os.path.join(output_dir, "match_events.csv"))
    shutil.move("score_board.csv", os.path.join(output_dir, "score_board.csv"))
    # shutil.move("exchange.log", os.path.join(output_dir, "exchange.log"))
    shutil.copy(args.autotrader[0].with_suffix(".json"), os.path.join(output_dir, args.autotrader[0].with_suffix(".json")))

    if args.autotrader[0].suffix != "":
        # we're running a benchmark of this autotrader with some
        # custom parameter settings so we can safely delete the configuration file
        custom_config_path = pathlib.Path(os.path.join("traders", args.autotrader[0].with_suffix(""), args.autotrader[0].name + ".json"))
        assert custom_config_path.exists()
        os.remove(custom_config_path)

    # No need to store the logs of each trader at the moment only the main one
    shutil.move(args.autotrader[0].with_suffix(".log"), os.path.join(output_dir, auto_trader.with_suffix(".log")))
    
    erase_trader_files_from_home(args)

def debug_competitor(args) -> None:
    tick_size = 1.00
    etf_clamp = 0.002
    with args.filename.open("r", newline="") as csv_file:
        ModifiedRecordedEventSource.from_csv(csv_file, etf_clamp, tick_size)

def main() -> None:
    """Process command line arguments and execute the given command."""
    parser = argparse.ArgumentParser(description="Ready Trader Go command line utility.")
    subparsers = parser.add_subparsers(title="command")

    run_parser = subparsers.add_parser("run", aliases=["go", "ru"],
                                       description="Run a Ready Trader Go match.",
                                       help="run a Ready Trader Go match")
    # On Mac OSX resolving the name 'localhost' can take forever, so be sure
    # to use '127.0.0.1' here.
    run_parser.add_argument("--host", default="127.0.0.1",
                            help="host name of the exchange simulator (default '127.0.0.1')")
    run_parser.add_argument("--port", default=12347,
                            help="port number of the exchange simulator (default 12347)")
    run_parser.add_argument("autotrader", nargs="*", type=pathlib.Path,
                            help="auto-traders to include in the match")
    run_parser.set_defaults(func=run)

    test_parser = subparsers.add_parser("test",
                                        description="Test a trading strategy against other algorithms or on its own.",
                                        help="test a trading strategy against other algorithms or on its own.") 

    test_parser.add_argument("--host", default="127.0.0.1",
                            help="host name of the exchange simulator (default '127.0.0.1')")
    test_parser.add_argument("--port", default=12347,
                            help="port number of the exchange simulator (default 12347)")
    test_parser.add_argument("autotrader", nargs="*", type=pathlib.Path,
                            help="auto-traders to include in the match, the first one should be the folder of the strategy being tested")
    test_parser.set_defaults(func=test)

    replay_parser = subparsers.add_parser("replay", aliases=["re"],
                                          description=("View a replay of a Ready Trader Go match from "
                                                       " a match events file."),
                                          help="replay a Ready Trader Go match from a file")
    replay_parser.add_argument("filename", nargs="?", default=pathlib.Path("match_events.csv"),
                               help="name of the match events file to replay (default 'match_events.csv')",
                               type=pathlib.Path)
    replay_parser.set_defaults(func=replay)

    debug_parser = subparsers.add_parser("debug")

    debug_parser.add_argument("filename", nargs="?", default=pathlib.Path("match_events.csv"), help="csv file of the match", type=pathlib.Path)

    debug_parser.set_defaults(func=debug_competitor)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    if sys.platform == "darwin":
        multiprocessing.set_start_method("spawn")
    main()
