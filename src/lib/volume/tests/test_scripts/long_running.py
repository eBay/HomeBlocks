#!/usr/bin/env python3
## @file long_running.py
#

import sys
import argparse
import io_test


def parse_arguments():
    parser = argparse.ArgumentParser(description='Run long-running tests.')
    parser.add_argument('--test_suit', help='Test suits to run', default='io_long_running')
    args, service_args = parser.parse_known_args()
    return args, service_args

test_functions = {
    'io_long_running': io_test.long_running
}

def main():
    args, service_args = parse_arguments()

    # Check if the test_suits argument is provided and is valid
    if args.test_suit in test_functions:
        test_functions[args.test_suit](service_args)
    else:
        print(f"Unknown test suite: {args.test_suit}")
        print(f"Available tests: {', '.join(test_functions.keys())}")
        sys.exit(1)


if __name__ == "__main__":
    main()