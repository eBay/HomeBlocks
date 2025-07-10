#!/usr/bin/env python3
## @file index_test.py
import subprocess
import argparse


class TestFailedError(Exception):
    pass


def run_test(options):
    cmd_opts = f"--gtest_filter=VolumeIOTest.LongRunningRandomIO --gtest_break_on_failure {options['log_mods']} --run_time={options['run_time']} --vol_size_gb={options['vol_size_gb']} --num_vols={options['num_vols']} --device_list {options['device_list']} --hs_chunk_size_mb={options['hs_chunk_size_mb']}"
    test_cmd = f"{options['dirpath']}/test_volume_io {cmd_opts}"
    print(f"Running command: {test_cmd}")
    try:
        subprocess.check_call(test_cmd, stderr=subprocess.STDOUT, shell=True)
    except subprocess.CalledProcessError as e:
        print(f"Test failed: {e}")
        raise TestFailedError(f"Test failed!")
    print("Test completed")


def parse_arguments():
    # Create the parser
    parser = argparse.ArgumentParser(description='Parse command line options.')

    # Add arguments with default values
    parser.add_argument('--dirpath', help='Directory path', default='.')
    parser.add_argument('--log_mods', help='Log modules', default='')
    parser.add_argument('--cp_timer_ms', help='cp_timer_ms', type=int, default=60000)
    parser.add_argument('--run_time', help='Run time in seconds', type=int, default=10)
    parser.add_argument('--vol_size_gb', help='vol_size_gb', type=int, default=500)
    parser.add_argument('--num_vols', help='num_vols', type=int, default=5)
    parser.add_argument('--device_list', help='Device list', default='')
    parser.add_argument('--init_device', help='Initialize device', type=bool, default=True)
    parser.add_argument('--use_file', help='Initialize device', action="store_true")
    parser.add_argument('--hs_chunk_size_mb', help='hs_chunk_size', type=int, default=2048)

    # Parse the known arguments and ignore any unknown arguments
    args, unknown = parser.parse_known_args()

    options = vars(args)

    return options


def long_runnig_io(options):
    print("Long running test started")
    if options['device_list'] == '':
        if not options['use_file']:
            print("No device list provided, use --use_file flag to use a file as a device")
            exit(1)
        # fallocate a file to use as a device
        options['device_list'] = f"{options['dirpath']}/io_long_running_device"
        dev_size = options['vol_size_gb'] * 1.5  # Ensure the device is larger than the volume size
        subprocess.check_call(f"fallocate -l {dev_size}G {options['device_list']}", shell=True)
        print(f"Created device: {options['device_list']} of size {dev_size} GB")
    print(f"options: {options}")
    run_test(options)
    # remove the device file after the test
    if "io_long_running_device" in options['device_list']:
        subprocess.check_call(f"rm -f {options['device_list']}", shell=True)
        print(f"Removed device: {options['device_list']}")
    print("Long running test completed")


def long_running(*args):
    options = parse_arguments()
    long_runnig_io(options)
