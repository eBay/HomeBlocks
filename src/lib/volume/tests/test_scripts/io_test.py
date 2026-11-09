#!/usr/bin/env python3
## @file index_test.py
import subprocess
import argparse
import shlex


class TestFailedError(Exception):
    pass


def run_test(options):
    cmd_opts = ["--gtest_filter=VolumeIOTest.LongRunningRandomIO", "--gtest_break_on_failure",
                f"--run_time={options['run_time']}", f"--vol_size_gb={options['vol_size_gb']}",
                f"--num_vols={options['num_vols']}", "--device_list", options['device_list']]
    if options['log_mods']:
        cmd_opts.append(f"--log_mods={options['log_mods']}")
    # Everything this script does not consume goes to the binary as-is (e.g. --io_size_kb_range, --cp_timer_ms).
    cmd_opts += options['passthrough']
    test_cmd = f"{options['dirpath']}/test_volume_io " + " ".join(shlex.quote(o) for o in cmd_opts)
    print(f"Running command: {test_cmd}")
    try:
        subprocess.check_call(test_cmd, stderr=subprocess.STDOUT, shell=True)
    except subprocess.CalledProcessError as e:
        print(f"Test failed: {e}")
        raise TestFailedError(f"Test failed!")
    print("Test completed")


def parse_arguments(argv=None):
    # Create the parser
    parser = argparse.ArgumentParser(description='Parse command line options.')

    # Add arguments with default values
    parser.add_argument('--dirpath', help='Directory path', default='.')
    parser.add_argument('--log_mods', help='Log modules', default='')
    parser.add_argument('--run_time', help='Run time in seconds', type=int, default=10)
    parser.add_argument('--vol_size_gb', help='vol_size_gb', type=int, default=500)
    parser.add_argument('--num_vols', help='num_vols', type=int, default=5)
    parser.add_argument('--device_list', help='Device list', default='')
    parser.add_argument('--use_file', help='Initialize device', action="store_true")

    # Unknown arguments are passed through to test_volume_io
    args, passthrough = parser.parse_known_args(argv)

    options = vars(args)
    options['passthrough'] = passthrough

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


def long_running(argv=None):
    options = parse_arguments(argv)
    long_runnig_io(options)
