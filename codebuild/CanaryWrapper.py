# Python wrapper script for collecting Canary metrics, setting-up/tearing-down alarms, reporting metrics to Cloudwatch,
# checking the alarms to ensure everything is correct at the end of the run, and pushing the log to S3 if successful.

# Needs to be installed prior to running
import boto3
import psutil
# Part of standard packages in Python 3.4+
import argparse
import threading
import subprocess
import time
import os
# Dependencies in project folder
from CanaryWrapper_Classes import *
from CanaryWrapper_MetricFunctions import *


# Code for command line argument parsing
# ================================================================================
command_parser = argparse.ArgumentParser("CanaryWrapper")
command_parser.add_argument("--canary_executable", type=str, required=True,
    help="The path to the canary executable (or program - like 'python3')")
command_parser.add_argument("--canary_arguments", type=str, default="",
    help="The arguments to pass/launch the canary executable with")
command_parser.add_argument("--git_hash", type=str, required=True,
    help="The Git commit hash that we are running the canary with")
command_parser.add_argument("--git_repo_name", type=str, required=True,
    help="The name of the Git repository")
command_parser.add_argument("--git_hash_as_namespace", type=bool, default=False,
    help="(OPTIONAL, default=False) If true, the git hash will be used as the name of the Cloudwatch namespace")
command_parser.add_argument("--output_log_filepath", type=str, default="output.log",
    help="(OPTIONAL, default=output.log) The file to output log info to. Set to 'None' to disable")
command_parser.add_argument("--output_to_console", type=bool, default=True,
    help="(OPTIONAL, default=True) If true, info will be output to the console")
command_parser.add_argument("--cloudwatch_region", type=str, default="us-east-1",
    help="(OPTIONAL, default=us-east-1) The AWS region for Cloudwatch")
command_parser.add_argument("--s3_bucket_name", type=str, default="canary-wrapper-folder",
    help="(OPTIONAL, default=canary-wrapper-folder) The name of the S3 bucket where success logs will be stored")
command_parser.add_argument("--snapshot_wait_time", type=int, default=600,
    help="(OPTIONAL, default=600) The number of seconds between gathering and sending snapshot reports")
command_parser.add_argument("--ticket_category", type=str, default="AWS",
    help="(OPTIONAL, default=AWS) The category to register the ticket under")
command_parser.add_argument("--ticket_type", type=str, default="SDKs and Tools",
    help="(OPTIONAL, default='SDKs and Tools') The type to register the ticket under")
command_parser.add_argument("--ticket_item", type=str, default="IoT SDK for CPP",
    help="(OPTIONAL, default='IoT SDK for CPP') The item to register the ticket under")
command_parser.add_argument("--ticket_group", type=str, default="AWS IoT Device SDK",
    help="(OPTIONAL, default='AWS IoT Device SDK') The group to register the ticket under")
command_parser.add_argument("--dependencies", type=str, default="",
    help="(OPTIONAL, default='') Any dependencies and their commit hashes. \
        Current expected format is '(name or path);(hash);(next name or path);(hash);(etc...)'.")
command_parser_arguments = command_parser.parse_args()

if (command_parser_arguments.output_log_filepath == "None"):
    command_parser_arguments.output_log_filepath = None
if (command_parser_arguments.snapshot_wait_time <= 0):
    command_parser_arguments.snapshot_wait_time = 60

# Code for setting up threads and kicking off the wrapper script so it can function
# (Also configures wrapper)
# ================================================================================

# Global variables that both threads use to communicate.
# NOTE - These should likely be replaced with futures or similar for better thread safety.
#        However, these variables are only either read or written to from a single thread, no
#        thread should read and write to these variables.

# NOTE 2 - this needs to be better sorted/defined/handled, as right now it's a bit of a mess
# For this file, it's mostly just naming needs to be improved I think...

# Tells the snapshot thread to stop running on the next interval
# (snapshot_thread reads, application_thread writes)
stop_snapshot_thread = False

# Tells the application thread that the snapshot thread has stopped
# (snapshot_thread writes, application_thread reads)
snapshot_thread_stopped = False

# Tells the application thread the snapshot thread stopped due to an error
# (snapshot_thread writes, application_thread reads)
snapshot_thread_had_error = False

# Tells the application thread the snapshot thread detected a Cloudwatch state in ALARM
# (snapshot_thread writes, application_thread reads)
snapshot_thread_had_cloudwatch_alarm = False
snapshot_thread_had_cloudwatch_alarm_names = []
snapshot_thread_had_cloudwatch_alarm_lowest_severity_value = 6


def snapshot_thread():
    global stop_snapshot_thread
    global snapshot_thread_stopped
    global snapshot_thread_had_error
    global snapshot_thread_had_cloudwatch_alarm
    global snapshot_thread_had_cloudwatch_alarm_names
    global snapshot_thread_had_cloudwatch_alarm_lowest_severity_value

    # Get the command line parser arguments
    global command_parser_arguments

    snapshot_had_internal_error = False

    print("Starting to run snapshot thread...")
    data_snapshot = DataSnapshot(
        git_hash=command_parser_arguments.git_hash,
        git_repo_name=command_parser_arguments.git_repo_name,
        git_hash_as_namespace=command_parser_arguments.git_hash_as_namespace,
        output_log_filepath=command_parser_arguments.output_log_filepath,
        output_to_console=command_parser_arguments.output_to_console,
        cloudwatch_region=command_parser_arguments.cloudwatch_region,
        s3_bucket_name=command_parser_arguments.s3_bucket_name
    )

    # Check for errors
    if (data_snapshot.abort_due_to_internal_error == True):
        snapshot_had_internal_error = True
        snapshot_thread_stopped = True
        snapshot_thread_had_error = True
        data_snapshot.cleanup()
        return

    # Register metrics
    data_snapshot.register_metric(
        new_metric_name="total_cpu_usage",
        new_metric_function=get_metric_total_cpu_usage,
        new_metric_unit="Percent",
        new_metric_alarm_threshold=70,
        new_metric_reports_to_skip=1,
        new_metric_alarm_severity=5)
    data_snapshot.register_metric(
        new_metric_name="total_memory_usage_value",
        new_metric_function=get_metric_total_memory_usage_value,
        new_metric_unit="Bytes")
    data_snapshot.register_metric(
        new_metric_name="total_memory_usage_percent",
        new_metric_function=get_metric_total_memory_usage_percent,
        new_metric_unit="Percent",
        new_metric_alarm_threshold=50,
        new_metric_reports_to_skip=0,
        new_metric_alarm_severity=5)

    # Check for errors
    if (data_snapshot.abort_due_to_internal_error == True):
        snapshot_had_internal_error = True
        snapshot_thread_stopped = True
        snapshot_thread_had_error = True
        data_snapshot.cleanup()
        return

    # Print general diagnosis information
    data_snapshot.output_diagnosis_information(command_parser_arguments.dependencies)

    data_snapshot.print_message("Starting job loop...")
    while True:

        # Should this thread shutdown?
        if (stop_snapshot_thread == True and snapshot_had_internal_error == False):
            # Get a report of all the alarms that might have been set to an alarm state
            triggered_alarms = data_snapshot.get_cloudwatch_alarm_results()
            if len(triggered_alarms) > 0:
                data_snapshot.print_message(
                    "ERROR - One or more alarms are in state of ALARM")
                for triggered_alarm in triggered_alarms:
                    data_snapshot.print_message('    Alarm with name "' + triggered_alarm[1] + '" is in the ALARM state!')
                    snapshot_thread_had_cloudwatch_alarm_names.append(triggered_alarm[1])
                    if (triggered_alarm[2] < snapshot_thread_had_cloudwatch_alarm_lowest_severity_value):
                        snapshot_thread_had_cloudwatch_alarm_lowest_severity_value = triggered_alarm[2]
                snapshot_thread_stopped = True
                snapshot_thread_had_error = True
                snapshot_thread_had_cloudwatch_alarm = True

            data_snapshot.print_message("Stopping job loop...")
            break

        # Check for internal errors
        if (data_snapshot.abort_due_to_internal_error == True):
            snapshot_had_internal_error = True
            snapshot_thread_stopped = True
            snapshot_thread_had_error = True
            break

        # Gather and post the metrics
        if (snapshot_had_internal_error == False):
            try:
                data_snapshot.post_metrics()
            except:
                data_snapshot.print_message("ERROR - exception occured posting metrics!")
                data_snapshot.print_message("(Likely session credentials expired)")

                snapshot_thread_stopped = True
                snapshot_thread_had_error = True
                snapshot_had_internal_error = True
                break

        # Wait for the next snapshot
        time.sleep(command_parser_arguments.snapshot_wait_time)

    # Clean up the task (also post-process: sends to s3 on success, removes alarms, etc)
    data_snapshot.cleanup(snapshot_thread_had_error)

    snapshot_thread_stopped = True
    print("Snapshot thread finished...")


def application_thread():
    global stop_snapshot_thread
    global snapshot_thread_stopped
    global snapshot_thread_had_error
    global snapshot_thread_had_cloudwatch_alarm
    global snapshot_thread_had_cloudwatch_alarm_names
    global snapshot_thread_had_cloudwatch_alarm_lowest_severity_value

    # Get the command line parser arguments
    global command_parser_arguments

    # Is the snapshot thread already stopped? If so, do not bother running the Canary
    time.sleep(5) # wait a few seconds to give the snapshot thread some time to start
    if snapshot_thread_stopped == True:
        print ("ERROR - the Snapshot thread failed before the application started. This generally means a misconfigured permission or credential.")
        exit(1)

    print("Starting to run application thread...")

    canary_arguments = []
    canary_arguments.append(command_parser_arguments.canary_executable)
    canary_arguments.append(command_parser_arguments.canary_arguments)
    command_parser_arguments

    canary_return_code = 0
    try:
        canary_result = subprocess.run(canary_arguments)
        canary_return_code = canary_result.returncode
        if (canary_return_code != 0):
            print("Something in the canary failed!")
            cut_ticket_using_cloudwatch_from_args(
                "The Canary result was non-zero, indicating that something in the canary application itself failed.",
                "Canary result was non-zero",
                command_parser_arguments)
    except Exception as e:
        print("Something in the canary had an exception!")
        cut_ticket_using_cloudwatch_from_args(
                "The code running the canary ran into an exception. This indicates that something in the canary crashed and/or segfaulted.",
                "Canary application ran into an exception",
                command_parser_arguments)
        canary_return_code = -1

    # Wait for the snapshot thread to finish
    stop_snapshot_thread = True
    while snapshot_thread_stopped == False:
        time.sleep(1)
    print("Application thread finished...")

    # If the snapshot thread had an error, then exit because something is wrong and we do not want
    # to report "success" even if the canary itself ran okay
    if (snapshot_thread_had_error == True and canary_return_code == 0):
        # Was it due to a cloudwatch alarm?
        if snapshot_thread_had_cloudwatch_alarm == True:
            cut_ticket_using_cloudwatch_from_args(
                "Canary alarm(s) that are required to pass are in a state of ALARM. \
                    List of metrics in alarm: " + str(snapshot_thread_had_cloudwatch_alarm_names) + ".",
                "Required canary alarm(s) are in state of ALARM",
                snapshot_thread_had_cloudwatch_alarm_lowest_severity_value,
                command_parser_arguments)
            print ("Snapshot thread detected Cloudwatch state(s) in ALARM!")

            if (snapshot_thread_had_cloudwatch_alarm_lowest_severity_value < 6):
                exit(1)
            else:
                exit(canary_return_code)

        else:
            cut_ticket_using_cloudwatch_from_args(
                "The code running the DataSnapshot had an error. See output.log for more information.",
                "DataSnapshot (metric gathering) had an error",
                command_parser_arguments)
            print ("Snapshot thread had an unknown error. See logs for details!")
            exit(1)
    else:
        if (canary_return_code != 0):
            cut_ticket_using_cloudwatch_from_args(
                "The Canary returned a non-zero exit code! Something went wrong in the Canary application itself.",
                "Canary returned non-zero exit code",
                command_parser_arguments)

        exit(canary_return_code)


# Create the threads
run_thread_snapshot = threading.Thread(target=snapshot_thread)
run_thread_application = threading.Thread(target=application_thread)
# Run the threads
run_thread_snapshot.start()
run_thread_application.start()
# Wait for threads to finish
run_thread_snapshot.join()
run_thread_application.join()
exit(0)
