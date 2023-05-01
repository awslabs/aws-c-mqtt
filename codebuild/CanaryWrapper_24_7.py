# Python wrapper script for collecting Canary metrics, setting up alarms, reporting metrics to Cloudwatch,
# checking the alarms to ensure everything is correct at the end of the run, and checking for new
# builds in S3, downloading them, and launching them if they exist (24/7 operation)
#
# Will only stop running if the Canary application itself has an issue - in which case it Canary application will
# need to be fixed and then the wrapper script restarted

# Needs to be installed prior to running
# Part of standard packages in Python 3.4+
import argparse
import time
# Dependencies in project folder
from CanaryWrapper_Classes import *
from CanaryWrapper_MetricFunctions import *

# TODO - Using subprocess may not work on Windows for starting/stopping the application thread.
#        Canary will likely be running on Linux, so it's probably okay, but need to confirm/check at some point....
# ================================================================================
# Code for command line argument parsing

command_parser = argparse.ArgumentParser("CanaryWrapper_24_7")
command_parser.add_argument("--canary_executable", type=str, required=True,
    help="The path to the canary executable")
command_parser.add_argument("--canary_arguments", type=str, default="",
    help="The arguments to pass/launch the canary executable with")
command_parser.add_argument("--s3_bucket_name", type=str, default="canary-wrapper-folder",
    help="(OPTIONAL, default=canary-wrapper-folder) The name of the S3 bucket where success logs will be stored")
command_parser.add_argument("--s3_bucket_application", type=str, required=True,
    help="(OPTIONAL, default=canary-wrapper-folder) The S3 URL to monitor for changes MINUS the bucket name")
command_parser.add_argument("--s3_bucket_application_in_zip", type=str, required=False, default="",
    help="(OPTIONAL, default="") The file path in the zip folder where the application is stored. Will be ignored if set to empty string")
command_parser.add_argument("--lambda_name", type=str, default="iot-send-email-lambda",
    help="(OPTIONAL, default='CanarySendEmailLambda') The name of the Lambda used to send emails")
command_parser_arguments = command_parser.parse_args()

# ================================================================================
# Global variables that both threads use to communicate.
# NOTE - These should likely be replaced with futures or similar for better thread safety.
#        However, these variables are only either read or written to from a single thread, no
#        thread should read and write to these variables.

# The local file path (and extension) of the Canary application that the wrapper will manage
# (This will also be the filename and directory used when a new file is detected in S3)
# [THIS IS READ ONLY]
canary_local_application_path = command_parser_arguments.canary_executable
if (canary_local_application_path == ""):
    print ("ERROR - required canary_executable is empty!")
    exit (1) # cannot run without a canary executable
# This is the arguments passed to the local file path when starting
# [THIS IS READ ONLY]
canary_local_application_arguments = command_parser_arguments.canary_arguments
# The "Git Hash" to use for metrics and dimensions
# [THIS IS READ ONLY]
canary_local_git_hash_stub = "Canary"
# The "Git Repo" name to use for metrics and dimensions. Is hard-coded since this is a 24/7 canary that should only run for MQTT
# [THIS IS READ ONLY]
canary_local_git_repo_stub = "MQTT5_24_7"
# The Fixed Namespace name for the Canary
# [THIS IS READ ONLY]
canary_local_git_fixed_namespace = "MQTT5_24_7_Canary"
# The S3 bucket name to monitor for the application
# [THIS IS READ ONLY]
canary_s3_bucket_name = command_parser_arguments.s3_bucket_name
if (canary_s3_bucket_name == ""):
    canary_s3_bucket_name = "canary-wrapper-folder"
# The file in the S3 bucket to monitor (The application filepath and file. Example: "canary/canary_application.exe")
# [THIS IS READ ONLY]
canary_s3_bucket_application_path = command_parser_arguments.s3_bucket_application
if (canary_s3_bucket_application_path == ""):
    print ("ERROR - required s3_bucket_application is empty!")
    exit (1) # cannot run without a s3_bucket_application to monitor
# The location of the file in the S3 zip, if the S3 file being monitored is a zip
# (THIS IS READ ONLY)
canary_s3_bucket_application_path_zip = command_parser_arguments.s3_bucket_application_in_zip
if (canary_s3_bucket_application_path_zip == ""):
    canary_s3_bucket_application_path_zip = None
# The name of the email lambda. If an empty string is set, it defaults to 'iot-send-email-lambda'
if (command_parser_arguments.lambda_name == ""):
    command_parser_arguments.lambda_name = "iot-send-email-lambda"
# The region the canary is running in
# (THIS IS READ ONLY)
canary_region_stub = "us-east-1"

# How long (in seconds) to wait before gathering metrics and pushing them to Cloudwatch
canary_metrics_wait_time = 600 # 10 minutes
# How long (in seconds) to run the Application thread loop. Should be shorter or equal to the Canary Metrics time
canary_application_loop_wait_time = 300 # 5 minutes

# For testing - set both to 30 seconds
# canary_metrics_wait_time = 30
# canary_application_loop_wait_time = 30

# ================================================================================

# Make the snapshot class
data_snapshot = DataSnapshot(
    git_hash=canary_local_git_hash_stub,
    git_repo_name=canary_local_git_repo_stub,
    git_hash_as_namespace=False,
    datetime_string=None,
    git_fixed_namespace_text=canary_local_git_fixed_namespace,
    output_log_filepath="output.txt",
    output_to_console=True,
    cloudwatch_region=canary_region_stub,
    cloudwatch_make_dashboard=True,
    cloudwatch_teardown_alarms_on_complete=True,
    cloudwatch_teardown_dashboard_on_complete=False,
    s3_bucket_name=canary_s3_bucket_name,
    s3_bucket_upload_on_complete=True,
    lambda_name=command_parser_arguments.lambda_name,
    metric_frequency=canary_metrics_wait_time)

# Make sure nothing failed
if (data_snapshot.abort_due_to_internal_error == True):
    print ("INFO - Stopping application due to error caused by credentials")
    print ("Please fix your credentials and then restart this application again")
    exit(0)

# Register metrics
data_snapshot.register_metric(
    new_metric_name="total_cpu_usage",
    new_metric_function=get_metric_total_cpu_usage,
    new_metric_unit="Percent",
    new_metric_alarm_threshold=70,
    new_metric_reports_to_skip=1,
    new_metric_alarm_severity=5,
    is_percent=True)
data_snapshot.register_metric(
    new_metric_name="total_memory_usage_value",
    new_metric_function=get_metric_total_memory_usage_value,
    new_metric_unit="Bytes")
data_snapshot.register_metric(
    new_metric_name="total_memory_usage_percent",
    new_metric_function=get_metric_total_memory_usage_percent,
    new_metric_unit="Percent",
    new_metric_alarm_threshold=70,
    new_metric_reports_to_skip=0,
    new_metric_alarm_severity=5,
    is_percent=True)

data_snapshot.register_dashboard_widget("Process CPU Usage - Percentage", ["total_cpu_usage"], 60)
data_snapshot.register_dashboard_widget("Process Memory Usage - Percentage", ["total_memory_usage_percent"], 60)

# Print diagnosis information
data_snapshot.output_diagnosis_information("24/7 Canary cannot show dependencies!")

# Make the S3 class
s3_monitor = S3Monitor(
    s3_bucket_name=canary_s3_bucket_name,
    s3_file_name=canary_s3_bucket_application_path,
    s3_file_name_in_zip=canary_s3_bucket_application_path_zip,
    canary_local_application_path=canary_local_application_path,
    data_snapshot=data_snapshot)

if (s3_monitor.had_internal_error == True):
    print ("INFO - Stopping application due to error caused by credentials")
    print ("Please fix your credentials and then restart this application again")
    exit(0)

# Make the snapshot (metrics) monitor
snapshot_monitor = SnapshotMonitor(
    wrapper_data_snapshot=data_snapshot,
    wrapper_metrics_wait_time=canary_metrics_wait_time)

# Make sure nothing failed
if (snapshot_monitor.had_internal_error == True):
    print ("INFO - Stopping application due to error caused by credentials")
    print ("Please fix your credentials and then restart this application again")
    exit(0)

# Make the application monitor
application_monitor = ApplicationMonitor(
    wrapper_application_path=canary_local_application_path,
    wrapper_application_arguments=canary_local_application_arguments,
    wrapper_application_restart_on_finish=True,
    data_snapshot=data_snapshot)

# Make sure nothing failed
if (application_monitor.error_has_occurred == True):
    print ("INFO - Stopping application due to error caused by credentials")
    print ("Please fix your credentials and then restart this application again")
    exit(0)

# For tracking if we stopped due to a metric alarm
stopped_due_to_metric_alarm = False

def execution_loop():
    while True:
        s3_monitor.monitor_loop_function(time_passed=canary_application_loop_wait_time)

        # Is there an error?
        if (s3_monitor.had_internal_error == True):
            print ("[Debug] S3 monitor had an internal error!")
            break

        # Is there a new file?
        if (s3_monitor.s3_file_needs_replacing == True):
            # Stop the application
            print ("[Debug] Stopping application monitor...")
            application_monitor.stop_monitoring()
            print ("[Debug] Getting S3 file...")
            s3_monitor.replace_current_file_for_new_file()
            # Start the application
            print ("[Debug] Starting application monitor...")
            application_monitor.start_monitoring()
            # Allow the snapshot monitor to cut a ticket
            snapshot_monitor.can_cut_ticket = True

        snapshot_monitor.monitor_loop_function(
            time_passed=canary_application_loop_wait_time, psutil_process=application_monitor.application_process_psutil)
        application_monitor.monitor_loop_function(
            time_passed=canary_application_loop_wait_time)

        # Did a metric go into alarm?
        if (snapshot_monitor.has_cut_ticket == True):
            # Do not allow it to cut anymore tickets until it gets a new build
            snapshot_monitor.can_cut_ticket = False

        # If an error has occurred or otherwise this thread needs to stop, then break the loop
        if (application_monitor.error_has_occurred == True or snapshot_monitor.had_internal_error == True):
            if (application_monitor.error_has_occurred == True):
                print ("[Debug] Application monitor error occurred!")
            else:
                print ("[Debug] Snapshot monitor internal error ocurred!")
            break

        time.sleep(canary_application_loop_wait_time)


def application_thread():
    # Start the application going
    snapshot_monitor.start_monitoring()
    application_monitor.start_monitoring()
    # Allow the snapshot monitor to cut tickets
    snapshot_monitor.can_cut_ticket = True

    start_email_body = "MQTT5 24/7 Canary Wrapper has started. This will run and continue to test new MQTT5 application builds as"
    start_email_body += " they pass CodeBuild and are uploaded to S3."
    snapshot_monitor.send_email(email_body=start_email_body, email_subject_text_append="Started")

    # Start the execution loop
    execution_loop()

    # Make sure everything is stopped
    snapshot_monitor.stop_monitoring()
    application_monitor.stop_monitoring()

    # Track whether this counts as an error (and therefore we should cleanup accordingly) or not
    wrapper_error_occurred = False

    send_finished_email = True
    finished_email_body = "MQTT5 24/7 Canary Wrapper has stopped."
    finished_email_body += "\n\n"

    try:
        # Find out why we stopped
        # S3 Monitor
        if (s3_monitor.had_internal_error == True):
            if (s3_monitor.error_due_to_credentials == False):
                print ("ERROR - S3 monitor stopped due to internal error!")
                cut_ticket_using_cloudwatch(
                    git_repo_name=canary_local_git_repo_stub,
                    git_hash=canary_local_git_hash_stub,
                    git_hash_as_namespace=False,
                    git_fixed_namespace_text=canary_local_git_fixed_namespace,
                    cloudwatch_region=canary_region_stub,
                    ticket_description="Snapshot monitor stopped due to internal error! Reason info: " + s3_monitor.internal_error_reason,
                    ticket_reason="S3 monitor stopped due to internal error",
                    ticket_allow_duplicates=True,
                    ticket_category="AWS",
                    ticket_type="SDKs and Tools",
                    ticket_item="IoT SDK for CPP",
                    ticket_group="AWS IoT Device SDK",
                    ticket_severity=4)
                finished_email_body += "Failure due to S3 monitor stopping due to an internal error."
                finished_email_body += " Reason given for error: " + s3_monitor.internal_error_reason
                wrapper_error_occurred = True
        # Snapshot Monitor
        elif (snapshot_monitor.had_internal_error == True):
            if (snapshot_monitor.has_cut_ticket == True):
                # We do not need to cut a ticket here - it's cut by the snapshot monitor!
                print ("ERROR - Snapshot monitor stopped due to metric in alarm!")
                finished_email_body += "Failure due to required metrics being in alarm! A new ticket should have been cut!"
                finished_email_body += "\nMetrics in Alarm: " + str(snapshot_monitor.cloudwatch_current_alarms_triggered)
                finished_email_body += "\nNOTE - this shouldn't occur in the 24/7 Canary! If it does, then the wrapper needs adjusting."
                wrapper_error_occurred = True
            else:
                print ("ERROR - Snapshot monitor stopped due to internal error!")
                cut_ticket_using_cloudwatch(
                    git_repo_name=canary_local_git_repo_stub,
                    git_hash=canary_local_git_hash_stub,
                    git_hash_as_namespace=False,
                    git_fixed_namespace_text=canary_local_git_fixed_namespace,
                    cloudwatch_region=canary_region_stub,
                    ticket_description="Snapshot monitor stopped due to internal error! Reason info: " + snapshot_monitor.internal_error_reason,
                    ticket_reason="Snapshot monitor stopped due to internal error",
                    ticket_allow_duplicates=True,
                    ticket_category="AWS",
                    ticket_type="SDKs and Tools",
                    ticket_item="IoT SDK for CPP",
                    ticket_group="AWS IoT Device SDK",
                    ticket_severity=4)
                wrapper_error_occurred = True
                finished_email_body += "Failure due to Snapshot monitor stopping due to an internal error."
                finished_email_body += " Reason given for error: " + snapshot_monitor.internal_error_reason
        # Application Monitor
        elif (application_monitor.error_has_occurred == True):
            if (application_monitor.error_due_to_credentials == True):
                print ("INFO - Stopping application due to error caused by credentials")
                print ("Please fix your credentials and then restart this application again")
                wrapper_error_occurred = True
                send_finished_email = False
            else:
                # Is the error something in the canary failed?
                if (application_monitor.error_code != 0):
                    cut_ticket_using_cloudwatch(
                        git_repo_name=canary_local_git_repo_stub,
                        git_hash=canary_local_git_hash_stub,
                        git_hash_as_namespace=False,
                        git_fixed_namespace_text=canary_local_git_fixed_namespace,
                        cloudwatch_region=canary_region_stub,
                        ticket_description="The 24/7 Canary exited with a non-zero exit code! This likely means something in the canary failed.",
                        ticket_reason="The 24/7 Canary exited with a non-zero exit code",
                        ticket_allow_duplicates=True,
                        ticket_category="AWS",
                        ticket_type="SDKs and Tools",
                        ticket_item="IoT SDK for CPP",
                        ticket_group="AWS IoT Device SDK",
                        ticket_severity=3)
                    wrapper_error_occurred = True
                    finished_email_body += "Failure due to MQTT5 application exiting with a non-zero exit code!"
                    finished_email_body += " This means something in the Canary application itself failed"
                else:
                    cut_ticket_using_cloudwatch(
                        git_repo_name=canary_local_git_repo_stub,
                        git_hash=canary_local_git_hash_stub,
                        git_hash_as_namespace=False,
                        git_fixed_namespace_text=canary_local_git_fixed_namespace,
                        cloudwatch_region=canary_region_stub,
                        ticket_description="The 24/7 Canary exited with a zero exit code but did not restart!",
                        ticket_reason="The 24/7 Canary exited with a zero exit code but did not restart",
                        ticket_allow_duplicates=True,
                        ticket_category="AWS",
                        ticket_type="SDKs and Tools",
                        ticket_item="IoT SDK for CPP",
                        ticket_group="AWS IoT Device SDK",
                        ticket_severity=3)
                    wrapper_error_occurred = True
                    finished_email_body += "Failure due to MQTT5 application stopping and not automatically restarting!"
                    finished_email_body += " This shouldn't occur and means something is wrong with the Canary wrapper!"
        # Other
        else:
            print ("ERROR - 24/7 Canary stopped due to unknown reason!")
            cut_ticket_using_cloudwatch(
                git_repo_name=canary_local_git_repo_stub,
                git_hash=canary_local_git_hash_stub,
                git_hash_as_namespace=False,
                git_fixed_namespace_text=canary_local_git_fixed_namespace,
                cloudwatch_region=canary_region_stub,
                ticket_description="The 24/7 Canary stopped for an unknown reason!",
                ticket_reason="The 24/7 Canary stopped for unknown reason",
                ticket_allow_duplicates=True,
                ticket_category="AWS",
                ticket_type="SDKs and Tools",
                ticket_item="IoT SDK for CPP",
                ticket_group="AWS IoT Device SDK",
                ticket_severity=3)
            wrapper_error_occurred = True
            finished_email_body += "Failure due to unknown reason! This shouldn't happen and means something has gone wrong!"
    except Exception as e:
        print ("ERROR: Could not (possibly) cut ticket due to exception!")
        print (f"Exception: {repr(e)}", flush=True)

    # Clean everything up and stop
    snapshot_monitor.cleanup_monitor(error_occurred=wrapper_error_occurred)
    application_monitor.cleanup_monitor(error_occurred=wrapper_error_occurred)
    print ("24/7 Canary finished!")

    finished_email_body += "\n\nYou can find the log file for this run at the following S3 location: "
    finished_email_body += "https://s3.console.aws.amazon.com/s3/object/"
    finished_email_body += command_parser_arguments.s3_bucket_name
    finished_email_body += "?region=" + canary_region_stub
    finished_email_body += "&prefix=" + canary_local_git_repo_stub + "/"
    if (wrapper_error_occurred == True):
        finished_email_body += "Failed_Logs/"
    finished_email_body += canary_local_git_hash_stub + ".log"
    # Send the finish email
    if (send_finished_email == True):
        if (wrapper_error_occurred == True):
            snapshot_monitor.send_email(email_body=finished_email_body, email_subject_text_append="Had an error")
        else:
            snapshot_monitor.send_email(email_body=finished_email_body, email_subject_text_append="Finished")

    exit (-1)


# Start the application!
application_thread()
