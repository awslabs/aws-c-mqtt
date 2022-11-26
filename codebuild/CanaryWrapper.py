# Python wrapper script for collecting Canary metrics, setting-up/tearing-down alarms, reporting metrics to Cloudwatch,
# checking the alarms to ensure everything is correct at the end of the run, and pushing the log to S3 if successful.

# Needs to be installed prior to running
# Part of standard packages in Python 3.4+
import argparse
import time
import datetime
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
command_parser.add_argument("--lambda_name", type=str, default="iot-send-email-lambda",
    help="(OPTIONAL, default='CanarySendEmailLambda') The name of the Lambda used to send emails")
command_parser.add_argument("--codebuild_log_path", type=str, default="",
    help="The CODEBUILD_LOG_PATH environment variable. Leave blank to ignore")
command_parser_arguments = command_parser.parse_args()

if (command_parser_arguments.output_log_filepath == "None"):
    command_parser_arguments.output_log_filepath = None
if (command_parser_arguments.snapshot_wait_time <= 0):
    command_parser_arguments.snapshot_wait_time = 60

# Deal with possibly empty values in semi-critical commands/arguments
if (command_parser_arguments.canary_executable == ""):
    print ("ERROR - required canary_executable is empty!", flush=True)
    exit (1) # cannot run without a canary executable
if (command_parser_arguments.git_hash == ""):
    print ("ERROR - required git_hash is empty!", flush=True)
    exit (1) # cannot run without git hash
if (command_parser_arguments.git_repo_name == ""):
    print ("ERROR - required git_repo_name is empty!", flush=True)
    exit (1) # cannot run without git repo name
if (command_parser_arguments.git_hash_as_namespace is not True and command_parser_arguments.git_hash_as_namespace is not False):
    command_parser_arguments.git_hash_as_namespace = False
if (command_parser_arguments.output_log_filepath == ""):
    command_parser_arguments.output_log_filepath = None
if (command_parser_arguments.output_to_console != True and command_parser_arguments.output_to_console != False):
    command_parser_arguments.output_to_console = True
if (command_parser_arguments.cloudwatch_region == ""):
    command_parser_arguments.cloudwatch_region = "us-east-1"
if (command_parser_arguments.s3_bucket_name == ""):
    command_parser_arguments.s3_bucket_name = "canary-wrapper-folder"
if (command_parser_arguments.ticket_category == ""):
    command_parser_arguments.ticket_category = "AWS"
if (command_parser_arguments.ticket_type == ""):
    command_parser_arguments.ticket_type = "SDKs and Tools"
if (command_parser_arguments.ticket_item == ""):
    command_parser_arguments.ticket_item = "IoT SDK for CPP"
if (command_parser_arguments.ticket_group == ""):
    command_parser_arguments.ticket_group = "AWS IoT Device SDK"



# ================================================================================

datetime_now = datetime.datetime.now()
datetime_string = datetime_now.strftime("%d-%m-%Y/%H-%M-%S")
print("Datetime string is: " + datetime_string, flush=True)

# Make the snapshot class
data_snapshot = DataSnapshot(
    git_hash=command_parser_arguments.git_hash,
    git_repo_name=command_parser_arguments.git_repo_name,
    datetime_string=datetime_string,
    git_hash_as_namespace=command_parser_arguments.git_hash_as_namespace,
    git_fixed_namespace_text="mqtt5_canary",
    output_log_filepath="output.txt",
    output_to_console=command_parser_arguments.output_to_console,
    cloudwatch_region="us-east-1",
    cloudwatch_make_dashboard=False,
    cloudwatch_teardown_alarms_on_complete=True,
    cloudwatch_teardown_dashboard_on_complete=True,
    s3_bucket_name=command_parser_arguments.s3_bucket_name,
    s3_bucket_upload_on_complete=True,
    lambda_name=command_parser_arguments.lambda_name,
    metric_frequency=command_parser_arguments.snapshot_wait_time)

# Make sure nothing failed
if (data_snapshot.abort_due_to_internal_error == True):
    print ("INFO - Stopping application due to error caused by credentials")
    print ("Please fix your credentials and then restart this application again", flush=True)
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

# Print diagnosis information
data_snapshot.output_diagnosis_information(command_parser_arguments.dependencies)

# Make the snapshot (metrics) monitor
snapshot_monitor = SnapshotMonitor(
    wrapper_data_snapshot=data_snapshot,
    wrapper_metrics_wait_time=command_parser_arguments.snapshot_wait_time)

# Make sure nothing failed
if (snapshot_monitor.had_internal_error == True):
    print ("INFO - Stopping application due to error caused by credentials")
    print ("Please fix your credentials and then restart this application again", flush=True)
    exit(0)

# Make the application monitor
application_monitor = ApplicationMonitor(
    wrapper_application_path=command_parser_arguments.canary_executable,
    wrapper_application_arguments=command_parser_arguments.canary_arguments,
    wrapper_application_restart_on_finish=False,
    data_snapshot=data_snapshot # pass the data_snapshot for printing to the log
)

# Make sure nothing failed
if (application_monitor.error_has_occurred == True):
    print ("INFO - Stopping application due to error caused by credentials")
    print ("Please fix your credentials and then restart this application again", flush=True)
    exit(0)

# For tracking if we stopped due to a metric alarm
stopped_due_to_metric_alarm = False

execution_sleep_time = 30
def execution_loop():
    while True:
        snapshot_monitor.monitor_loop_function(
            time_passed=execution_sleep_time, psutil_process=application_monitor.application_process_psutil)
        application_monitor.monitor_loop_function(
            time_passed=execution_sleep_time)

        # Did a metric go into alarm?
        if (snapshot_monitor.has_cut_ticket == True):
            # Set that we had an 'internal error' so we go down the right code path
            snapshot_monitor.had_internal_error = True
            break

        # If an error has occurred or otherwise this thread needs to stop, then break the loop
        if (application_monitor.error_has_occurred == True or snapshot_monitor.had_internal_error == True):
            break

        time.sleep(execution_sleep_time)


def application_thread():

    start_email_body = "MQTT5 Short Running Canary Wrapper has started for "
    start_email_body += "\"" + command_parser_arguments.git_repo_name + "\" commit \"" + command_parser_arguments.git_hash + "\""
    start_email_body += "\nThe wrapper will run for the length the MQTT5 Canary application is set to run for, which is determined by "
    start_email_body += "the arguments set. The arguments used for this run are listed below:"
    start_email_body += "\n  Arguments: " + command_parser_arguments.canary_arguments
    snapshot_monitor.send_email(email_body=start_email_body, email_subject_text_append="Started")

    # Start the application going
    snapshot_monitor.start_monitoring()
    application_monitor.start_monitoring()
    # Allow the snapshot monitor to cut tickets
    snapshot_monitor.can_cut_ticket = True

    # Start the execution loop
    execution_loop()

    # Make sure everything is stopped
    snapshot_monitor.stop_monitoring()
    application_monitor.stop_monitoring()

    # Track whether this counts as an error (and therefore we should cleanup accordingly) or not
    wrapper_error_occurred = False
    # Finished Email
    send_finished_email = True
    finished_email_body = "MQTT5 Short Running Canary Wrapper has stopped."
    finished_email_body += "\n\n"

    try:
        # Find out why we stopped
        if (snapshot_monitor.had_internal_error == True):
            if (snapshot_monitor.has_cut_ticket == True):
                # We do not need to cut a ticket here - it's cut by the snapshot monitor!
                print ("ERROR - Snapshot monitor stopped due to metric in alarm!", flush=True)
                finished_email_body += "Failure due to required metrics being in alarm! A new ticket should have been cut!"
                finished_email_body += "\nMetrics in Alarm: " + str(snapshot_monitor.cloudwatch_current_alarms_triggered)
                wrapper_error_occurred = True
            else:
                print ("ERROR - Snapshot monitor stopped due to internal error!", flush=True)
                cut_ticket_using_cloudwatch(
                    git_repo_name=command_parser_arguments.git_repo_name,
                    git_hash=command_parser_arguments.git_hash,
                    git_hash_as_namespace=command_parser_arguments.git_hash_as_namespace,
                    git_fixed_namespace_text="mqtt5_canary",
                    cloudwatch_region="us-east-1",
                    ticket_description="Snapshot monitor stopped due to internal error! Reason info: " + snapshot_monitor.internal_error_reason,
                    ticket_reason="Snapshot monitor stopped due to internal error",
                    ticket_allow_duplicates=True,
                    ticket_category=command_parser_arguments.ticket_category,
                    ticket_item=command_parser_arguments.ticket_item,
                    ticket_group=command_parser_arguments.ticket_group,
                    ticket_type=command_parser_arguments.ticket_type,
                    ticket_severity=4)
                wrapper_error_occurred = True
                finished_email_body += "Failure due to Snapshot monitor stopping due to an internal error."
                finished_email_body += " Reason given for error: " + snapshot_monitor.internal_error_reason

        elif (application_monitor.error_has_occurred == True):
            if (application_monitor.error_due_to_credentials == True):
                print ("INFO - Stopping application due to error caused by credentials")
                print ("Please fix your credentials and then restart this application again", flush=True)
                wrapper_error_occurred = True
                send_finished_email = False
            else:
                # Is the error something in the canary failed?
                if (application_monitor.error_code != 0):
                    cut_ticket_using_cloudwatch(
                        git_repo_name=command_parser_arguments.git_repo_name,
                        git_hash=command_parser_arguments.git_hash,
                        git_hash_as_namespace=command_parser_arguments.git_hash_as_namespace,
                        git_fixed_namespace_text="mqtt5_canary",
                        cloudwatch_region="us-east-1",
                        ticket_description="The Short Running Canary exited with a non-zero exit code! This likely means something in the canary failed.",
                        ticket_reason="The Short Running Canary exited with a non-zero exit code",
                        ticket_allow_duplicates=True,
                        ticket_category=command_parser_arguments.ticket_category,
                        ticket_item=command_parser_arguments.ticket_item,
                        ticket_group=command_parser_arguments.ticket_group,
                        ticket_type=command_parser_arguments.ticket_type,
                        ticket_severity=4)
                    wrapper_error_occurred = True
                    finished_email_body += "Failure due to MQTT5 application exiting with a non-zero exit code! This means something in the Canary application itself failed"
                else:
                    print ("INFO - Stopping application. No error has occurred, application has stopped normally", flush=True)
                    application_monitor.print_stdout()
                    finished_email_body += "Short Running Canary finished successfully and run without errors!"
                    wrapper_error_occurred = False
        else:
            print ("ERROR - Short Running Canary stopped due to unknown reason!", flush=True)
            cut_ticket_using_cloudwatch(
                git_repo_name=command_parser_arguments.git_repo_name,
                git_hash=command_parser_arguments.git_hash,
                git_hash_as_namespace=command_parser_arguments.git_hash_as_namespace,
                git_fixed_namespace_text="mqtt5_canary",
                cloudwatch_region="us-east-1",
                ticket_description="The Short Running Canary stopped for an unknown reason!",
                ticket_reason="The Short Running Canary stopped for unknown reason",
                ticket_allow_duplicates=True,
                ticket_category=command_parser_arguments.ticket_category,
                ticket_item=command_parser_arguments.ticket_item,
                ticket_group=command_parser_arguments.ticket_group,
                ticket_type=command_parser_arguments.ticket_type,
                ticket_severity=4)
            wrapper_error_occurred = True
            finished_email_body += "Failure due to unknown reason! This shouldn't happen and means something has gone wrong!"
    except Exception as e:
        print ("ERROR: Could not (possibly) cut ticket due to exception!")
        print ("Exception: " + str(e), flush=True)

    # Clean everything up and stop
    snapshot_monitor.cleanup_monitor(error_occurred=wrapper_error_occurred)
    application_monitor.cleanup_monitor(error_occurred=wrapper_error_occurred)
    print ("Short Running Canary finished!", flush=True)

    finished_email_body += "\n\nYou can find the log file for this run at the following S3 location: "
    finished_email_body += "https://s3.console.aws.amazon.com/s3/object/"
    finished_email_body += command_parser_arguments.s3_bucket_name
    finished_email_body += "?region=" + command_parser_arguments.cloudwatch_region
    finished_email_body += "&prefix=" + command_parser_arguments.git_repo_name + "/" + datetime_string + "/"
    if (wrapper_error_occurred == True):
        finished_email_body += "Failed_Logs/"
    finished_email_body += command_parser_arguments.git_hash + ".log"
    if (command_parser_arguments.codebuild_log_path != ""):
        print ("\n Codebuild log path: " + command_parser_arguments.codebuild_log_path + "\n")

    # Send the finish email
    if (send_finished_email == True):
        if (wrapper_error_occurred == True):
            snapshot_monitor.send_email(email_body=finished_email_body, email_subject_text_append="Had an error")
        else:
            snapshot_monitor.send_email(email_body=finished_email_body, email_subject_text_append="Finished")

    exit (application_monitor.error_code)


# Start the application!
application_thread()
