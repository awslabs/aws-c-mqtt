# Python wrapper script for collecting Canary metrics, setting up alarms, reporting metrics to Cloudwatch,
# checking the alarms to ensure everything is correct at the end of the run, and checking for new
# builds in S3, downloading them, and launching them if they exist (24/7 opperation)
#
# Will only stop running if the Canary application itself has an issue - in which case it Canary application will
# need to be fixed and then the wrapper script restarted

# Needs to be installed prior to running
import boto3
import psutil
# Part of standard packages in Python 3.4+
import argparse
import threading
import subprocess
import time
import os
import subprocess
import json
# Dependencies in project folder
from CanaryWrapper_Classes import *
from CanaryWrapper_MetricFunctions import *

# TODO - Using subprocess may not work on Windows for starting/stopping the application thread.
#        Canary will likely be running on Linux, so it's probably okay, but need to confirm/check at some point....

# NOTE - There is a bug where you sometimes will have to try launching it a few times to clear exceptions if you
#        stopped execution using CTRL-C instead of pressing enter. TODO - figure out what causes this and fix it.

# TODO - better support running something like 'python3' as the executable and '<path to python file>' as the argument(s)
#        currently running 'python3' and setting the arguments works, but it does not place the S3 file in the arguments
#        filepath and instead places it in a file called python3, which is NOT what we want. Need to think on how to best
#        support/workaround this...

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
    help="(OPTIONAL, default=canary-wrapper-folder) The name of the S3 bucket where success logs will be stored")
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

# How long (in seconds) to wait before checking S3
# [THIS IS READ ONLY]
canary_s3_check_wait_time = 300 # 5 minutes
# How long (in seconds) to wait before gathering metrics and pushing them to Cloudwatch
# [THIS IS READ ONLY]
canary_metrics_wait_time = 600 # 10 minutes
# How long (in seconds) to run the Application thread loop. Should be around/shorter than the Canary S3 check time
canary_application_loop_wait_time = 300 # 5 minutes

# If true, the S3 thread will stop. Should only be set by the application thread
# [THIS IS WRITTEN TO ONLY FROM THE APPLICATION THREAD]
canary_s3_thread_stop = False
# A variable for the application thread to check if the S3 thread has finished
# [THIS IS WRITTEN TO ONLY FROM THE S3 THREAD]
canary_s3_thread_has_stopped = False

# Tell the application thread to finish and get ready to restart. The application thread is expected
# to set canary_file_replace_application_thread_ready to true when it has stopped the current application
# and is ready for the new application
# [THIS IS WRITTEN TO ONLY FROM THE S3 THREAD]
canary_file_replace_application_thread_start = False
# Tell the application thread to restart the canary application again. This needs to be set to true
# after the S3 thread has fully replaced the canary application with the new one from S3
# [THIS IS WRITTEN TO ONLY FROM THE S3 THREAD]
canary_file_reaplce_application_thread_restart = False
# Tells the S3 thread that the application thread has finished stopping the application monitor, so the
# S3 thread knows it is safe to replace the application file.
# [THIS IS WRITTEN TO ONLY FROM THE APPLICATION THREAD]
canary_file_replace_application_thread_ready = False

# A way to stop both threads. Primarily used for debugging purposes
# [THIS IS READ ONLY (except for where we set it in the main process...)]
canary_stop_all_threads = False

# ================================================================================
class S3_Monitor():
    global canary_local_application_path

    def __init__(self, s3_bucket_name, s3_file_name) -> None:
        self.s3_client = None
        self.s3_current_object_version_id = None
        self.s3_current_object_last_modified = None
        self.s3_bucket_name = s3_bucket_name
        self.s3_file_name = s3_file_name
        self.s3_file_name_only_path, self.s3_file_name_only_extension = os.path.splitext(s3_file_name)

        self.s3_file_needs_replacing = False

        self.had_interal_error = False
        self.internal_error_reason = ""

        # Check for valid credentials
        # ==================
        try:
            tmp_sts_client = boto3.client('sts')
            tmp_sts_client.get_caller_identity()
        except Exception as e:
            print ("ERROR - (S3 Check) AWS credentials are NOT valid!")
            self.had_interal_error = True
            self.internal_error_reason = "AWS credentials are NOT valid!"
            return
        # ==================

        try:
            self.s3_client = boto3.client("s3")
        except Exception as e:
            print ("ERROR - (S3 Check) Could not make S3 client")
            self.had_interal_error = True
            self.internal_error_reason = "Could not make S3 client for S3 Monitor"
            return


    def check_for_file_change(self):
        try:
            version_check_response = self.s3_client.list_object_versions(
                Bucket=self.s3_bucket_name,
                Prefix=self.s3_file_name_only_path)
            if "Versions" in version_check_response:
                for version in version_check_response["Versions"]:
                    if (version["IsLatest"] == True):
                        if (version["VersionId"] != self.s3_current_object_version_id or
                            version["LastModified"] != self.s3_current_object_last_modified):

                            print ("S3 monitor - Found new version of Canary/Application in S3!")
                            print ("Changing running Canary/Application to new one...")

                            # Will be checked by thread to trigger replacing the file
                            self.s3_file_needs_replacing = True

                            self.s3_current_object_version_id = version["VersionId"]
                            self.s3_current_object_last_modified = version["LastModified"]
                            return

        except Exception as e:
            print ("ERROR - Could not check for new version of file in S3 due to exception!")
            print ("Exception: " + str(e))
            self.had_interal_error = True
            self.internal_error_reason = "Could not check for S3 file due to exception in S3 client"


    def replace_current_file_for_new_file(self):
        try:
            print ("Making directory...")
            if not os.path.exists("tmp"):
                os.makedirs("tmp")
        except Exception as e:
            print ("ERROR - could not make tmp directory to place S3 file into!")
            self.had_interal_error = True
            self.internal_error_reason = "Could not make TMP folder for S3 file download"
            return

        # Download the file
        try:
            print ("Downloading file...")
            s3_resource = boto3.resource("s3")
            s3_resource.meta.client.download_file(self.s3_bucket_name, self.s3_file_name, "tmp/new_file" + self.s3_file_name_only_extension)
        except Exception as e:
            print ("ERROR - could not download latest S3 file into TMP folder!")
            self.had_interal_error = True
            self.internal_error_reason = "Could not download latest S3 file into TMP folder"
            return

        try:
            print ("Moving file...")
            os.replace("tmp/new_file" + self.s3_file_name_only_extension, canary_local_application_path)
        except Exception as e:
            print ("ERROR - could not move file into local application path!")
            self.had_interal_error = True
            self.internal_error_reason = "Could not move file into local application path"
            return

        print ("New file downloaded and moved into correct location!")
        self.s3_file_needs_replacing = False


def s3_monitor_thread():
    global canary_s3_check_wait_time
    global canary_file_replace_application_thread_start
    global canary_file_replace_application_thread_ready
    global canary_file_reaplce_application_thread_restart
    global canary_s3_thread_stop
    global canary_s3_thread_has_stopped
    global canary_stop_all_threads

    s3_monitor = S3_Monitor(
        s3_bucket_name=canary_s3_bucket_name,
        s3_file_name=canary_s3_bucket_application_path)

    while True:
        if (canary_s3_thread_stop == True or canary_stop_all_threads == True):
            break
        if (s3_monitor.had_interal_error == True):
            print ("Stopping S3 monitor thread. S3 monitor had internal error")
            print ("Error reason: " + s3_monitor.internal_error_reason)
            break

        s3_monitor.check_for_file_change()

        if (s3_monitor.s3_file_needs_replacing == True):

            # Tell the application thread to stop the application and wait for it to restart
            canary_file_replace_application_thread_start = True
            while canary_file_replace_application_thread_ready == False:
                # Check thread stopping variables just to make sure we're not in an endless loop if something goes wrong
                if (canary_s3_thread_stop == True or canary_stop_all_threads == True):
                    break
                time.sleep(1)

            s3_monitor.replace_current_file_for_new_file()

            # Tell the application thread to start up again
            canary_file_reaplce_application_thread_restart = True

        time.sleep(canary_s3_check_wait_time)

    canary_s3_thread_has_stopped = True
    exit(0)

# ================================================================================

class SnapshotMonitor():
    global canary_local_git_hash_stub
    global canary_local_git_repo_stub
    global canary_local_git_fixed_namespace
    global canary_metrics_wait_time

    def __init__(self) -> None:

        self.data_snapshot = DataSnapshot(
            git_hash=canary_local_git_hash_stub,
            git_repo_name=canary_local_git_repo_stub,
            git_hash_as_namespace=False,
            git_fixed_namespace_text=canary_local_git_fixed_namespace,
            output_log_filepath="output.log",
            output_to_console=True,
            cloudwatch_region="us-east-1",
            cloudwatch_teardown_alarms_on_complete=True,
            cloudwatch_teardown_dashboard_on_complete=False,
            cloudwatch_make_dashboard=True,
            s3_bucket_name="", # We will not be uploading to S3 anyway, so just pass an empty string
            s3_bucket_upload_on_complete=False)

        self.had_interal_error = False
        self.internal_error_reason = ""

        # A list of all the alarms triggered in the last check, cached for later
        # NOTE - this is only the alarm names! Not the severity. This just makes it easier to process
        self.cloudwatch_current_alarms_triggered = []

        # Check for errors
        if (self.data_snapshot.abort_due_to_internal_error == True):
            self.had_interal_error = True
            self.internal_error_reason = "Could not initialize DataSnapshot. Likely credentials are not setup!"
            self.data_snapshot.cleanup()
            return

        # How long to wait before posting a metric
        self.metric_post_timer = canary_metrics_wait_time
        self.metric_post_timer_time = canary_metrics_wait_time


    def register_metric(self, new_metric_name, new_metric_function, new_metric_unit="None", new_metric_alarm_threshold=None,
                        new_metric_reports_to_skip=0, new_metric_alarm_severity=6):

        try:
            self.data_snapshot.register_metric(
                new_metric_name=new_metric_name,
                new_metric_function=new_metric_function,
                new_metric_unit=new_metric_unit,
                new_metric_alarm_threshold=new_metric_alarm_threshold,
                new_metric_reports_to_skip=new_metric_reports_to_skip,
                new_metric_alarm_severity=new_metric_alarm_severity)
        except Exception as e:
            print ("ERROR - could not register metric in data snapshot due to exception!")
            print ("Exception: " + str(e))
            self.had_interal_error = True
            self.internal_error_reason = "Could not register metric in data snapshot due to exception"
            return

    def register_dashboard_widget(self, new_widget_name, metrics_to_add=[]):
        self.data_snapshot.register_dashboard_widget(new_widget_name=new_widget_name, metrics_to_add=metrics_to_add)

    def output_diagnosis_information(self, dependencies=""):
        self.data_snapshot.output_diagnosis_information(dependencies_list=dependencies)


    def cleanup(self):
        self.data_snapshot.cleanup()

    def check_alarms_for_new_alarms(self, triggered_alarms):

        if len(triggered_alarms) > 0:
            self.data_snapshot.print_message(
                "WARNING - One or more alarms are in state of ALARM")

            old_alarms_still_active = []
            new_alarms = []
            new_alarms_highest_severity = 6
            new_alarm_found = True
            new_alarm_ticket_description = "24_7 Canary has metrics in ALARM state!\n\nMetrics in alarm:\n"

            for triggered_alarm in triggered_alarms:
                new_alarm_found = True

                # Is this a new alarm?
                for old_alarm_name in self.cloudwatch_current_alarms_triggered:
                    if (old_alarm_name == triggered_alarm[1]):
                        new_alarm_found = False
                        old_alarms_still_active.append(triggered_alarm[1])

                        new_alarm_ticket_description += "* (STILL IN ALARM) " + triggered_alarm[1] + "\n"
                        new_alarm_ticket_description += "\tSeverity: " + str(triggered_alarm[2])
                        new_alarm_ticket_description += "\n"
                        break

                # If it is a new alarm, then add it to our list so we can cut a new ticket
                if (new_alarm_found == True):
                    self.data_snapshot.print_message('    (NEW) Alarm with name "' + triggered_alarm[1] + '" is in the ALARM state!')
                    new_alarms.append(triggered_alarm[1])
                    if (triggered_alarm[2] < new_alarms_highest_severity):
                        new_alarms_highest_severity = triggered_alarm[2]
                    new_alarm_ticket_description += "* " + triggered_alarm[1] + "\n"
                    new_alarm_ticket_description += "\tSeverity: " + str(triggered_alarm[2])
                    new_alarm_ticket_description += "\n"


            if len(new_alarms) > 0:
                cut_ticket_using_cloudwatch(
                    git_repo_name=canary_local_git_repo_stub,
                    git_hash=canary_local_git_hash_stub,
                    git_hash_as_namespace=False,
                    git_fixed_namespace_text=canary_local_git_fixed_namespace,
                    cloudwatch_region="us-east-1",
                    ticket_description="Long running canary application thread stopped due to the S3 checking thread stopping unexpectedly. "
                                        "This is likely due to a credential error or setup error",
                    ticket_reason="S3 thread stopped unexpectedly",
                    ticket_allow_duplicates=True,
                    ticket_category="AWS",
                    ticket_item="IoT SDK for CPP",
                    ticket_group="AWS IoT Device SDK",
                    ticket_type="SDKs and Tools",
                    ticket_severity=4)

            # Cache the new alarms and the old alarms
            self.cloudwatch_current_alarms_triggered = old_alarms_still_active + new_alarms

        else:
            self.cloudwatch_current_alarms_triggered.clear()


    def application_monitor_loop_function(self, time_passed=30):
        # Check for internal errors
        if (self.data_snapshot.abort_due_to_internal_error == True):
            self.had_interal_error = True
            self.internal_error_reason = "Data Snapshot internal error: " + self.data_snapshot.abort_due_to_internal_error_reason
            return

        # Gather and post the metrics
        self.metric_post_timer -= time_passed
        if (self.metric_post_timer <= 0):
            if (self.had_interal_error == False):
                try:
                    self.data_snapshot.post_metrics()
                except:
                    self.data_snapshot.print_message("ERROR - exception occured posting metrics!")
                    self.data_snapshot.print_message("(Likely session credentials expired)")

                    self.had_interal_error = True
                    self.internal_error_reason = "Exception occured posting metrics! Likely session credentials expired"
                    return

                try:
                    # Poll the metric alarms
                    if (self.had_interal_error == False):
                        # Get a report of all the alarms that might have been set to an alarm state
                        triggered_alarms = self.data_snapshot.get_cloudwatch_alarm_results()
                        self.check_alarms_for_new_alarms(triggered_alarms)
                except:
                    self.data_snapshot.print_message("ERROR - exception occured checking metric alarms!")
                    self.data_snapshot.print_message("(Likely session credentials expired)")

                    self.had_interal_error = True
                    self.internal_error_reason = "Exception occured checking metric alarms! Likely session credentials expired"
                    return

            # reset the timer
            self.metric_post_timer += self.metric_post_timer_time

# ================================================================================

class ApplicationMonitor():
    global canary_local_application_path

    def __init__(self) -> None:

        self.wrapper_monitor = SnapshotMonitor()
        self.application_process = None
        self.error_has_occured = False
        self.error_reason = ""
        self.error_code = 0

        if (self.wrapper_monitor.had_interal_error == True):
            self.error_has_occured = True
            self.error_reason = "Snapshot monitor had exception on creating!"
            self.error_code = 1
            return

        # Register metrics
        self.wrapper_monitor.register_metric(
            new_metric_name="total_cpu_usage",
            new_metric_function=get_metric_total_cpu_usage,
            new_metric_unit="Percent",
            new_metric_alarm_threshold=70,
            new_metric_reports_to_skip=1,
            new_metric_alarm_severity=5)
        self.wrapper_monitor.register_metric(
            new_metric_name="total_memory_usage_value",
            new_metric_function=get_metric_total_memory_usage_value,
            new_metric_unit="Bytes")
        self.wrapper_monitor.register_metric(
            new_metric_name="total_memory_usage_percent",
            new_metric_function=get_metric_total_memory_usage_percent,
            new_metric_unit="Percent",
            new_metric_alarm_threshold=50,
            new_metric_reports_to_skip=0,
            new_metric_alarm_severity=5)

        self.wrapper_monitor.register_dashboard_widget("System Percentages", ["total_cpu_usage", "total_memory_usage_percent"])

        # No good way to get the dependencies since it could change at any given point. Just skip printing them for the 24_7 canary
        self.wrapper_monitor.output_diagnosis_information("Cannot show dependencies in 24_7 wrapper")


    def start_application_monitoring(self):
        if (self.application_process == None):
            try:
                canary_command = canary_local_application_path + " " + canary_local_application_arguments
                self.application_process = subprocess.Popen(canary_command, shell=True)
            except Exception as e:
                print ("ERROR - Could not launch Canary/Application due to exception!")
                print ("Exception: " + str(e))
                self.error_has_occured = True
                self.error_reason = "Could not launch Canary/Application due to exception"
                self.error_code = 1
                return

    def restart_application_monitoring(self):
        if (self.application_process != None):
            try:
                self.stop_application_monitoring()
                self.start_application_monitoring()
            except Exception as e:
                print ("ERROR - Could not restart Canary/Application due to exception!")
                print ("Exception: " + str(e))
                self.error_has_occured = True
                self.error_reason = "Could not restart Canary/Application due to exception"
                self.error_code = 1
                return
        else:
            print ("ERROR - Application process restart called but process is/was not running!")
            self.error_has_occured = True
            self.error_reason = "Could not restart Canary/Application due to application process not being started initially"
            self.error_code = 1
            return


    def stop_application_monitoring(self):
        if (not self.application_process == None):
            self.application_process.terminate()
            self.application_process.wait()
            self.application_process = None


    def application_monitor_loop_function(self, time_passed=30):
        if (self.application_process != None):

            application_process_return_code = None
            try:
                application_process_return_code = self.application_process.poll()
            except Exception as e:
                print ("ERROR - exception occured while trying to poll application status!")
                print ("Exception: " + str(e))
                self.error_has_occured = True
                self.error_reason = "Exception when polling application status"
                self.error_code = 1
                return

            # If it is not none, then the application finished
            if (application_process_return_code != None):

                if (application_process_return_code != 0):
                    print ("ERROR - Something Crashed in Canary/Application!")
                    print ("Error code: " + str(application_process_return_code))

                    self.error_has_occured = True
                    self.error_reason = "Canary application crashed!"
                    self.error_code = application_process_return_code
                else:
                    # Restar the canary
                    print ("NOTE - Canary finished running and is restarting...")
                    self.restart_application_monitoring()

        if (self.wrapper_monitor != None):
            if (self.wrapper_monitor.had_interal_error == True):
                self.error_has_occured = True
                self.error_reason = self.wrapper_monitor.internal_error_reason
                self.error_code = 1
            else:
                self.wrapper_monitor.application_monitor_loop_function(time_passed)


    def cleanup_all(self):
        self.wrapper_monitor.cleanup()



def application_thread():
    global canary_file_replace_application_thread_start
    global canary_file_replace_application_thread_ready
    global canary_file_reaplce_application_thread_restart
    global canary_s3_thread_stop
    global canary_s3_thread_has_stopped
    global canary_stop_all_threads
    global canary_local_git_hash_stub
    global canary_local_git_repo_stub
    global canary_local_git_fixed_namespace
    global canary_application_loop_wait_time

    application_monitor = ApplicationMonitor()

    while True:

        # Stop and restart the canary application
        if (canary_file_replace_application_thread_start == True):
            application_monitor.stop_application_monitoring()

            # Tell the S3 thread we stopped and wait for it to tell us to start again
            canary_file_replace_application_thread_ready = True
            while (canary_file_reaplce_application_thread_restart == False):
                # Check for loop breakers just to ensure we do not get stuck in an endless loop
                if (application_monitor.error_has_occured == True or canary_s3_thread_has_stopped == True or
                    canary_stop_all_threads == True):
                    break
                time.sleep(1)

            application_monitor.start_application_monitoring()

            # Reset the variables
            canary_file_replace_application_thread_start = False
            canary_file_replace_application_thread_ready = False
            canary_file_reaplce_application_thread_restart = False
            continue

        application_monitor.application_monitor_loop_function(canary_application_loop_wait_time)

        # If an error has occured or otherwise this thead needs to stop, then break the loop
        if (application_monitor.error_has_occured == True or canary_s3_thread_has_stopped == True or canary_stop_all_threads == True):
            break

        time.sleep(canary_application_loop_wait_time)

    # Stop the application from running if it is somewhow still running in the background
    application_monitor.stop_application_monitoring()

    if (canary_stop_all_threads == True):
        print ("DEBUG - Application thread stopped due to all thread stop signal sent...")
    elif (canary_s3_thread_has_stopped == True):
        print ("Application thread stopped due to S3 thread stopping unexpectedly!")

        cut_ticket_using_cloudwatch(
            git_repo_name=canary_local_git_repo_stub,
            git_hash=canary_local_git_hash_stub,
            git_hash_as_namespace=False,
            git_fixed_namespace_text=canary_local_git_fixed_namespace,
            cloudwatch_region="us-east-1",
            ticket_description="Long running canary application thread stopped due to the S3 checking thread stopping unexpectedly. "
                                "This is likely due to a credential error or setup error",
            ticket_reason="S3 thread stopped unexpectedly",
            ticket_allow_duplicates=True,
            ticket_category="AWS",
            ticket_item="IoT SDK for CPP",
            ticket_group="AWS IoT Device SDK",
            ticket_type="SDKs and Tools",
            ticket_severity=4)
    else:
        print ("Application thread stopping due to an internal error in application monitor.")
        print ("Error reason: " + application_monitor.error_reason)
        print ("Error code: " + str(application_monitor.error_code))

        if (application_monitor.error_code != 0):
            cut_ticket_using_cloudwatch(
                git_repo_name=canary_local_git_repo_stub,
                git_hash=canary_local_git_hash_stub,
                git_hash_as_namespace=False,
                git_fixed_namespace_text=canary_local_git_fixed_namespace,
                cloudwatch_region="us-east-1",
                ticket_description="The 24_7 canary exited with a non-zero exit code! This likely means something in the canary failed.",
                ticket_reason="The 24_7 canary exited with a non-zero exit code",
                ticket_allow_duplicates=True,
                ticket_category="AWS",
                ticket_item="IoT SDK for CPP",
                ticket_group="AWS IoT Device SDK",
                ticket_type="SDKs and Tools",
                ticket_severity=4)
        else:
            cut_ticket_using_cloudwatch(
                git_repo_name=canary_local_git_repo_stub,
                git_hash=canary_local_git_hash_stub,
                git_hash_as_namespace=False,
                git_fixed_namespace_text=canary_local_git_fixed_namespace,
                cloudwatch_region="us-east-1",
                ticket_description="The 24_7 canary exited with a zero exit code. The canary should run 24/7 and may need to be restarted.",
                ticket_reason="The 24_7 canary exited with a zero exit code",
                ticket_allow_duplicates=True,
                ticket_category="AWS",
                ticket_item="IoT SDK for CPP",
                ticket_group="AWS IoT Device SDK",
                ticket_type="SDKs and Tools",
                ticket_severity=5)

    application_monitor.cleanup_all()

    print ("Shutting down S3 check...")
    canary_s3_thread_stop = True
    while canary_s3_thread_has_stopped == False:
        time.sleep(1)

    exit (application_monitor.error_code)

# ================================================================================

# Create the threads
run_thread_s3_monitor = threading.Thread(target=s3_monitor_thread)
run_thread_application = threading.Thread(target=application_thread)
# Run the threads
run_thread_s3_monitor.start()
run_thread_application.start()

# FOR DEBUGGING ONLY
# A way to cleanly stop all the processes for debugging. (Wait a few seconds so we see the message)
input("\n\nDEBUG ONLY -- Press enter to stop program...\n\n")
canary_stop_all_threads = True

# Wait for threads to finish
run_thread_s3_monitor.join()
run_thread_application.join()
exit(0)
