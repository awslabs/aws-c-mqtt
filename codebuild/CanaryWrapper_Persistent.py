# Python wrapper script for collecting Canary metrics, setting up alarms, reporting metrics to Cloudwatch,
# checking the alarms to ensure everything is correct at the end of the run, and checking for new
# builds in S3, downloading them, and launching them if they exist (24/7 opperation)

# Needs to be installed prior to running
from sys import prefix
import boto3
import psutil
# Part of standard packages in Python 3.4+
import argparse
import threading
import subprocess
import time
import os
import subprocess
# Dependencies in project folder
from CanaryWrapper_Classes import *
from CanaryWrapper_MetricFunctions import *

# TODO - Refactor this entire file! It is very messy currently, even if it (mostly) works
# TODO - Add better error checking!
#        Right now the code does not use proper error checking to ensure segfaults/exceptions do not occur
# TODO - Use a persistent cloudwatch namespace/alarm/etc - rather than creating and tearing it down every time
# TODO - Fix bug where sometimes you have to just try launching it multiple times for it to work...

# ================================================================================
# Global variables that both threads use to communicate.
# NOTE - These should likely be replaced with futures or similar for better thread safety.
#        However, these variables are only either read or written to from a single thread, no
#        thread should read and write to these variables.

# NOTE 2 - this needs to be better sorted/defined/handled, as right now it's a bit of a mess

# S3 checking variables (only used by S3)
canary_local_application_path = "tmp/canary_application.py"
canary_s3_check_wait_time = 30
# If true, the S3 thread will stop. Should only be set by the application thread
canary_s3_thread_stop = False
canary_s3_thread_has_stopped = False

# Tell the application thread to finish and get ready to restart.
# # Will set canary_file_replace_application_thread_ready when it is ready to replace
canary_file_replace_application_thread_start = False
# Tell the application thread to restart the canary application again
canary_file_reaplce_application_thread_restart = False
# Communication between threads to know when it is okay to replace the file
canary_file_replace_application_thread_ready = False

# A way to stop both threads
canary_stop_all_threads = False

# ================================================================================
class S3_Monitor():
    global canary_local_application_path

    def __init__(self, s3_bucket_name, s3_file_name) -> None:
        self.s3_client = boto3.client("s3")
        self.s3_current_object_version_id = None
        self.s3_current_object_last_modified = None
        self.s3_bucket_name = s3_bucket_name
        self.s3_file_name = s3_file_name
        self.s3_file_name_only_path, self.s3_file_name_only_extension = os.path.splitext(s3_file_name)

        self.s3_file_needs_replacing = False


    def check_for_file_change(self):

        version_check_response = self.s3_client.list_object_versions(
            Bucket=self.s3_bucket_name,
            Prefix=self.s3_file_name_only_path)
        if "Versions" in version_check_response:
            for version in version_check_response["Versions"]:
                if (version["IsLatest"] == True):
                    if (version["VersionId"] != self.s3_current_object_version_id or
                        version["LastModified"] != self.s3_current_object_last_modified):

                        print ("FOUND NEW VERSION!")

                        # Will be checked by thread to trigger replacing the file
                        self.s3_file_needs_replacing = True

                        self.s3_current_object_version_id = version["VersionId"]
                        self.s3_current_object_last_modified = version["LastModified"]
                        break


    def replace_current_file_for_new_file(self):
        print ("Making directory...")
        if not os.path.exists("tmp"):
            os.makedirs("tmp")

        # Download the file...
        s3_resource = boto3.resource("s3")
        print ("Downloading file...")
        s3_resource.meta.client.download_file(self.s3_bucket_name, self.s3_file_name, "tmp/new_file" + self.s3_file_name_only_extension)

        print ("Moving file...")
        os.replace("tmp/new_file" + self.s3_file_name_only_extension, canary_local_application_path)

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
        s3_bucket_name="ncbeard-canary-wrapper-folder",
        s3_file_name="canary-application/CanaryMockApplication.py"
    )

    while True:
        if (canary_s3_thread_stop == True):
            break
        if (canary_stop_all_threads == True):
            break

        s3_monitor.check_for_file_change()

        if (s3_monitor.s3_file_needs_replacing == True):

            # Tell the application thread to stop the application and wait for it to restart
            canary_file_replace_application_thread_start = True
            while canary_file_replace_application_thread_ready == False:
                time.sleep(1)

            s3_monitor.replace_current_file_for_new_file()

            # Tell the application thread to start up again
            canary_file_reaplce_application_thread_restart = True

        time.sleep(canary_s3_check_wait_time)

    canary_s3_thread_has_stopped = True
    exit(0)

# ================================================================================

class SnapshotMonitor():
    def __init__(self) -> None:

        # TODO - replace this so the values are NOT hard coded
        self.data_snapshot = DataSnapshot(
            git_hash="1234567890",
            git_repo_name="aws-c-example",
            git_hash_as_namespace=False,
            output_log_filepath="output.log",
            output_to_console=True,
            cloudwatch_region="us-east-1",
            s3_bucket_name="ncbeard-canary-wrapper-folder")

        self.had_interal_error = False
        self.internal_error_reason = ""

        self.cloudwatch_current_alarms_triggered = []
        self.cloudwatch_current_alarms_lowest_alarm_severity = 6

        # Check for errors
        if (self.data_snapshot.abort_due_to_internal_error == True):
            self.had_interal_error = True
            self.internal_error_reason = "Could not initialize DataSnapshot. Likely credentials are not setup!"
            self.data_snapshot.cleanup()
            return

        # How long to wait before posting a metric
        self.metric_post_timer = 60
        self.metric_post_timer_time = 60

         # Print general diagnosis information
         # TODO - add dependencies here!
        self.data_snapshot.output_diagnosis_information("")

        # TODO - add a better way to register metrics?

    def cleanup(self):
        self.data_snapshot.cleanup()

    def application_monitor_loop_function(self, time_passed=30):
        # Check for internal errors
        if (self.data_snapshot.abort_due_to_internal_error == True):
            self.had_interal_error = True
            self.internal_error_reason = "Data Snapshot internal error!"
            # TODO - add a way to get the error reason from the data snapshot
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
                        if len(triggered_alarms) > 0:
                            # TODO - instead of clearing instantly, append to new array and check to see if there is a difference.
                            # If there is, potentially cut a ticket
                            self.cloudwatch_current_alarms_triggered.clear()

                            self.data_snapshot.print_message(
                                "WARNING - One or more alarms are in state of ALARM")
                            for triggered_alarm in triggered_alarms:
                                self.data_snapshot.print_message('    Alarm with name "' + triggered_alarm[1] + '" is in the ALARM state!')
                                self.cloudwatch_current_alarms_triggered.append(triggered_alarm[1])
                                if (triggered_alarm[2] < self.cloudwatch_current_alarms_lowest_alarm_severity):
                                    self.cloudwatch_current_alarms_lowest_alarm_severity = triggered_alarm[2]

                            # TODO - cut a ticket if the states in alarm are different than cached states in alarm?
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

        # Register metrics
        self.wrapper_monitor.data_snapshot.register_metric(
            new_metric_name="total_cpu_usage",
            new_metric_function=get_metric_total_cpu_usage,
            new_metric_unit="Percent",
            new_metric_alarm_threshold=70,
            new_metric_reports_to_skip=1,
            new_metric_alarm_severity=5)
        self.wrapper_monitor.data_snapshot.register_metric(
            new_metric_name="total_memory_usage_value",
            new_metric_function=get_metric_total_memory_usage_value,
            new_metric_unit="Bytes")
        self.wrapper_monitor.data_snapshot.register_metric(
            new_metric_name="total_memory_usage_percent",
            new_metric_function=get_metric_total_memory_usage_percent,
            new_metric_unit="Percent",
            new_metric_alarm_threshold=50,
            new_metric_reports_to_skip=0,
            new_metric_alarm_severity=5)

        self.application_process = None

        self.error_has_occured = False
        self.error_reason = ""
        self.error_code = 0
        pass


    def start_application_monitoring(self):
        if (self.application_process == None):
            canary_command = "python3 " + canary_local_application_path
            print ("\n\nAPPLICATION COMMAND: " + canary_command + "\n\n")
            self.application_process = subprocess.Popen(canary_command, shell=True)


    def stop_application_monitoring(self):
        if (not self.application_process == None):
            self.application_process.terminate()
            self.application_process.wait()
            self.application_process = None


    def application_monitor_loop_function(self, time_passed=30):
        if (self.application_process != None):
            application_process_return_code = self.application_process.poll()
            if (application_process_return_code != None):
                print ("SOMETHING CRASHED IN CANARY!")
                print ("Got error code: " + str(application_process_return_code))
                # TODO - store why the error occured
                # TODO - exit everything

                self.error_has_occured = True
                self.error_reason = "Canary application crashed!"
                self.error_code = application_process_return_code

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

    application_monitor = ApplicationMonitor()

    while True:

        # Stop and restart the canary application
        if (canary_file_replace_application_thread_start == True):
            application_monitor.stop_application_monitoring()
            canary_file_replace_application_thread_ready = True
            while (canary_file_reaplce_application_thread_restart == False):
                time.sleep(1)
            application_monitor.start_application_monitoring()

            # Reset the variables
            canary_file_replace_application_thread_start = False
            canary_file_replace_application_thread_ready = False
            canary_file_reaplce_application_thread_restart = False
            continue

        application_monitor.application_monitor_loop_function()

        if (application_monitor.error_has_occured == True):
            break

        if (canary_s3_thread_has_stopped == True):
            break

        if (canary_stop_all_threads == True):
            break

        time.sleep(10)

    # TODO - cut a ticket
    if (canary_stop_all_threads == True):
        print ("DEBUG - Application thread stopped due to all thread stop signal sent...")
    elif (canary_s3_thread_has_stopped == True):
        print ("Application thread stopped due to S3 thread stopping unexpectedly!")
    else:
        print ("Application thread stopping due to an internal error in application monitor.")
        print ("Error reason: " + application_monitor.error_reason)
        print ("Error code: " + str(application_monitor.error_code))

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

# A way to cleanly stop all the processes for debugging...
input("\n\nDEBUG ONLY -- Press enter to stop program...\n\n")
canary_stop_all_threads = True

# Wait for threads to finish
run_thread_s3_monitor.join()
run_thread_application.join()
exit(0)
