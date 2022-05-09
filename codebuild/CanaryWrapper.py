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

# TODO - split this into sub-files for easier management/modularity?
# TODO - write a fallback using OS if psutil is not present?
# TODO - find a way to check for expired credentials
# TODO - create Cloudwatch dashboard(s)?

# Code for metric collection, examination, exporting (cloudwatch, s3, etc)
# ================================================================================


# Class that holds metric data and has a few utility functions for getting that data in a format we can use for Cloudwatch
class DataSnapshot_Metric():
    def __init__(self, metric_name, metric_function, metric_dimensions=[],
                metric_unit="None", metric_alarm_threshold=None, metric_alarm_severity=6, git_hash="", reports_to_skip=0):
        self.metric_name = metric_name
        self.metric_function = metric_function
        self.metric_dimensions = metric_dimensions
        self.metric_unit = metric_unit
        self.metric_alarm_threshold = metric_alarm_threshold
        self.metric_alarm_name = self.metric_name + "-" + git_hash
        self.metric_alarm_description = 'Alarm for metric "' + self.metric_name + '" - git hash: ' + git_hash
        self.metric_value = None
        self.reports_to_skip = reports_to_skip
        self.metric_alarm_severity = metric_alarm_severity

    # Gets the latest metric value from the metric_function callback
    def get_metric_value(self):
        if not self.metric_function is None:
            self.metric_value = self.metric_function()
        return self.metric_value

    # Returns the data needed to send to Cloudwatch when posting metrics
    def get_metric_cloudwatch_dictionary(self):
        if (self.reports_to_skip > 0):
            self.reports_to_skip -= 1
            return None # skips sending to Cloudwatch

        return {
            "MetricName": self.metric_name,
            "Dimensions": self.metric_dimensions,
            "Value": self.metric_value,
            "Unit": self.metric_unit
        }


# Class that keeps track of the metrics registered, sets up Cloudwatch and S3, and sends periodic reports
# Is the backbone of the reporting operation
class DataSnapshot():
    def __init__(self,
                 git_hash=None,
                 git_repo_name=None,
                 git_hash_as_namespace=False,
                 git_fixed_namespace_text="mqtt5_canary",
                 output_log_filepath=None,
                 output_to_console=True,
                 cloudwatch_region="us-east-1",
                 s3_bucket_name="canary-wrapper-bucket"):

        self.first_metric_call = True
        self.metrics = []
        self.metric_report_number = 0

        # Needed so we can initialize Cloudwatch alarms, etc, outside of the init function
        # but before we start sending data.
        # This boolean tracks whether we have done the post-initization prior to sending the first report.
        self.perform_final_initialization = True

        # Watched by the thread creating the snapshot. Will cause the thread to abort and return an error.
        self.abort_due_to_internal_error = False

        # Git related stuff
        # ==================
        if (git_hash == None or git_repo_name == None):
            print("ERROR - a Git hash and repository name are REQUIRED for the canary wrapper to run!")
            self.abort_due_to_internal_error = True
            return

        self.git_hash = git_hash
        self.git_repo_name = git_repo_name
        self.git_hash_as_namespace = git_hash_as_namespace
        self.git_fixed_namespace_text = git_fixed_namespace_text

        if (self.git_hash_as_namespace == False):
            self.git_metric_namespace = self.git_fixed_namespace_text
        else:
            git_namespace_prepend_text = self.git_repo_name + "-" + self.git_hash
            self.git_metric_namespace = git_namespace_prepend_text
        # ==================

        # Cloudwatch related stuff
        # ==================
        self.cloudwatch_region = cloudwatch_region

        try:
            self.cloudwatch_client = boto3.client('cloudwatch', self.cloudwatch_region)
        except Exception as e:
            self.print_message("ERROR - could not make Cloudwatch client due to exception!")
            self.print_message("Exception: " + str(e))
            self.cloudwatch_client = None
            self.abort_due_to_internal_error = True
            return
        # ==================

        # S3 related stuff
        # ==================
        self.s3_bucket_name = s3_bucket_name
        try:
            self.s3_client = boto3.client("s3")
        except Exception as e:
            self.print_message("ERROR - could not make S3 client due to exception!")
            self.print_message("Exception: " + str(e))
            self.s3_client = None
            self.abort_due_to_internal_error = True
            return
        # ==================

        # File output (logs) related stuff
        # ==================
        self.output_to_file_filepath = output_log_filepath
        if (not output_log_filepath is None):
            self.output_to_file = True
            self.output_file = open(self.output_to_file_filepath, "w")
        else:
            self.output_to_file = False
            self.output_file = None

        self.output_to_console = output_to_console
        # ==================

    # Cleans the class - closing any files, removing alarms, and sending data to S3.
    # Should be called at the end when you are totally finished shadowing metrics
    def cleanup(self, error_occured=False):
        if (error_occured == False):
            self.export_result_to_s3_bucket(True)
        self._cleanup_cloudwatch_alarms()

        if (self.output_file is not None):
            self.output_file.close()
            self.output_file = None

    # Utility function for printing messages
    def print_message(self, message):
        if self.output_to_file == True:
            self.output_file.write(message + "\n")
        if self.output_to_console == True:
            print(message)

    # Utilty function - adds the metric alarms to Cloudwatch. We do run this right before the first
    # collection of metrics so we can register metrics before we initialize Cloudwatch
    def _init_cloudwatch_pre_first_run(self):
        for metric in self.metrics:
            if (not metric.metric_alarm_threshold is None):
                self._add_cloudwatch_metric_alarm(metric)

    # Utility function - The function that adds each individual metric alarm.
    def _add_cloudwatch_metric_alarm(self, metric):
        if self.cloudwatch_client is None:
            self.print_message(
                "ERROR - Cloudwatch client not setup. Cannot register alarm")
            return

        try:
            self.cloudwatch_client.put_metric_alarm(
                AlarmName=metric.metric_alarm_name,
                AlarmDescription=metric.metric_alarm_description,
                MetricName=metric.metric_name,
                Namespace=self.git_metric_namespace,
                Statistic="Maximum",
                Dimensions=metric.metric_dimensions,
                Period=60,  # How long (in seconds) is an evaluation period?
                EvaluationPeriods=120,  # How many periods does it need to be invalid for?
                DatapointsToAlarm=1,  # How many data points need to be invalid?
                Threshold=metric.metric_alarm_threshold,
                ComparisonOperator="GreaterThanOrEqualToThreshold",
            )
        except Exception as e:
            self.print_message(
                "ERROR - could not register alarm for metric due to exception: " + metric.metric_name)
            self.print_message("Exception: " + str(e))

    # Utilty function - removes all the Cloudwatch alarms for the metrics
    def _cleanup_cloudwatch_alarms(self):
        try:
            for metric in self.metrics:
                if (not metric.metric_alarm_threshold is None):
                    self.cloudwatch_client.delete_alarms(AlarmNames=[
                        metric.metric_alarm_name
                    ])
        except Exception as e:
            self.print_message(
                "ERROR - could not delete alarms due to exception!")
            self.print_message("Exception: " + str(e))
        return

    # Returns the results of the metric alarms. Will return a list containing tuples with the following structure:
    # [Boolean (False = the alarm is in the ALARM state), String (Name of the alarm that is in the ALARM state), int (severity of alarm)]
    # Currently this function will only return a list of failed alarms, so if the returned list is empty, then it means all
    # alarms did not get to the ALARM state in Cloudwatch for the registered metrics
    def get_cloudwatch_alarm_results(self):
        return self._check_cloudwatch_alarm_states()

    # Utility function - collects the metric alarm results and returns them in a list.
    def _check_cloudwatch_alarm_states(self):
        return_result_list = []

        tmp = None
        for metric in self.metrics:
            tmp = self._check_cloudwatch_alarm_state_metric(metric)
            if (tmp[0] != True):
                return_result_list.append(tmp)

        return return_result_list

    # Utility function - checks each individual alarm and returns a tuple with the following format:
    # [Boolean (False if the alarm is in the ALARM state, otherwise it is true), String (name of the alarm), Int (severity of alarm)]
    def _check_cloudwatch_alarm_state_metric(self, metric):
        alarms_response = self.cloudwatch_client.describe_alarms_for_metric(
            MetricName=metric.metric_name,
            Namespace=self.git_metric_namespace,
            Dimensions=metric.metric_dimensions)

        return_result = [True, None, metric.metric_alarm_severity]

        for metric_alarm_dict in alarms_response["MetricAlarms"]:
            if metric_alarm_dict["StateValue"] == "ALARM":
                return_result[0] = False
                return_result[1] = metric_alarm_dict["AlarmName"]
                break

        return return_result

    # Exports a file with the same name as the commit Git hash to an S3 bucket in a folder with the Git repo name.
    # By default, this file will only contain the Git hash.
    # If copy_output_log is true, then the output log will be copied into this file, which may be useful for debugging.
    def export_result_to_s3_bucket(self, copy_output_log=False):
        if (self.s3_client is None):
            self.print_message(
                "ERROR - No S3 client initialized! Cannot send log to S3")
            self.abort_due_to_internal_error = True
            return

        s3_file = open(self.git_hash + ".log", "w")
        s3_file.write(self.git_hash)

        # Might be useful for debugging?
        if (copy_output_log == True and self.output_to_file == True):
            # Are we still writing? If so, then we need to close the file first so everything is written to it
            is_output_file_open_previously = False
            if (self.output_file != None):
                self.output_file.close()
                is_output_file_open_previously = True
            self.output_file = open(self.output_to_file_filepath, "r")

            s3_file.write("\n\nOUTPUT LOG\n")
            s3_file.write(
                "==========================================================================================\n")
            output_file_lines = self.output_file.readlines()
            for line in output_file_lines:
                s3_file.write(line)

            self.output_file.close()

            # If we were writing to the output previously, then we need to open in RW mode so we can continue to write to it
            if (is_output_file_open_previously == True):
                self.output_to_file = open(self.output_to_file_filepath, "a")

        s3_file.close()

        # Upload to S3
        try:
            self.s3_client.upload_file(
                self.git_hash + ".log", self.s3_bucket_name, self.git_repo_name + "/" + self.git_hash + ".log")
            self.print_message("Uploaded to S3!")
        except Exception as e:
            self.print_message(
                "ERROR - could not upload to S3 due to exception!")
            self.print_message("Exception: " + str(e))
            self.abort_due_to_internal_error = True
            os.remove(self.git_hash + ".log")
            return

        # Delete the file when finished
        os.remove(self.git_hash + ".log")

    # Registers a metric to be polled by the Snapshot.
    # * (REQUIRED) new_metric_name is the name of the metric. Cloudwatch will use this name
    # * (REQUIRED) new_metric_function is expected to be a pointer to a Python function and will not work if you pass a value/object
    # * (OPTIONAL) new_metric_unit is the metric unit. There is a list of possible metric unit types on the Boto3 documentation for Cloudwatch
    # * (OPTIONAL) new_metric_alarm_threshold is the value that the metric has to exceed in order to be registered as an alarm
    # * (OPTIONAL) new_reports_to_skip is the number of reports this metric will return nothing, but will get it's value.
    #     * Useful for CPU calculations that require deltas
    # * (OPTIONAL) new_metric_alarm_severity is the severity of the ticket if this alarm is triggered. A severity of 6+ means no ticket.
    def register_metric(self, new_metric_name, new_metric_function, new_metric_unit="None",
                        new_metric_alarm_threshold=None, new_metric_reports_to_skip=0, new_metric_alarm_severity=6):

        new_metric_dimensions = []

        if (self.git_hash_as_namespace == False):
            git_namespace_prepend_text = self.git_repo_name + "-" + self.git_hash
            new_metric_dimensions.append(
                {"Name": git_namespace_prepend_text, "Value": new_metric_name})
        else:
            new_metric_dimensions.append(
                {"Name": "System_Metrics", "Value": new_metric_name})

        new_metric = DataSnapshot_Metric(
            metric_name=new_metric_name,
            metric_function=new_metric_function,
            metric_dimensions=new_metric_dimensions,
            metric_unit=new_metric_unit,
            metric_alarm_threshold=new_metric_alarm_threshold,
            metric_alarm_severity=new_metric_alarm_severity,
            git_hash=self.git_hash,
            reports_to_skip=new_metric_reports_to_skip
        )
        self.metrics.append(new_metric)

    # Prints the metrics to the console
    def export_metrics_console(self):
        self.print_message("Metric report: " + str(self.metric_report_number))
        for metric in self.metrics:
            self.print_message("    " + metric.metric_name +
                               " - value: " + str(metric.metric_value))

    # Sends all registered metrics to Cloudwatch.
    # Does NOT need to called on loop. Call post_metrics on loop to send all the metrics as expected.
    # This is just the Cloudwatch part of that loop.
    def export_metrics_cloudwatch(self):
        if (self.cloudwatch_client == None):
            self.print_message(
                "Error - cannot export Cloudwatch metrics! Cloudwatch was not initiallized.")
            self.abort_due_to_internal_error = True
            return

        self.print_message("Preparing to send to Cloudwatch...")
        metrics_data = []
        metric_data_tmp = None
        for metric in self.metrics:
            metric_data_tmp = metric.get_metric_cloudwatch_dictionary()
            if (not metric_data_tmp is None):
                metrics_data.append(metric_data_tmp)

        try:
            self.cloudwatch_client.put_metric_data(
                Namespace=self.git_metric_namespace,
                MetricData=metrics_data)
            self.print_message("Metrics sent to Cloudwatch.")
        except Exception as e:
            self.print_message(
                "Error - something when wrong posting cloudwatch metrics!")
            self.print_message("Exception: " + str(e))
            self.abort_due_to_internal_error = True
            return

    # Call this at a set interval to post the metrics to Cloudwatch, etc.
    # This is the function you want to call repeatedly after you have everything setup.
    def post_metrics(self):
        if (self.perform_final_initialization == True):
            self.perform_final_initialization = False

            # Make sure we have AWS system environment credentials. If we do not error out
            if not "AWS_ACCESS_KEY_ID" in os.environ or not "AWS_SECRET_ACCESS_KEY" in os.environ:
                self.print_message(
                    "ERROR - No AWS credentials found! Cannot post metrics")
                self.abort_due_to_internal_error = True
                return

            self._init_cloudwatch_pre_first_run()

        # Update the metric values internally
        for metric in self.metrics:
            metric.get_metric_value()
        self.metric_report_number += 1

        self.export_metrics_console()
        self.export_metrics_cloudwatch()


# Code for cutting a ticket
# ================================================================================

# Cuts a ticket to SIM using a temporary Cloudwatch metric that is quickly created, triggered, and destroyed.
# Can be called in any thread - creates its own Cloudwatch client and any data it needs is passed in.
#
# See (https://w.amazon.com/bin/view/CloudWatchAlarms/Internal/CloudWatchAlarmsSIMTicketing) for more details
# on how the alarm is sent using Cloudwatch.
def cut_ticket_using_cloudwatch(
    ticket_description="Description here!",
    ticket_reason="Reason here!",
    ticket_severity=5,
    ticket_category="AWS",
    ticket_type="SDKs and Tools",
    ticket_item="IoT SDK for CPP",
    ticket_group="AWS IoT Device SDK",
    ticket_allow_duplicates=False,
    git_repo_name="REPO NAME",
    git_hash="HASH",
    git_hash_as_namespace=False,
    git_fixed_namespace_text="mqtt5_canary",
    cloudwatch_region="us-east-1"):

    git_metric_namespace = ""
    if (git_hash_as_namespace == False):
        git_metric_namespace = git_fixed_namespace_text
    else:
        git_namespace_prepend_text = git_repo_name + "-" + git_hash
        git_metric_namespace = git_namespace_prepend_text

    cloudwatch_client = boto3.client('cloudwatch', cloudwatch_region)
    ticket_alarm_name = git_repo_name + "-" + git_hash + "-AUTO-TICKET"

    new_metric_dimensions = []
    if (git_hash_as_namespace == False):
        git_namespace_prepend_text = git_repo_name + "-" + git_hash
        new_metric_dimensions.append(
            {"Name": git_namespace_prepend_text, "Value": ticket_alarm_name})
    else:
        new_metric_dimensions.append(
            {"Name": "System_Metrics", "Value": ticket_alarm_name})

    ticket_arn = f"arn:aws:cloudwatch::cwa-internal:ticket:{ticket_severity}:{ticket_category}:{ticket_type}:{ticket_item}:{ticket_group}:"
    if (ticket_allow_duplicates == True):
        # use "DO-NOT-DEDUPE" so we can run the same commit again and it will cut another ticket.
        ticket_arn += "DO-NOT-DEDUPE"
    # In the ticket ARN, all spaces need to be replaced with +
    ticket_arn = ticket_arn.replace(" ", "+")

    ticket_alarm_description = f"AUTO CUT CANARY WRAPPER TICKET\n\nREASON: {ticket_reason}\n\nDESCRIPTION: {ticket_description}\n\n"

    # Regsiter a metric alarm so it can auto-cut a ticket for us
    cloudwatch_client.put_metric_alarm(
        AlarmName=ticket_alarm_name,
        AlarmDescription=ticket_alarm_description,
        MetricName=ticket_alarm_name,
        Namespace=git_metric_namespace,
        Statistic="Maximum",
        Dimensions=new_metric_dimensions,
        Period=60,  # How long (in seconds) is an evaluation period?
        EvaluationPeriods=1,  # How many periods does it need to be invalid for?
        DatapointsToAlarm=1,  # How many data points need to be invalid?
        Threshold=1,
        ComparisonOperator="GreaterThanOrEqualToThreshold",
        # The data above does not really matter - it just needs to be valid input data.
        # This is the part that tells Cloudwatch to cut the ticket
        AlarmActions=[ticket_arn]
    )

    # Trigger the alarm so it cuts the ticket
    try:
        cloudwatch_client.set_alarm_state(
            AlarmName=ticket_alarm_name,
            StateValue="ALARM",
            StateReason="AUTO TICKET CUT")
    except Exception as e:
        print ("ERROR - could not cut ticket due to exception!")
        print ("Exception: " + str(e))
        return

    print("Waiting for ticket metric to trigger...")
    # Wait a little bit (2 seconds)...
    time.sleep(2)

    # Remove the metric
    print("Removing ticket metric...")
    cloudwatch_client.delete_alarms(AlarmNames=[ticket_alarm_name])

    print ("Finished cutting ticket via Cloudwatch!")
    return

# A helper function that gets the majority of the ticket information from the arguments result from argparser.
def cut_ticket_using_cloudwatch_from_args(
    ticket_description="",
    ticket_reason="",
    ticket_severity=6,
    arguments=None):

    # Do not cut a ticket for a severity of 6+
    if (ticket_severity >= 6):
        return

    cut_ticket_using_cloudwatch(
        ticket_description=ticket_description,
        ticket_reason=ticket_reason,
        ticket_severity=ticket_severity,
        ticket_category=arguments.ticket_category,
        ticket_type=arguments.ticket_type,
        ticket_item=arguments.ticket_item,
        ticket_group=arguments.ticket_group,
        ticket_allow_duplicates=False,
        git_repo_name=arguments.git_repo_name,
        git_hash=arguments.git_hash,
        git_hash_as_namespace=arguments.git_hash_as_namespace)

    pass


# Code for getting metric data
# ================================================================================

def get_metric_total_cpu_usage():
    return psutil.cpu_percent(None, False)


def get_metric_total_memory_usage_value():
    memory_data = psutil.virtual_memory()
    return memory_data.total - memory_data.available


def get_metric_total_memory_usage_percent():
    memory_data = psutil.virtual_memory()
    metric_total_memory_usage_percent = memory_data.available * 100 / memory_data.total
    # Reduce precision to just 2 decimal places
    metric_total_memory_usage_percent = int(
        metric_total_memory_usage_percent * 100) / 100.0
    return metric_total_memory_usage_percent


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
command_parser.add_argument("--snapshot_wait_time", type=int, default=60,
    help="(OPTIONAL, default=60) The number of seconds between gathering and sending snapshot reports")
command_parser.add_argument("--ticket_category", type=str, default="AWS",
    help="(OPTIONAL, default=AWS) The category to register the ticket under")
command_parser.add_argument("--ticket_type", type=str, default="SDKs and Tools",
    help="(OPTIONAL, default='SDKs and Tools') The type to register the ticket under")
command_parser.add_argument("--ticket_item", type=str, default="IoT SDK for CPP",
    help="(OPTIONAL, default='IoT SDK for CPP') The item to register the ticket under")
command_parser.add_argument("--ticket_group", type=str, default="AWS IoT Device SDK",
    help="(OPTIONAL, default='AWS IoT Device SDK') The group to register the ticket under")
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

    # Register metrics
    data_snapshot.register_metric(
        new_metric_name="total_cpu_usage",
        new_metric_function=get_metric_total_cpu_usage,
        new_metric_unit="Percent",
        new_metric_alarm_threshold=50,
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
        new_metric_alarm_threshold=33,
        new_metric_reports_to_skip=0,
        new_metric_alarm_severity=6)

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
exit(1)
