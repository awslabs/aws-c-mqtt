# Contains all of the classes that are shared across both the Canary Wrapper and the Persistent Canary Wrapper scripts
# If a class can/is reused, then it should be in this file.

# Needs to be installed prior to running
import boto3
# Part of standard packages in Python 3.4+
import time
import os

# TODO - split this into sub-files for easier management/modularity?
# TODO - write a fallback using OS if psutil is not present?
# TODO - create Cloudwatch dashboard(s)?

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

# ================================================================================

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

        # Setting initial values
        # ==================
        self.first_metric_call = True
        self.metrics = []
        self.metric_report_number = 0

        # Needed so we can initialize Cloudwatch alarms, etc, outside of the init function
        # but before we start sending data.
        # This boolean tracks whether we have done the post-initization prior to sending the first report.
        self.perform_final_initialization = True

        # Watched by the thread creating the snapshot. Will cause the thread to abort and return an error.
        self.abort_due_to_internal_error = False

        self.git_hash = None
        self.git_repo_name = None
        self.git_hash_as_namespace = git_hash_as_namespace
        self.git_fixed_namespace_text = git_fixed_namespace_text
        self.git_metric_namespace = None

        self.cloudwatch_region = cloudwatch_region
        self.cloudwatch_client = None

        self.s3_bucket_name = s3_bucket_name
        self.s3_client = None

        self.output_to_file_filepath = output_log_filepath
        self.output_to_file = False
        self.output_file = None
        self.output_to_console = output_to_console
        # ==================

        # Check for valid credentials
        # ==================
        tmp_sts_client = boto3.client('sts')
        try:
            tmp_sts_client.get_caller_identity()
        except Exception as e:
            print ("ERROR - AWS credentials are NOT valid!")
            self.abort_due_to_internal_error = True
            return
        # ==================

        # Git related stuff
        # ==================
        if (git_hash == None or git_repo_name == None):
            print("ERROR - a Git hash and repository name are REQUIRED for the canary wrapper to run!")
            self.abort_due_to_internal_error = True
            return

        self.git_hash = git_hash
        self.git_repo_name = git_repo_name

        if (self.git_hash_as_namespace == False):
            self.git_metric_namespace = self.git_fixed_namespace_text
        else:
            git_namespace_prepend_text = self.git_repo_name + "-" + self.git_hash
            self.git_metric_namespace = git_namespace_prepend_text
        # ==================

        # Cloudwatch related stuff
        # ==================
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
        if (not output_log_filepath is None):
            self.output_to_file = True
            self.output_file = open(self.output_to_file_filepath, "w")
        else:
            self.output_to_file = False
            self.output_file = None
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

    def output_diagnosis_information(self, dependencies_list):

        # Print general diagnosis information
        self.print_message("\n========== Canary Wrapper diagnosis information ==========")
        self.print_message("\nRunning Canary for repository: " + self.git_repo_name)
        self.print_message("\t Commit hash: " + self.git_hash)

        if not dependencies_list == "":
            self.print_message("\nDependencies:")
            dependencies_list = dependencies_list.split(";")
            dependencies_list_found_hash = False
            for i in range(0, len(dependencies_list)):
                # There's probably a better way to do this...
                if (dependencies_list_found_hash == True):
                    dependencies_list_found_hash = False
                    continue
                self.print_message("* " + dependencies_list[i])
                if (i+1 < len(dependencies_list)):
                    self.print_message("\t Commit hash: " + dependencies_list[i+1])
                    dependencies_list_found_hash = True
                else:
                    self.print_message("\t Commit hash: Unknown")

        self.print_message("\nMetrics:")
        for metric in self.metrics:
            self.print_message("* " + metric.metric_name)
            if metric.metric_alarm_threshold is not None:
                self.print_message("\t Alarm Threshold: " + str(metric.metric_alarm_threshold))
                self.print_message("\t Alarm Severity: " + str(metric.metric_alarm_severity))
            else:
                self.print_message("\t No alarm set for metric.")

        self.print_message("\n")
        self.print_message("==========================================================")
        self.print_message("\n")

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

# ================================================================================
