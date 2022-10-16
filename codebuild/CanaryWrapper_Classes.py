# Contains all of the classes that are shared across both the Canary Wrapper and the Persistent Canary Wrapper scripts
# If a class can/is reused, then it should be in this file.

# Needs to be installed prior to running
import boto3
import psutil
# Part of standard packages in Python 3.4+
import time
import os
import json
import subprocess
import zipfile

# ================================================================================

# Class that holds metric data and has a few utility functions for getting that data in a format we can use for Cloudwatch
class DataSnapshot_Metric():
    def __init__(self, metric_name, metric_function, metric_dimensions=[],
                metric_unit="None", metric_alarm_threshold=None, metric_alarm_severity=6,
                git_hash="", git_repo_name="", reports_to_skip=0):
        self.metric_name = metric_name
        self.metric_function = metric_function
        self.metric_dimensions = metric_dimensions
        self.metric_unit = metric_unit
        self.metric_alarm_threshold = metric_alarm_threshold
        self.metric_alarm_name = self.metric_name + "-" + git_repo_name + "-" + git_hash
        self.metric_alarm_description = 'Alarm for metric "' + self.metric_name + '" - git hash: ' + git_hash
        self.metric_value = None
        self.reports_to_skip = reports_to_skip
        self.metric_alarm_severity = metric_alarm_severity

    # Gets the latest metric value from the metric_function callback
    def get_metric_value(self, psutil_process : psutil.Process):
        if not self.metric_function is None:
            self.metric_value = self.metric_function(psutil_process)
        return self.metric_value

    # Returns the data needed to send to Cloudwatch when posting metrics
    def get_metric_cloudwatch_dictionary(self):
        if (self.reports_to_skip > 0):
            self.reports_to_skip -= 1
            return None # skips sending to Cloudwatch

        if (self.metric_value == None):
            return None # skips sending to Cloudwatch

        return {
            "MetricName": self.metric_name,
            "Dimensions": self.metric_dimensions,
            "Value": self.metric_value,
            "Unit": self.metric_unit
        }

class DataSnapshot_Dashboard_Widget():
    def __init__(self, widget_name, metric_namespace, metric_dimension, cloudwatch_region="us-east-1", widget_period=60) -> None:
        self.metric_list = []
        self.region = cloudwatch_region
        self.widget_name = widget_name
        self.metric_namespace = metric_namespace
        self.metric_dimension = metric_dimension
        self.widget_period = widget_period

    def add_metric_to_widget(self, new_metric_name):
        try:
            self.metric_list.append(new_metric_name)
        except Exception as e:
            print ("ERROR - could not add metric to dashboard widget due to exception!")
            print ("Exception: " + str(e))

    def remove_metric_from_widget(self, existing_metric_name):
        try:
            self.metric_list.remove(existing_metric_name)
        except Exception as e:
            print ("ERROR - could not remove metric from dashboard widget due to exception!")
            print ("Exception: " + str(e))

    def get_widget_dictionary(self):
        metric_list_json = []
        for metric_name in self.metric_list:
            metric_list_json.append([self.metric_namespace, metric_name, self.metric_dimension, metric_name])

        return {
            "type":"metric",
            "properties" : {
                "metrics" : metric_list_json,
                "region": self.region,
                "title": self.widget_name,
                "period": self.widget_period,
            },
            "width": 14,
            "height": 10
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
                 datetime_string=None,
                 output_log_filepath=None,
                 output_to_console=True,
                 cloudwatch_region="us-east-1",
                 cloudwatch_make_dashboard=False,
                 cloudwatch_teardown_alarms_on_complete=True,
                 cloudwatch_teardown_dashboard_on_complete=True,
                 s3_bucket_name="canary-wrapper-bucket",
                 s3_bucket_upload_on_complete=True,
                 lambda_name="CanarySendEmailLambda"):

        # Setting initial values
        # ==================
        self.first_metric_call = True
        self.metrics = []
        self.metric_report_number = 0

        # Needed so we can initialize Cloudwatch alarms, etc, outside of the init function
        # but before we start sending data.
        # This boolean tracks whether we have done the post-initization prior to sending the first report.
        self.perform_final_initialization = True

        # Watched by the thread creating the snapshot. Will cause the thread(s) to abort and return an error.
        self.abort_due_to_internal_error = False
        self.abort_due_to_internal_error_reason = ""
        self.abort_due_to_internal_error_due_to_credentials = False

        self.git_hash = None
        self.git_repo_name = None
        self.git_hash_as_namespace = git_hash_as_namespace
        self.git_fixed_namespace_text = git_fixed_namespace_text
        self.git_metric_namespace = None

        self.cloudwatch_region = cloudwatch_region
        self.cloudwatch_client = None
        self.cloudwatch_make_dashboard = cloudwatch_make_dashboard
        self.cloudwatch_teardown_alarms_on_complete = cloudwatch_teardown_alarms_on_complete
        self.cloudwatch_teardown_dashboard_on_complete = cloudwatch_teardown_dashboard_on_complete
        self.cloudwatch_dashboard_name = ""
        self.cloudwatch_dashboard_widgets = []

        self.s3_bucket_name = s3_bucket_name
        self.s3_client = None
        self.s3_bucket_upload_on_complete = s3_bucket_upload_on_complete

        self.output_to_file_filepath = output_log_filepath
        self.output_to_file = False
        self.output_file = None
        self.output_to_console = output_to_console

        self.lambda_client = None
        self.lambda_name = lambda_name

        self.datetime_string = datetime_string
        # ==================

        # Check for valid credentials
        # ==================
        try:
            tmp_sts_client = boto3.client('sts')
            tmp_sts_client.get_caller_identity()
        except Exception as e:
            print ("ERROR - AWS credentials are NOT valid!")
            self.abort_due_to_internal_error = True
            self.abort_due_to_internal_error_reason = "AWS credentials are NOT valid!"
            self.abort_due_to_internal_error_due_to_credentials = True
            return
        # ==================

        # Git related stuff
        # ==================
        if (git_hash == None or git_repo_name == None):
            print("ERROR - a Git hash and repository name are REQUIRED for the canary wrapper to run!")
            self.abort_due_to_internal_error = True
            self.abort_due_to_internal_error_reason = "No Git hash and repository passed!"
            return

        self.git_hash = git_hash
        self.git_repo_name = git_repo_name

        if (self.git_hash_as_namespace == False):
            self.git_metric_namespace = self.git_fixed_namespace_text
        else:
            if (self.datetime_string == None):
                git_namespace_prepend_text = self.git_repo_name + "-" + self.git_hash
            else:
                git_namespace_prepend_text = self.git_repo_name + "/" + self.datetime_string + "-" + self.git_hash
            self.git_metric_namespace = git_namespace_prepend_text
        # ==================

        # Cloudwatch related stuff
        # ==================
        try:
            self.cloudwatch_client = boto3.client('cloudwatch', self.cloudwatch_region)
            self.cloudwatch_dashboard_name = self.git_metric_namespace
        except Exception as e:
            self.print_message("ERROR - could not make Cloudwatch client due to exception!")
            self.print_message("Exception: " + str(e))
            self.cloudwatch_client = None
            self.abort_due_to_internal_error = True
            self.abort_due_to_internal_error_reason = "Could not make Cloudwatch client!"
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
            self.abort_due_to_internal_error_reason = "Could not make S3 client!"
            return
        # ==================

        # Lambda related stuff
        # ==================
        try:
            self.lambda_client = boto3.client("lambda", self.cloudwatch_region)
        except Exception as e:
            self.print_message("ERROR - could not make Lambda client due to exception!")
            self.print_message("Exception: " + str(e))
            self.lambda_client = None
            self.abort_due_to_internal_error = True
            self.abort_due_to_internal_error_reason = "Could not make Lambda client!"
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

        self.print_message("Data snapshot created!")

    # Cleans the class - closing any files, removing alarms, and sending data to S3.
    # Should be called at the end when you are totally finished shadowing metrics
    def cleanup(self, error_occurred=False):
        if (self.s3_bucket_upload_on_complete == True):
            self.export_result_to_s3_bucket(copy_output_log=True, log_is_error=error_occurred)

        self._cleanup_cloudwatch_alarms()
        if (self.cloudwatch_make_dashboard == True):
            self._cleanup_cloudwatch_dashboard()

        self.print_message("Data snapshot cleaned!")

        if (self.output_file is not None):
            self.output_file.close()
            self.output_file = None

    # Utility function for printing messages
    def print_message(self, message):
        if self.output_to_file == True:
            self.output_file.write(message + "\n")
        if self.output_to_console == True:
            print(message, flush=True)

    # Utility function - adds the metric alarms to Cloudwatch. We do run this right before the first
    # collection of metrics so we can register metrics before we initialize Cloudwatch
    def _init_cloudwatch_pre_first_run(self):
        for metric in self.metrics:
            if (not metric.metric_alarm_threshold is None):
                self._add_cloudwatch_metric_alarm(metric)

        if (self.cloudwatch_make_dashboard == True):
            self._init_cloudwatch_pre_first_run_dashboard()

    # Utility function - adds the Cloudwatch Dashboard for the currently running data snapshot
    def _init_cloudwatch_pre_first_run_dashboard(self):
        try:
            # Remove the old dashboard if it exists before adding a new one
            self._cleanup_cloudwatch_dashboard()

            new_dashboard_widgets_array = []
            for widget in self.cloudwatch_dashboard_widgets:
                new_dashboard_widgets_array.append(widget.get_widget_dictionary())

            new_dashboard_body = {
                "start": "-PT1H",
                "widgets": new_dashboard_widgets_array,
            }
            new_dashboard_body_json = json.dumps(new_dashboard_body)

            self.cloudwatch_client.put_dashboard(
                DashboardName=self.cloudwatch_dashboard_name,
                DashboardBody= new_dashboard_body_json)
            self.print_message("Added Cloudwatch dashboard successfully")
        except Exception as e:
            self.print_message("ERROR - Couldwatch client could not make dashboard due to exception!")
            self.print_message("Exception: " + str(e))
            self.abort_due_to_internal_error = True
            self.abort_due_to_internal_error_reason = "Couldwatch client could not make dashboard due to exception"
            return

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

    # Utility function - removes all the Cloudwatch alarms for the metrics
    def _cleanup_cloudwatch_alarms(self):
        if (self.cloudwatch_teardown_alarms_on_complete == True):
            try:
                for metric in self.metrics:
                    if (not metric.metric_alarm_threshold is None):
                        self.cloudwatch_client.delete_alarms(AlarmNames=[metric.metric_alarm_name])
            except Exception as e:
                self.print_message(
                    "ERROR - could not delete alarms due to exception!")
                self.print_message("Exception: " + str(e))

    # Utility function - removes all Cloudwatch dashboards created
    def _cleanup_cloudwatch_dashboard(self):
        if (self.cloudwatch_teardown_dashboard_on_complete == True):
            try:
                self.cloudwatch_client.delete_dashboards(DashboardNames=[self.cloudwatch_dashboard_name])
                self.print_message("Cloudwatch Dashboards deleted successfully!")
            except Exception as e:
                self.print_message("ERROR - dashboard cleaning function failed due to exception!")
                self.print_message("Exception: " + str(e))
                self.abort_due_to_internal_error = True
                self.abort_due_to_internal_error_reason = "Cloudwatch dashboard cleaning function failed due to exception"
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
    def export_result_to_s3_bucket(self, copy_output_log=False, log_is_error=False):
        if (self.s3_client is None):
            self.print_message(
                "ERROR - No S3 client initialized! Cannot send log to S3")
            self.abort_due_to_internal_error = True
            self.abort_due_to_internal_error_reason = "S3 client not initialized and therefore cannot send log to S3"
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
            if (log_is_error == False):
                if (self.datetime_string == None):
                    self.s3_client.upload_file(self.git_hash + ".log", self.s3_bucket_name, self.git_repo_name + "/" + self.git_hash + ".log")
                else:
                    self.s3_client.upload_file(self.git_hash + ".log", self.s3_bucket_name, self.git_repo_name + "/" + self.datetime_string + "/" + self.git_hash + ".log")
            else:
                if (self.datetime_string == None):
                    self.s3_client.upload_file(self.git_hash + ".log", self.s3_bucket_name, self.git_repo_name + "/Failed_Logs/" + self.git_hash + ".log")
                else:
                    self.s3_client.upload_file(self.git_hash + ".log", self.s3_bucket_name, self.git_repo_name + "/Failed_Logs/" + self.datetime_string + "/" + self.git_hash + ".log")
            self.print_message("Uploaded to S3!")
        except Exception as e:
            self.print_message(
                "ERROR - could not upload to S3 due to exception!")
            self.print_message("Exception: " + str(e))
            self.abort_due_to_internal_error = True
            self.abort_due_to_internal_error_reason = "S3 client had exception and therefore could not upload log!"
            os.remove(self.git_hash + ".log")
            return

        # Delete the file when finished
        os.remove(self.git_hash + ".log")

    # Sends an email via a special lambda. The payload has to contain a message and a subject
    # * (REQUIRED) message is the message you want to send in the body of the email
    # * (REQUIRED) subject is the subject that the email will be sent with
    def lambda_send_email(self, message, subject):

        payload = {"Message":message, "Subject":subject}
        payload_string = json.dumps(payload)

        try:
            self.lambda_client.invoke(
                FunctionName=self.lambda_name,
                InvocationType="Event",
                ClientContext="MQTT Wrapper Script",
                Payload=payload_string
            )
        except Exception as e:
            self.print_message("ERROR - could not send email via Lambda due to exception!")
            self.print_message("Exception: " + str(e))
            self.abort_due_to_internal_error = True
            self.abort_due_to_internal_error_reason = "Lambda email function had an exception!"
            return

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
            git_repo_name=self.git_repo_name,
            reports_to_skip=new_metric_reports_to_skip
        )
        self.metrics.append(new_metric)

    def register_dashboard_widget(self, new_widget_name, metrics_to_add=[], new_widget_period=60):

        # We need to know what metric dimension to get the metric(s) from
        metric_dimension_string = ""
        if (self.git_hash_as_namespace == False):
            metric_dimension_string = self.git_repo_name + "-" + self.git_hash
        else:
            metric_dimension_string = "System_Metrics"

        widget = self._find_cloudwatch_widget(name=new_widget_name)
        if (widget == None):
            widget = DataSnapshot_Dashboard_Widget(
                widget_name=new_widget_name, metric_namespace=self.git_metric_namespace,
                metric_dimension=metric_dimension_string,
                cloudwatch_region=self.cloudwatch_region,
                widget_period=new_widget_period)
            self.cloudwatch_dashboard_widgets.append(widget)

        for metric in metrics_to_add:
            self.register_metric_to_dashboard_widget(widget_name=new_widget_name, metric_name=metric)

    def register_metric_to_dashboard_widget(self, widget_name, metric_name, widget=None):
        if widget is None:
            widget = self._find_cloudwatch_widget(name=widget_name)
            if widget is None:
                print ("ERROR - could not find widget with name: " + widget_name, flush=True)
                return

        # Adjust metric name so it has the git hash, repo, etc
        metric_name_formatted = metric_name

        widget.add_metric_to_widget(new_metric_name=metric_name_formatted)
        return

    def remove_metric_from_dashboard_widget(self, widget_name, metric_name, widget=None):
        if widget is None:
            widget = self._find_cloudwatch_widget(name=widget_name)
            if widget is None:
                print ("ERROR - could not find widget with name: " + widget_name, flush=True)
                return
        widget.remove_metric_from_widget(existing_metric_name=metric_name)
        return

    def _find_cloudwatch_widget(self, name):
        result = None
        for widget in self.cloudwatch_dashboard_widgets:
            if widget.widget_name == name:
                return widget
        return result

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
            self.abort_due_to_internal_error_reason = "Could not export Cloudwatch metrics due to no Cloudwatch client initialized!"
            return

        self.print_message("Preparing to send to Cloudwatch...")
        metrics_data = []
        metric_data_tmp = None
        for metric in self.metrics:
            metric_data_tmp = metric.get_metric_cloudwatch_dictionary()
            if (not metric_data_tmp is None):
                metrics_data.append(metric_data_tmp)

        if (len(metrics_data) == 0):
            self.print_message("INFO - no metric data to send. Skipping...")
            return

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
            self.abort_due_to_internal_error_reason = "Could not export Cloudwatch metrics due to exception in Cloudwatch client!"
            return

    # Call this at a set interval to post the metrics to Cloudwatch, etc.
    # This is the function you want to call repeatedly after you have everything setup.
    def post_metrics(self, psutil_process : psutil.Process):
        if (self.perform_final_initialization == True):
            self.perform_final_initialization = False
            self._init_cloudwatch_pre_first_run()

        # Update the metric values internally
        for metric in self.metrics:
            metric.get_metric_value(psutil_process)
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

class SnapshotMonitor():
    def __init__(self, wrapper_data_snapshot, wrapper_metrics_wait_time) -> None:

        self.data_snapshot = wrapper_data_snapshot
        self.had_internal_error = False
        self.error_due_to_credentials = False
        self.internal_error_reason = ""
        self.error_due_to_alarm = False

        self.can_cut_ticket = False
        self.has_cut_ticket = False

        # A list of all the alarms triggered in the last check, cached for later
        # NOTE - this is only the alarm names! Not the severity. This just makes it easier to process
        self.cloudwatch_current_alarms_triggered = []

        # Check for errors
        if (self.data_snapshot.abort_due_to_internal_error == True):
            self.had_internal_error = True
            self.internal_error_reason = "Could not initialize DataSnapshot. Likely credentials are not setup!"
            if (self.data_snapshot.abort_due_to_internal_error_due_to_credentials == True):
                self.error_due_to_credentials = True
            self.data_snapshot.cleanup()
            return

        # How long to wait before posting a metric
        self.metric_post_timer = 0
        self.metric_post_timer_time = wrapper_metrics_wait_time


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
            print ("Exception: " + str(e), flush=True)
            self.had_internal_error = True
            self.internal_error_reason = "Could not register metric in data snapshot due to exception"
            return

    def register_dashboard_widget(self, new_widget_name, metrics_to_add=[], widget_period=60):
        self.data_snapshot.register_dashboard_widget(new_widget_name=new_widget_name, metrics_to_add=metrics_to_add, new_widget_period=widget_period)

    def output_diagnosis_information(self, dependencies=""):
        self.data_snapshot.output_diagnosis_information(dependencies_list=dependencies)

    def check_alarms_for_new_alarms(self, triggered_alarms):

        if len(triggered_alarms) > 0:
            self.data_snapshot.print_message(
                "WARNING - One or more alarms are in state of ALARM")

            old_alarms_still_active = []
            new_alarms = []
            new_alarms_highest_severity = 6
            new_alarm_found = True
            new_alarm_ticket_description = "Canary has metrics in ALARM state!\n\nMetrics in alarm:\n"

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
                if (self.can_cut_ticket == True):
                    cut_ticket_using_cloudwatch(
                        git_repo_name=self.data_snapshot.git_repo_name,
                        git_hash=self.data_snapshot.git_hash,
                        git_hash_as_namespace=False,
                        git_fixed_namespace_text=self.data_snapshot.git_fixed_namespace_text,
                        cloudwatch_region="us-east-1",
                        ticket_description="New metric(s) went into alarm for the Canary! Metrics in alarm: " + str(new_alarms),
                        ticket_reason="New metric(s) went into alarm",
                        ticket_allow_duplicates=True,
                        ticket_category="AWS",
                        ticket_item="IoT SDK for CPP",
                        ticket_group="AWS IoT Device SDK",
                        ticket_type="SDKs and Tools",
                        ticket_severity=4)
                    self.has_cut_ticket = True

            # Cache the new alarms and the old alarms
            self.cloudwatch_current_alarms_triggered = old_alarms_still_active + new_alarms

        else:
            self.cloudwatch_current_alarms_triggered.clear()


    def monitor_loop_function(self, psutil_process : psutil.Process, time_passed=30):
        # Check for internal errors
        if (self.data_snapshot.abort_due_to_internal_error == True):
            self.had_internal_error = True
            self.internal_error_reason = "Data Snapshot internal error: " + self.data_snapshot.abort_due_to_internal_error_reason
            return

        try:
            # Poll the metric alarms
            if (self.had_internal_error == False):
                # Get a report of all the alarms that might have been set to an alarm state
                triggered_alarms = self.data_snapshot.get_cloudwatch_alarm_results()
                self.check_alarms_for_new_alarms(triggered_alarms)
        except Exception as e:
            self.data_snapshot.print_message("ERROR - exception occurred checking metric alarms!")
            self.data_snapshot.print_message("(Likely session credentials expired)")
            self.had_internal_error = True
            self.internal_error_reason = "Exception occurred checking metric alarms! Likely session credentials expired"
            return

        if (self.metric_post_timer <= 0):
            if (self.had_internal_error == False):
                try:
                    self.data_snapshot.post_metrics(psutil_process)
                except Exception as e:
                    self.data_snapshot.print_message("ERROR - exception occurred posting metrics!")
                    self.data_snapshot.print_message("(Likely session credentials expired)")

                    print (e, flush=True)

                    self.had_internal_error = True
                    self.internal_error_reason = "Exception occurred posting metrics! Likely session credentials expired"
                    return

            # reset the timer
            self.metric_post_timer += self.metric_post_timer_time

        # Gather and post the metrics
        self.metric_post_timer -= time_passed


    def send_email(self, email_body, email_subject_text_append=None):
        if (email_subject_text_append != None):
            self.data_snapshot.lambda_send_email(email_body, "Canary: " + self.data_snapshot.git_repo_name + ":" + self.data_snapshot.git_hash + " - " + email_subject_text_append)
        else:
            self.data_snapshot.lambda_send_email(email_body, "Canary: " + self.data_snapshot.git_repo_name + ":" + self.data_snapshot.git_hash)


    def stop_monitoring(self):
        # Stub - just added for consistency
        pass


    def start_monitoring(self):
        # Stub - just added for consistency
        pass


    def restart_monitoring(self):
        # Stub - just added for consistency
        pass


    def cleanup_monitor(self, error_occurred=False):
        self.data_snapshot.cleanup(error_occurred=error_occurred)

# ================================================================================

class ApplicationMonitor():
    def __init__(self, wrapper_application_path, wrapper_application_arguments, wrapper_application_restart_on_finish=True) -> None:
        self.application_process = None
        self.application_process_psutil = None
        self.error_has_occurred = False
        self.error_due_to_credentials = False
        self.error_reason = ""
        self.error_code = 0
        self.wrapper_application_path = wrapper_application_path
        self.wrapper_application_arguments = wrapper_application_arguments
        self.wrapper_application_restart_on_finish = wrapper_application_restart_on_finish

    def start_monitoring(self):
        print ("Starting to monitor application...", flush=True)

        if (self.application_process == None):
            try:
                canary_command = self.wrapper_application_path + " " + self.wrapper_application_arguments
                self.application_process = subprocess.Popen(canary_command, shell=True)
                self.application_process_psutil = psutil.Process(self.application_process.pid)
                print ("Application started...", flush=True)
            except Exception as e:
                print ("ERROR - Could not launch Canary/Application due to exception!")
                print ("Exception: " + str(e), flush=True)
                self.error_has_occurred = True
                self.error_reason = "Could not launch Canary/Application due to exception"
                self.error_code = 1
                return
        else:
            print ("ERROR - Monitor already has an application process! Cannot monitor two applications with one monitor class!", flush=True)

    def restart_monitoring(self):
        print ("Restarting monitor application...", flush=True)

        if (self.application_process != None):
            try:
                self.stop_monitoring()
                self.start_monitoring()
                print ("Restarted monitor application!", flush=True)
            except Exception as e:
                print ("ERROR - Could not restart Canary/Application due to exception!")
                print ("Exception: " + str(e), flush=True)
                self.error_has_occurred = True
                self.error_reason = "Could not restart Canary/Application due to exception"
                self.error_code = 1
                return
        else:
            print ("ERROR - Application process restart called but process is/was not running!", flush=True)
            self.error_has_occurred = True
            self.error_reason = "Could not restart Canary/Application due to application process not being started initially"
            self.error_code = 1
            return


    def stop_monitoring(self):
        print ("Stopping monitor application...", flush=True)
        if (not self.application_process == None):
            self.application_process.terminate()
            self.application_process.wait()
            self.application_process = None
            print ("Stopped monitor application!", flush=True)
        else:
            print ("ERROR - cannot stop monitor application because no process is found!", flush=True)


    def monitor_loop_function(self, time_passed=30):
        if (self.application_process != None):

            application_process_return_code = None
            try:
                application_process_return_code = self.application_process.poll()
            except Exception as e:
                print ("ERROR - exception occurred while trying to poll application status!")
                print ("Exception: " + str(e), flush=True)
                self.error_has_occurred = True
                self.error_reason = "Exception when polling application status"
                self.error_code = 1
                return

            # If it is not none, then the application finished
            if (application_process_return_code != None):

                print ("Monitor application has stopped! Processing result...", flush=True)

                if (application_process_return_code != 0):
                    print ("ERROR - Something Crashed in Canary/Application!")
                    print ("Error code: " + str(application_process_return_code), flush=True)

                    self.error_has_occurred = True
                    self.error_reason = "Canary application crashed!"
                    self.error_code = application_process_return_code
                else:
                    # Should we restart?
                    if (self.wrapper_application_restart_on_finish == True):
                        print ("NOTE - Canary finished running and is restarting...", flush=True)
                        self.restart_monitoring()
                    else:
                        print ("Monitor application has stopped and monitor is not supposed to restart... Finishing...", flush=True)
                        self.error_has_occurred = True
                        self.error_reason = "Canary Application Finished"
                        self.error_code = 0

    def cleanup_monitor(self, error_occurred=False):
        pass

# ================================================================================

class S3_Monitor():

    def __init__(self, s3_bucket_name, s3_file_name, s3_file_name_in_zip, canary_local_application_path) -> None:
        self.s3_client = None
        self.s3_current_object_version_id = None
        self.s3_current_object_last_modified = None
        self.s3_bucket_name = s3_bucket_name
        self.s3_file_name = s3_file_name
        self.s3_file_name_only_path, self.s3_file_name_only_extension = os.path.splitext(s3_file_name)

        self.canary_local_application_path = canary_local_application_path

        self.s3_file_name_in_zip = s3_file_name_in_zip
        self.s3_file_name_in_zip_only_path = None
        self.s3_file_name_in_zip_only_extension = None
        if (self.s3_file_name_in_zip != None):
            self.s3_file_name_in_zip_only_path, self.s3_file_name_in_zip_only_extension = os.path.splitext(s3_file_name_in_zip)

        self.s3_file_needs_replacing = False

        self.had_internal_error = False
        self.error_due_to_credentials = False
        self.internal_error_reason = ""

        # Check for valid credentials
        # ==================
        try:
            tmp_sts_client = boto3.client('sts')
            tmp_sts_client.get_caller_identity()
        except Exception as e:
            print ("ERROR - (S3 Check) AWS credentials are NOT valid!", flush=True)
            self.had_internal_error = True
            self.error_due_to_credentials = True
            self.internal_error_reason = "AWS credentials are NOT valid!"
            return
        # ==================

        try:
            self.s3_client = boto3.client("s3")
        except Exception as e:
            print ("ERROR - (S3 Check) Could not make S3 client", flush=True)
            self.had_internal_error = True
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
                            print ("Changing running Canary/Application to new one...", flush=True)

                            # Will be checked by thread to trigger replacing the file
                            self.s3_file_needs_replacing = True

                            self.s3_current_object_version_id = version["VersionId"]
                            self.s3_current_object_last_modified = version["LastModified"]
                            return

        except Exception as e:
            print ("ERROR - Could not check for new version of file in S3 due to exception!")
            print ("Exception: " + str(e), flush=True)
            self.had_internal_error = True
            self.internal_error_reason = "Could not check for S3 file due to exception in S3 client"


    def replace_current_file_for_new_file(self):
        try:
            print ("Making directory...", flush=True)
            if not os.path.exists("tmp"):
                os.makedirs("tmp")
        except Exception as e:
            print ("ERROR - could not make tmp directory to place S3 file into!", flush=True)
            self.had_internal_error = True
            self.internal_error_reason = "Could not make TMP folder for S3 file download"
            return

        # Download the file
        new_file_path = "tmp/new_file" + self.s3_file_name_only_extension
        try:
            print ("Downloading file...", flush=True)
            s3_resource = boto3.resource("s3")
            s3_resource.meta.client.download_file(self.s3_bucket_name, self.s3_file_name, new_file_path)
        except Exception as e:
            print ("ERROR - could not download latest S3 file into TMP folder!", flush=True)
            self.had_internal_error = True
            self.internal_error_reason = "Could not download latest S3 file into TMP folder"
            return

        # Is it a zip file?
        if (self.s3_file_name_in_zip != None):
            # Unzip it!
            with zipfile.ZipFile(new_file_path, 'r') as zip_file:
                zip_file.extractall("tmp/new_file_zip")
                new_file_path = "tmp/new_file_zip/" + self.s3_file_name_in_zip_only_path + self.s3_file_name_in_zip_only_extension

        try:
            # is there a file already present there?
            if os.path.exists(self.canary_local_application_path) == True:
                os.remove(self.canary_local_application_path)

            print ("Moving file...", flush=True)
            os.replace(new_file_path, self.canary_local_application_path)
            print ("Getting execution rights...", flush=True)
            os.system("chmod u+x " + self.canary_local_application_path)

        except Exception as e:
            print ("ERROR - could not move file into local application path due to exception!")
            print ("Exception: " + str(e), flush=True)
            self.had_internal_error = True
            self.internal_error_reason = "Could not move file into local application path"
            return

        print ("New file downloaded and moved into correct location!")
        self.s3_file_needs_replacing = False


    def stop_monitoring(self):
        # Stub - just added for consistency
        pass


    def start_monitoring(self):
        # Stub - just added for consistency
        pass


    def restart_monitoring(self):
        # Stub - just added for consistency
        pass


    def cleanup_monitor(self):
        # Stub - just added for consistency
        pass

    def monitor_loop_function(self, time_passed=30):
        self.check_for_file_change()

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

    return

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
        print ("Exception: " + str(e), flush=True)
        return

    print("Waiting for ticket metric to trigger...", flush=True)
    # Wait a little bit (2 seconds)...
    time.sleep(2)

    # Remove the metric
    print("Removing ticket metric...", flush=True)
    cloudwatch_client.delete_alarms(AlarmNames=[ticket_alarm_name])

    print ("Finished cutting ticket via Cloudwatch!", flush=True)
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
