# Contains all of the metric reporting functions for the Canary Wrappers

# Needs to be installed prior to running
import psutil


cache_cpu_psutil_process = None
def get_metric_total_cpu_usage(psutil_process : psutil.Process):
    global cache_cpu_psutil_process

    try:
        if (psutil_process == None):
            print ("ERROR - No psutil.process passed! Cannot gather metric!", flush=True)
            return None
        # We always need to skip the first CPU poll
        if (cache_cpu_psutil_process != psutil_process):
            psutil.cpu_percent(interval=None)
            cache_cpu_psutil_process = psutil_process
            return None
        return psutil.cpu_percent(interval=None)
    except Exception as e:
        print ("ERROR - exception occurred gathering metrics!")
        print (f"Exception: {repr(e)}", flush=True)
        return None

# Note: This value is in BYTES.
def get_metric_total_memory_usage_value(psutil_process : psutil.Process):
    try:
        if (psutil_process == None):
            print ("ERROR - No psutil.process passed! Cannot gather metric!", flush=True)
            return None
        return psutil.virtual_memory()[3]
    except Exception as e:
        print ("ERROR - exception occurred gathering metrics!")
        print (f"Exception: {repr(e)}", flush=True)
        return None


def get_metric_total_memory_usage_percent(psutil_process : psutil.Process):
    try:
        if (psutil_process == None):
            print ("ERROR - No psutil.process passed! Cannot gather metric!", flush=True)
            return None
        return psutil.virtual_memory()[2]
    except Exception as e:
        print ("ERROR - exception occurred gathering metrics!")
        print (f"Exception: {repr(e)}", flush=True)
        return None
