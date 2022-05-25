# Contains all of the metric reporting functions for the Canary Wrappers

# Needs to be installed prior to running
import psutil


cache_cpu_psutil_process = None
def get_metric_total_cpu_usage(psutil_process : psutil.Process):
    global cache_cpu_psutil_process

    try:
        if (psutil_process == None):
            print ("ERROR - No psutil.process passed! Cannot gather metric!")
            return None
        # We always need to skip the first CPU poll on a new process
        if (cache_cpu_psutil_process != psutil_process):
            psutil_process.cpu_percent(0.0)
            cache_cpu_psutil_process = psutil_process
            return None
        return psutil_process.cpu_percent(0.0)
    except Exception as e:
        print ("ERROR - exception occured gathering metrics!")
        print ("Exception: " + str(e))
        return None


def get_metric_total_memory_usage_value(psutil_process : psutil.Process):
    try:
        if (psutil_process == None):
            print ("ERROR - No psutil.process passed! Cannot gather metric!")
            return None
        return psutil_process.memory_info().rss
    except Exception as e:
        print ("ERROR - exception occured gathering metrics!")
        print ("Exception: " + str(e))
        return None


def get_metric_total_memory_usage_percent(psutil_process : psutil.Process):
    try:
        if (psutil_process == None):
            print ("ERROR - No psutil.process passed! Cannot gather metric!")
            return None
        return psutil_process.memory_percent()
    except Exception as e:
        print ("ERROR - exception occured gathering metrics!")
        print ("Exception: " + str(e))
        return None

