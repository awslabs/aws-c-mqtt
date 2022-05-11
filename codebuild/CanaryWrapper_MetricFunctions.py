# Contains all of the metric reporting functions for the Canary Wrappers

# Needs to be installed prior to running
import psutil


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
