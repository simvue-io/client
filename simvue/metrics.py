import contextlib
import logging

from .pynvml import (
    nvmlDeviceGetComputeRunningProcesses,
    nvmlDeviceGetCount,
    nvmlDeviceGetGraphicsRunningProcesses,
    nvmlDeviceGetHandleByIndex,
    nvmlDeviceGetMemoryInfo,
    nvmlDeviceGetUtilizationRates,
    nvmlInit,
    nvmlShutdown,
)

logger = logging.getLogger(__name__)


def get_process_memory(processes):
    """
    Get the resident set size
    """
    rss = 0
    for process in processes:
        with contextlib.suppress(Exception):
            rss += process.memory_info().rss / 1024 / 1024

    return rss


def get_process_cpu(processes):
    """
    Get the CPU usage
    """
    cpu_percent = 0
    for process in processes:
        with contextlib.suppress(Exception):
            cpu_percent += process.cpu_percent()

    return cpu_percent


def is_gpu_used(handle, processes):
    """
    Check if the GPU is being used by the list of processes
    """
    pids = [process.pid for process in processes]

    gpu_pids = []
    for process in nvmlDeviceGetComputeRunningProcesses(handle):
        gpu_pids.append(process.pid)

    for process in nvmlDeviceGetGraphicsRunningProcesses(handle):
        gpu_pids.append(process.pid)

    return len(list(set(gpu_pids) & set(pids))) > 0


def get_gpu_metrics(processes):
    """
    Get GPU metrics
    """
    gpu_metrics = {}

    with contextlib.suppress(Exception):
        nvmlInit()
        device_count = nvmlDeviceGetCount()
        for i in range(device_count):
            handle = nvmlDeviceGetHandleByIndex(i)
            if is_gpu_used(handle, processes):
                utilisation_percent = nvmlDeviceGetUtilizationRates(handle).gpu
                memory = nvmlDeviceGetMemoryInfo(handle)
                memory_percent = 100 * memory.free / memory.total
                gpu_metrics[f"resources/gpu.utilisation.percent.{i}"] = (
                    utilisation_percent
                )
                gpu_metrics[f"resources/gpu.memory.percent.{i}"] = memory_percent

        nvmlShutdown()

    return gpu_metrics
