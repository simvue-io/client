import contextlib
import datetime
import json
import logging
import os
import threading
import time

import msgpack
import psutil

from .metrics import get_gpu_metrics, get_process_cpu, get_process_memory
from .utilities import create_file, get_offline_directory

logger = logging.getLogger(__name__)

HEARTBEAT_INTERVAL = 60
POLLING_INTERVAL = 20
MAX_BUFFER_SEND = 16000


def update_processes(parent, processes):
    """
    Create an array containing a list of processes
    """
    try:
        for child in parent.children(recursive=True):
            if child not in processes:
                processes.append(child)
        if parent not in processes:
            processes.append(parent)
    except Exception:
        return None

    return processes


class Worker(threading.Thread):
    def __init__(
        self,
        metrics_queue,
        events_queue,
        shutdown_event,
        uuid,
        run_name,
        run_id,
        url,
        headers,
        mode,
        pid,
        resources_metrics_interval,
    ):
        threading.Thread.__init__(self)
        self._parent_thread = threading.current_thread()
        self._metrics_queue = metrics_queue
        self._events_queue = events_queue
        self._shutdown_event = shutdown_event
        self._run_name = run_name
        self._run_id = run_id
        self._uuid = uuid
        self._url = url
        self._headers = headers
        self._headers_mp = headers.copy()
        self._headers_mp["Content-Type"] = "application/msgpack"
        self._mode = mode
        self._directory = os.path.join(get_offline_directory(), self._uuid)
        self._start_time = time.time()
        self._processes = []
        self._resources_metrics_interval = resources_metrics_interval
        self._pid = pid
        if pid:
            self._processes = update_processes(psutil.Process(pid), [])
        logger.debug("Worker thread started")

    def heartbeat(self):
        """
        Send a heartbeat
        """
        data = {"id": self._run_id}

        if self._mode == "online":
            from .api import put

            put(f"{self._url}/api/runs/heartbeat", self._headers, data)
        else:
            create_file(f"{self._directory}/heartbeat")

    def post(self, endpoint, data):
        """
        Send the supplied data
        """
        if self._mode == "online":
            from .api import post

            post(
                f"{self._url}/api/{endpoint}",
                self._headers_mp,
                data=data,
                is_json=False,
            )
        else:
            unique_id = time.time()
            filename = f"{self._directory}/{endpoint}-{unique_id}"
            try:
                with open(filename, "w") as fh:
                    json.dump(data, fh)
            except Exception as err:
                logger.error(
                    "Got exception writing offline update for %s: %s",
                    endpoint,
                    str(err),
                )

    def run(self):
        """
        Loop sending heartbeats, metrics and events
        """
        last_heartbeat = 0
        last_metrics = 0
        collected = False
        final_recovery_flush = False
        while True:
            # Collect metrics if necessary
            if (
                time.time() - last_metrics > self._resources_metrics_interval
                and self._processes
            ):
                if self._pid:
                    try:
                        self._processes = update_processes(
                            psutil.Process(self._pid), self._processes
                        )
                    except Exception:
                        self._processes = None

                if self._processes is not None:
                    cpu = get_process_cpu(self._processes)
                    if not collected:
                        # Need to wait before sending metrics, otherwise first point will have zero CPU usage
                        collected = True
                    else:
                        memory = get_process_memory(self._processes)
                        gpu = get_gpu_metrics(self._processes)
                        if memory is not None and cpu is not None:
                            data = {}
                            data["step"] = 0
                            data["values"] = {
                                "resources/cpu.usage.percent": cpu,
                                "resources/memory.usage": memory,
                            }
                            if gpu:
                                for item in gpu:
                                    data["values"][item] = gpu[item]
                            data["time"] = time.time() - self._start_time
                            data["timestamp"] = datetime.datetime.utcnow().strftime(
                                "%Y-%m-%d %H:%M:%S.%f"
                            )
                            with contextlib.suppress(Exception):
                                self._metrics_queue.put(data, block=False)

                        last_metrics = time.time()

            # Send heartbeat if necessary
            if time.time() - last_heartbeat > HEARTBEAT_INTERVAL:
                try:
                    self.heartbeat()
                except Exception as err:
                    logger.error("Error sending heartbeat: %s", str(err))
                last_heartbeat = time.time()

            # Send metrics
            buffer = []
            while not self._metrics_queue.empty() and len(buffer) < MAX_BUFFER_SEND:
                item = self._metrics_queue.get(block=False)
                buffer.append(item)
                self._metrics_queue.task_done()

            if buffer:
                logger.debug("Sending metrics")
                buffer = {"metrics": buffer, "run": self._run_id}
                try:
                    if self._mode == "online":
                        buffer = msgpack.packb(buffer, use_bin_type=True)
                    self.post("metrics", buffer)
                except Exception as err:
                    logger.error(str(err))
                buffer = []

            # Send events
            buffer = []
            while not self._events_queue.empty() and len(buffer) < MAX_BUFFER_SEND:
                item = self._events_queue.get(block=False)
                buffer.append(item)
                self._events_queue.task_done()

            if buffer:
                logger.debug("Sending events")
                buffer = {"events": buffer, "run": self._run_id}
                try:
                    if self._mode == "online":
                        buffer = msgpack.packb(buffer, use_bin_type=True)
                    self.post("events", buffer)
                except Exception as err:
                    logger.error(str(err))
                buffer = []

            if not self._parent_thread.is_alive():
                if final_recovery_flush or (
                    self._metrics_queue.empty() and self._events_queue.empty()
                ):
                    raise RuntimeError(
                        "Aborting worker due to parent thread termination"
                    )
                else:
                    logger.warning(
                        "Parent thread dead. Attempting final flush of buffers"
                    )
                    final_recovery_flush = True
                    continue

            if self._shutdown_event.is_set():
                logger.debug("Ending worker thread")
                return
            else:
                counter = 0
                while (
                    counter < POLLING_INTERVAL
                    and not self._shutdown_event.is_set()
                    and self._parent_thread.is_alive()
                    and not self._events_queue.full()
                    and not self._metrics_queue.full()
                ):
                    time.sleep(0.1)
                    counter += 1
