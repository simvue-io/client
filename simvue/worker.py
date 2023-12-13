import datetime
import json
import logging
import os
import psutil
import sys
import time
import threading
import msgpack

from .api import get
from .metrics import get_process_memory, get_process_cpu, get_gpu_metrics
from .utilities import get_offline_directory, create_file, get_server_version

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
    except:
        return None

    return processes


class Worker(threading.Thread):
    def __init__(self, metrics_queue, events_queue, abort_queue, shutdown_event, uuid, run_name, run_id, url, headers, mode, pid, enable_abort, resources_metrics_interval):
        threading.Thread.__init__(self)
        self._parent_thread = threading.current_thread()
        self._metrics_queue = metrics_queue
        self._events_queue = events_queue
        self._abort_queue = abort_queue
        self._shutdown_event = shutdown_event
        self._run_name = run_name
        self._run_id = run_id
        self._uuid = uuid
        self._url = url
        self._headers = headers
        self._headers_mp = headers.copy()
        self._headers_mp['Content-Type'] = 'application/msgpack'
        self._mode = mode
        self._directory = os.path.join(get_offline_directory(), self._uuid)
        self._start_time = time.time()
        self._processes = []
        self._resources_metrics_interval = resources_metrics_interval
        self._version = get_server_version()
        self._pid = pid
        if pid:
            self._processes = update_processes(psutil.Process(pid), [])
        self._enable_abort = enable_abort
        logger.debug('Worker thread started')

    def heartbeat(self):
        """
        Send a heartbeat
        """
        data = {'id': self._run_id}
        if self._version == 0:
            data = {'name': self._run_name}

        if self._mode == 'online':
            from .api import put
            put(f"{self._url}/api/runs/heartbeat", self._headers, data)
        else:
            create_file(f"{self._directory}/heartbeat")

    def post(self, endpoint, data):
        """
        Send the supplied data
        """
        if self._mode == 'online':
            from .api import post
            post(f"{self._url}/api/{endpoint}", self._headers_mp, data=data, is_json=False)
        else:
            unique_id = time.time()
            filename = f"{self._directory}/{endpoint}-{unique_id}"
            try:
                with open(filename, 'w') as fh:
                    json.dump(data, fh)
            except Exception as err:
                logger.error('Got exception writing offline update for %s: %s', endpoint, str(err))

    def run(self):
        """
        Loop sending heartbeats, metrics and events
        """
        last_heartbeat = 0
        last_metrics = 0
        collected = False
        while True:
            # Collect metrics if necessary
            if time.time() - last_metrics > self._resources_metrics_interval and self._processes:
                if self._pid:
                    try:
                        self._processes = update_processes(psutil.Process(self._pid), self._processes)
                    except:
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
                            if self._version == 0:
                                data['run'] = self._run_name
                            data['step'] = 0
                            data['values'] = {'resources/cpu.usage.percent': cpu,
                                          'resources/memory.usage': memory}
                            if gpu:
                                for item in gpu:
                                    data['values'][item] = gpu[item]
                            data['time'] = time.time() - self._start_time
                            data['timestamp'] = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')
                            try:
                                self._metrics_queue.put(data, block=False)
                            except:
                                pass
                        last_metrics = time.time()

            # Send heartbeat if necessary
            if time.time() - last_heartbeat > HEARTBEAT_INTERVAL:
                try:
                    self.heartbeat()
                except Exception as err:
                    logger.error('Error sending heartbeat: %s', str(err))
                last_heartbeat = time.time()

            # Send metrics
            buffer = []
            while not self._metrics_queue.empty() and len(buffer) < MAX_BUFFER_SEND:
                item = self._metrics_queue.get(block=False)
                buffer.append(item)
                self._metrics_queue.task_done()

            if buffer:
                logger.debug('Sending metrics')
                if self._version > 0:
                    buffer = {'metrics': buffer, 'run': self._run_id}
                try:
                    if self._mode == 'online': buffer = msgpack.packb(buffer, use_bin_type=True)
                    self.post('metrics', buffer)
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
                logger.debug('Sending events')
                if self._version > 0:
                    buffer = {'events': buffer, 'run': self._run_id}
                try:
                    if self._mode == 'online': buffer = msgpack.packb(buffer, use_bin_type=True)
                    self.post('events', buffer)
                except Exception as err:
                    logger.error(str(err))
                buffer = []

            # Check if user application should be aborted & abort it if necessary
            if self._enable_abort:
                try:
                    response = get(f"{self._url}/api/runs/{self._run_id}/abort", self._headers)
                    if response.json()['status']:
                        if self._pid:
                            logger.info('Aborting user process due to request from server')
                            self._abort_queue.put('aborted')
                            p = psutil.Process(self._pid)
                            p.kill()
                except Exception as err:
                    logger.error("Got exception when checking whether run should be aborted: %s", str(err))

            if self._shutdown_event.is_set() or not self._parent_thread.is_alive():
                if self._metrics_queue.empty() and self._events_queue.empty():
                    logger.debug('Ending worker thread')
                    sys.exit(0)
            else:
                counter = 0
                while counter < POLLING_INTERVAL and not self._shutdown_event.is_set() and self._parent_thread.is_alive() and not self._events_queue.full() and not self._metrics_queue.full():
                    time.sleep(0.1)
                    counter += 1
