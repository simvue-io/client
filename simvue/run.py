"""
Simvue Run
==========

Main class for recording metrics and information to Simvue during code execution.
This forms the central API for users.
"""

import contextlib
import datetime
import json
import logging
import mimetypes
import multiprocessing.synchronize
import threading
import humanfriendly
import os
import multiprocessing
import pydantic
import re
import sys
import time
import platform
import typing
import uuid
from datetime import timezone

import click
import msgpack
import psutil
from pydantic import ValidationError

import simvue.api as sv_api

from .factory.dispatch import Dispatcher
from .executor import Executor
from .factory.proxy import Simvue
from .metrics import get_gpu_metrics, get_process_cpu, get_process_memory
from .models import RunInput
from .serialization import serialize_object
from .system import get_system
from .metadata import git_info
from .utilities import (
    calculate_sha256,
    compare_alerts,
    skip_if_failed,
    get_auth,
    get_offline_directory,
    validate_timestamp,
)

if typing.TYPE_CHECKING:
    from .factory.proxy import SimvueBaseClass
    from .factory.dispatch import DispatcherBaseClass

UPLOAD_TIMEOUT: int = 30
HEARTBEAT_INTERVAL: int = 60
RESOURCES_METRIC_PREFIX: str = "resources"

logger = logging.getLogger(__name__)


class Run:
    """Track simulation details based on token and URL

    The Run class provides a way of monitoring simulation runs by logging metrics
    and creating alerts based on such metrics. The recommended usage is as a
    context manager to ensure the run is closed upon completion.
    """

    @pydantic.validate_call
    def __init__(self, mode: typing.Literal["online", "offline"] = "online") -> None:
        """Initialise a new Simvue run

        Parameters
        ----------
        mode : Literal['online', 'offline'], optional
            mode of running, by default "online"
        """
        self._uuid: str = f"{uuid.uuid4()}"
        self._mode: typing.Literal["online", "offline", "disabled"] = mode
        self._name: typing.Optional[str] = None
        self._dispatch_mode: typing.Literal["direct", "queued"] = "queued"
        self._executor = Executor(self)
        self._dispatcher: typing.Optional[DispatcherBaseClass] = None
        self._id: typing.Optional[str] = None
        self._suppress_errors: bool = False
        self._queue_blocking: bool = False
        self._status: typing.Optional[
            typing.Literal[
                "created", "running", "completed", "failed", "terminated", "lost"
            ]
        ] = None
        self._data: dict[str, typing.Any] = {}
        self._step: int = 0
        self._active: bool = False
        self._aborted: bool = False
        self._url, self._token = get_auth()
        self._resources_metrics_interval: typing.Optional[int] = None
        self._headers: dict[str, str] = {"Authorization": f"Bearer {self._token}"}
        self._simvue: typing.Optional[SimvueBaseClass] = None
        self._pid: typing.Optional[int] = 0
        self._shutdown_event: typing.Optional[threading.Event] = None
        self._configuration_lock = threading.Lock()
        self._heartbeat_termination_trigger: typing.Optional[threading.Event] = None
        self._storage_id: typing.Optional[str] = None
        self._heartbeat_thread: typing.Optional[threading.Thread] = None

    def __enter__(self) -> "Run":
        return self

    def __exit__(
        self,
        exc_type: typing.Optional[typing.Type[BaseException]],
        value: BaseException,
        traceback: typing.Optional[
            typing.Union[typing.Type[BaseException], BaseException]
        ],
    ) -> None:
        # Wait for the executor to finish with currently running processes
        self._executor.wait_for_completion()

        identifier = self._id
        logger.debug(
            "Automatically closing run '%s' in status %s",
            identifier if self._mode == "online" else "unregistered",
            self._status,
        )

        # Stop the run heartbeat
        if self._heartbeat_thread and self._heartbeat_termination_trigger:
            self._heartbeat_termination_trigger.set()
            self._heartbeat_thread.join()

        # Handle case where run is aborted by user KeyboardInterrupt
        if (self._id or self._mode == "offline") and self._status == "running":
            if not exc_type:
                if self._shutdown_event is not None:
                    self._shutdown_event.set()
                if self._dispatcher:
                    self._dispatcher.join()
                self.set_status("completed")
            else:
                if self._active:
                    self.log_event(f"{exc_type.__name__}: {value}")
                if exc_type.__name__ in ("KeyboardInterrupt",) and self._active:
                    self.set_status("terminated")
                else:
                    if traceback and self._active:
                        self.log_event(f"Traceback: {traceback}")
                        self.set_status("failed")
        else:
            if self._shutdown_event is not None:
                self._shutdown_event.set()
            if self._dispatcher:
                self._dispatcher.purge()
                self._dispatcher.join()

        if _non_zero := self.executor.exit_status:
            logger.error(
                f"Simvue process executor terminated with non-zero exit status {_non_zero}"
            )
            sys.exit(_non_zero)

    @property
    def duration(self) -> float:
        """Return current run duration"""
        return time.time() - self._start_time

    @property
    def time_stamp(self) -> str:
        """Return current timestamp"""
        return datetime.datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")

    @property
    def processes(self) -> list[psutil.Process]:
        """Create an array containing a list of processes"""
        if not self._parent_process:
            return []

        _all_processes: list[psutil.Process] = [self._parent_process]

        with contextlib.suppress(psutil.NoSuchProcess, psutil.ZombieProcess):
            for child in self._parent_process.children(recursive=True):
                if child not in _all_processes:
                    _all_processes.append(child)

        return list(set(_all_processes))

    def _get_sysinfo(self) -> dict[str, typing.Any]:
        """Retrieve system administration

        Returns
        -------
        dict[str, typing.Any]
            retrieved system specifications
        """
        cpu = get_process_cpu(self.processes)
        memory = get_process_memory(self.processes)
        gpu = get_gpu_metrics(self.processes)
        data: dict[str, typing.Any] = {}

        if memory is not None and cpu is not None:
            data = {
                f"{RESOURCES_METRIC_PREFIX}/cpu.usage.percent": cpu,
                f"{RESOURCES_METRIC_PREFIX}/memory.usage": memory,
            }
            if gpu:
                for item in gpu:
                    data[item] = gpu[item]
        return data

    def _create_heartbeat_callback(
        self,
    ) -> typing.Callable[[str, dict, str, bool], None]:
        if (
            self._mode == "online" and (not self._url or not self._id)
        ) or not self._heartbeat_termination_trigger:
            raise RuntimeError("Could not commence heartbeat, run not initialised")

        def _heartbeat(
            heartbeat_trigger: threading.Event = self._heartbeat_termination_trigger,
        ) -> None:
            last_heartbeat = time.time()
            last_res_metric_call = time.time()

            while not heartbeat_trigger.is_set():
                time.sleep(0.1)

                with self._configuration_lock:
                    if (
                        self._resources_metrics_interval
                        and (res_time := time.time()) - last_res_metric_call
                        > self._resources_metrics_interval
                    ):
                        # Set join on fail to false as if an error is thrown
                        # join would be called on this thread and a thread cannot
                        # join itself!
                        self._add_metrics_to_dispatch(
                            self._get_sysinfo(), join_on_fail=False
                        )
                        last_res_metric_call = res_time

                if time.time() - last_heartbeat < HEARTBEAT_INTERVAL:
                    continue

                last_heartbeat = time.time()

                if self._simvue:
                    self._simvue.send_heartbeat()

        return _heartbeat

    def _create_dispatch_callback(
        self,
    ) -> typing.Callable:
        """Generates the relevant callback for posting of metrics and events

        The generated callback is assigned to the dispatcher instance and is
        executed on metrics and events objects held in a buffer.
        """

        if self._mode == "online" and not self._id:
            raise RuntimeError("Expected identifier for run")

        if not self._url:
            raise RuntimeError("Cannot commence dispatch, run not initialised")

        def _offline_dispatch_callback(
            buffer: list[typing.Any],
            category: str,
            run_id: typing.Optional[str] = self._id,
            uuid: str = self._uuid,
        ) -> None:
            if not os.path.exists((_offline_directory := get_offline_directory())):
                logger.error(
                    f"Cannot write to offline directory '{_offline_directory}', directory not found."
                )
                return
            _directory = os.path.join(_offline_directory, uuid)

            unique_id = time.time()
            filename = f"{_directory}/{category}-{unique_id}"
            _data = {category: buffer, "run": run_id}
            try:
                with open(filename, "w") as fh:
                    json.dump(_data, fh)
            except Exception as err:
                if self._suppress_errors:
                    logger.error(
                        "Got exception writing offline update for %s: %s",
                        category,
                        str(err),
                    )
                else:
                    raise err

        def _online_dispatch_callback(
            buffer: list[typing.Any],
            category: str,
            url: str = self._url,
            run_id: str = self._id,
            headers: dict[str, str] = self._headers,
        ) -> None:
            if not buffer:
                return
            _data = {category: buffer, "run": run_id}
            _data_bin = msgpack.packb(_data, use_bin_type=True)
            _url: str = f"{url}/api/{category}"

            _msgpack_header = headers | {"Content-Type": "application/msgpack"}

            sv_api.post(
                url=_url, headers=_msgpack_header, data=_data_bin, is_json=False
            )

        return (
            _online_dispatch_callback
            if self._mode == "online"
            else _offline_dispatch_callback
        )

    def _start(self, reconnect: bool = False) -> bool:
        """Start a run

        Parameters
        ----------
        reconnect : bool, optional
            whether this is a reconnect to an existing run, by default False

        Returns
        -------
        bool
            if successful
        """
        if self._mode == "disabled":
            return True

        if self._mode != "offline":
            self._uuid = "notused"

        logger.debug("Starting run")

        if self._simvue and not self._simvue.check_token():
            return False

        data: dict[str, typing.Any] = {"status": self._status}

        if reconnect:
            data["system"] = get_system()

            if self._simvue and not self._simvue.update(data):
                return False

        self._start_time = time.time()

        if self._pid == 0:
            self._pid = os.getpid()

        self._parent_process = psutil.Process(self._pid) if self._pid else None

        self._shutdown_event = threading.Event()
        self._heartbeat_termination_trigger = threading.Event()

        try:
            self._dispatcher = Dispatcher(
                mode=self._dispatch_mode,
                termination_trigger=self._shutdown_event,
                object_types=["events", "metrics"],
                callback=self._create_dispatch_callback(),
            )

            self._heartbeat_thread = threading.Thread(
                target=self._create_heartbeat_callback()
            )

        except RuntimeError as e:
            self._error(e.args[0])
            return False

        self._active = True

        self._dispatcher.start()
        self._heartbeat_thread.start()

        return True

    def _error(self, message: str, join_threads: bool = True) -> None:
        """Raise an exception if necessary and log error

        Parameters
        ----------
        message : str
            message to display in exception or logger message
        join_threads : bool
            whether to join the threads on failure. This option exists to
            prevent join being called in nested thread calls to this function.

        Raises
        ------
        RuntimeError
            exception throw
        """
        # Stop heartbeat
        if self._heartbeat_termination_trigger and self._heartbeat_thread:
            self._heartbeat_termination_trigger.set()
            if join_threads:
                self._heartbeat_thread.join()

        # Finish stopping all threads
        if self._shutdown_event:
            self._shutdown_event.set()

        # Purge the queue as we can no longer send metrics
        if self._dispatcher and self._dispatcher.is_alive():
            self._dispatcher.purge()
            if join_threads:
                self._dispatcher.join()

        if not self._suppress_errors:
            raise RuntimeError(message)
        else:
            # Simvue support now terminated as the instance of Run has entered
            # the dormant state due to exception throw so set listing to be 'lost'
            if self._status == "running" and self._simvue:
                self._simvue.update({"name": self._name, "status": "lost"})

            logger.error(message)

        self._aborted = True

    @skip_if_failed("_aborted", "_suppress_errors", False)
    @pydantic.validate_call
    def init(
        self,
        name: typing.Optional[str] = None,
        metadata: typing.Optional[dict[str, typing.Any]] = None,
        tags: typing.Optional[list[str]] = None,
        description: typing.Optional[str] = None,
        folder: str = "/",
        running: bool = True,
        retention_period: typing.Optional[str] = None,
        resources_metrics_interval: typing.Optional[int] = HEARTBEAT_INTERVAL,
        visibility: typing.Union[
            typing.Literal["public", "tenant"], list[str], None
        ] = None,
    ) -> bool:
        """Initialise a Simvue run

        Parameters
        ----------
        name : typing.Optional[str], optional
            the name to allocate this run, if not specified a name will be
            selected at random, by default None
        metadata : typing.Optional[dict[str, typing.Any]], optional
            any metadata relating to the run as key-value pairs, by default None
        tags : typing.Optional[list[str]], optional
            a list of tags for this run, by default None
        description : typing.Optional[str], optional
            description of the run, by default None
        folder : str, optional
            folder within which to store the run, by default "/"
        running : bool, optional
            whether to set the status as running or created, the latter implying
            the run will be commenced at a later time. Default is True.
        retention_period : typing.Optional[str], optional
            describer for time period to retain run, the default of None
            removes this constraint.
        resources_metrics_interval : int, optional
            how often to publish resource metrics, if None these will not be published
        visibility : Literal['public', 'tenant'] | list[str], optional
            set visibility options for this run, either:
                * public: run viewable to all.
                * tenant: run viewable to all within the current tenant.
                * A list of usernames with which to share this run

        Returns
        -------
        bool
            whether the initialisation was successful
        """

        if isinstance(visibility, str) and visibility not in ("public", "tenant"):
            self._error(
                "invalid visibility option, must be either None, 'public', 'tenant' or a list of users"
            )

        if self._mode not in ("online", "offline", "disabled"):
            self._error("invalid mode specified, must be online, offline or disabled")
            return False

        if self._mode == "disabled":
            return True

        if not self._token or not self._url:
            self._error(
                "Unable to get URL and token from environment variables or config file"
            )
            return False

        if name and not re.match(r"^[a-zA-Z0-9\-\_\s\/\.:]+$", name):
            self._error("specified name is invalid")
            return False

        self._resources_metrics_interval = resources_metrics_interval

        self._name = name

        self._status = "running" if running else "created"

        # Parse the time to live/retention time if specified
        try:
            if retention_period:
                retention_secs: typing.Optional[int] = int(
                    humanfriendly.parse_timespan(retention_period)
                )
            else:
                retention_secs = None
        except humanfriendly.InvalidTimespan as e:
            self._error(e.args[0])
            return False

        data: dict[str, typing.Any] = {
            "metadata": (metadata or {}) | git_info(os.getcwd()),
            "tags": tags or [],
            "status": self._status,
            "ttl": retention_secs,
            "folder": folder,
            "name": name,
            "description": description,
            "system": get_system()
            if self._status == "running"
            else {"cpu": {}, "gpu": {}, "platform": {}},
            "visibility": {
                "users": [] if not isinstance(visibility, list) else visibility,
                "tenant": visibility == "tenant",
                "public": visibility == "public",
            },
        }

        # Check against the expected run input
        try:
            RunInput(**data)
        except ValidationError as err:
            self._error(f"{err}")
            return False

        self._simvue = Simvue(self._name, self._uuid, self._mode, self._suppress_errors)
        name, self._id = self._simvue.create_run(data)

        self._data = data

        if not name:
            return False
        elif name is not True:
            self._name = name

        if self._status == "running":
            self._start()

        if self._mode == "online":
            click.secho(f"[simvue] Run {self._name} created", bold=True, fg="green")
            click.secho(
                f"[simvue] Monitor in the UI at {self._url}/dashboard/runs/run/{self._id}",
                bold=True,
                fg="green",
            )

        return True

    @skip_if_failed("_aborted", "_suppress_errors", None)
    @pydantic.validate_call(config={"arbitrary_types_allowed": True})
    def add_process(
        self,
        identifier: str,
        *cmd_args,
        executable: typing.Optional[str] = None,
        script: typing.Optional[str] = None,
        input_file: typing.Optional[str] = None,
        completion_callback: typing.Optional[
            typing.Callable[[int, str, str], None]
        ] = None,
        completion_trigger: typing.Optional[multiprocessing.synchronize.Event] = None,
        env: typing.Optional[typing.Dict[str, str]] = None,
        **cmd_kwargs,
    ) -> None:
        """Add a process to be executed to the executor.

        This process can take many forms, for example a be a set of positional arguments:

        ```python
        executor.add_process("my_process", "ls", "-ltr")
        ```

        Provide explicitly the components of the command:

        ```python
        executor.add_process("my_process", executable="bash", debug=True, c="return 1")
        executor.add_process("my_process", executable="bash", script="my_script.sh", input="parameters.dat")
        ```

        or a mixture of both. In the latter case arguments which are not 'executable', 'script', 'input'
        are taken to be options to the command, for flags `flag=True` can be used to set the option and
        for options taking values `option=value`.

        When the process has completed if a function has been provided for the `completion_callback` argument
        this will be called, this callback is expected to take the following form:

        ```python
        def callback_function(status_code: int, std_out: str, std_err: str) -> None:
            ...
        ```

        Note `completion_callback` is not supported on Windows operating systems.

        Alternatively you can use `completion_trigger` to create a multiprocessing event which will be set
        when the process has completed.

        Parameters
        ----------
        identifier : str
            A unique identifier for this process
        executable : str | None, optional
            the main executable for the command, if not specified this is taken to be the first
            positional argument, by default None
        *positional_arguments
            all other positional arguments are taken to be part of the command to execute
        script : str | None, optional
            the script to run, note this only work if the script is not an option, if this is the case
            you should provide it as such and perform the upload manually, by default None
        input_file : str | None, optional
            the input file to run, note this only work if the input file is not an option, if this is the case
            you should provide it as such and perform the upload manually, by default None
        completion_callback : typing.Callable | None, optional
            callback to run when process terminates (not supported on Windows)
        completion_trigger : multiprocessing.Event | None, optional
            this trigger event is set when the processes completes
        env : typing.Dict[str, str], optional
            environment variables for process
        **kwargs
            all other keyword arguments are interpreted as options to the command
        """
        if platform.system() == "Windows" and completion_callback:
            raise RuntimeError(
                "Use of 'completion_callback' on Windows based operating systems is unsupported "
                "due to function pickling restrictions for multiprocessing"
            )

        _cmd_list: typing.List[str] = []
        _pos_args = list(cmd_args)

        # Assemble the command for saving to metadata as string
        if executable:
            _cmd_list += [executable]
        else:
            _cmd_list += [_pos_args[0]]
            executable = _pos_args[0]
            _pos_args.pop(0)

        for kwarg, val in cmd_kwargs.items():
            _quoted_val: str = f'"{val}"'
            if len(kwarg) == 1:
                if isinstance(val, bool) and val:
                    _cmd_list += [f"-{kwarg}"]
                else:
                    _cmd_list += [f"-{kwarg}{(' '+ _quoted_val) if val else ''}"]
            else:
                if isinstance(val, bool) and val:
                    _cmd_list += [f"--{kwarg}"]
                else:
                    _cmd_list += [f"--{kwarg}{(' '+_quoted_val) if val else ''}"]

        _cmd_list += _pos_args
        _cmd_str = " ".join(_cmd_list)

        # Store the command executed in metadata
        self.update_metadata({f"{identifier}_command": _cmd_str})

        # Add the process to the executor
        self._executor.add_process(
            identifier,
            *_pos_args,
            executable=executable,
            script=script,
            input_file=input_file,
            completion_callback=completion_callback,
            completion_trigger=completion_trigger,
            env=env,
            **cmd_kwargs,
        )

    @pydantic.validate_call
    def kill_process(self, process_id: str) -> None:
        """Kill a running process by ID

        Parameters
        ----------
        process_id : str
            the unique identifier for the added process
        """
        self._executor.kill_process(process_id)

    def kill_all_processes(self) -> None:
        """Kill all currently running processes."""
        self._executor.kill_all()

    @property
    def executor(self) -> Executor:
        """Return the executor for this run"""
        return self._executor

    @property
    def name(self) -> typing.Optional[str]:
        """Return the name of the run"""
        return self._name

    @property
    def uid(self) -> str:
        """Return the local unique identifier of the run"""
        return self._uuid

    @property
    def id(self) -> typing.Optional[str]:
        """Return the unique id of the run"""
        return self._id

    @pydantic.validate_call
    @skip_if_failed("_aborted", "_suppress_errors", False)
    @pydantic.validate_call
    def reconnect(self, run_id: str) -> bool:
        """Reconnect to a run in the created state

        Parameters
        ----------
        run_id : str
            identifier of run to connect to
        uid : typing.Optional[str], optional
            unique identifier for this run, by default None

        Returns
        -------
        bool
            _description_
        """
        if self._mode == "disabled":
            return True

        self._status = "running"

        self._id = run_id
        self._simvue = Simvue(self._name, self._id, self._mode, self._suppress_errors)
        self._start(reconnect=True)

        return True

    @skip_if_failed("_aborted", "_suppress_errors", None)
    @pydantic.validate_call
    def set_pid(self, pid: int) -> None:
        """Set pid of process to be monitored

        Parameters
        ----------
        pid : int
            PID of the process to be monitored
        """
        self._pid = pid

    @pydantic.validate_call
    @skip_if_failed("_aborted", "_suppress_errors", False)
    @pydantic.validate_call
    def config(
        self,
        *,
        suppress_errors: typing.Optional[bool] = None,
        queue_blocking: typing.Optional[bool] = None,
        resources_metrics_interval: typing.Optional[int] = None,
        disable_resources_metrics: typing.Optional[bool] = None,
        storage_id: typing.Optional[str] = None,
    ) -> bool:
        """Optional configuration

        Parameters
        ----------
        suppress_errors : bool, optional
            disable exception throwing instead putting Simvue into a
            dormant state if an error occurs
        queue_blocking : bool, optional
            block thread queues during metric/event recording
        resource_metrics_interval : int, optional
            frequency at which to collect resource metrics
        disable_resources_metrics : bool, optional
            disable monitoring of resource metrics
        storage_id : str, optional
            identifier of storage to use, by default None

        Returns
        -------
        bool
            _description_
        """

        with self._configuration_lock:
            if suppress_errors is not None:
                self._suppress_errors = suppress_errors

            if queue_blocking is not None:
                self._queue_blocking = queue_blocking

            if resources_metrics_interval and disable_resources_metrics:
                self._error(
                    "Setting of resource metric interval and disabling resource metrics is ambiguous"
                )
                return False

            if disable_resources_metrics:
                self._pid = None
                self._resources_metrics_interval = None

            if resources_metrics_interval:
                self._resources_metrics_interval = resources_metrics_interval

            if storage_id:
                self._storage_id = storage_id

        return True

    @skip_if_failed("_aborted", "_suppress_errors", False)
    @pydantic.validate_call
    def update_metadata(self, metadata: dict[str, typing.Any]) -> bool:
        """
        Add/update metadata
        """

        if self._mode == "disabled":
            return True

        if not self._simvue:
            self._error("Cannot update metadata, run not initialised")
            return False

        if not isinstance(metadata, dict):
            self._error("metadata must be a dict")
            return False

        data: dict[str, dict[str, typing.Any]] = {"metadata": metadata}

        if self._simvue and self._simvue.update(data):
            return True

        return False

    @skip_if_failed("_aborted", "_suppress_errors", False)
    @pydantic.validate_call
    def update_tags(self, tags: list[str]) -> bool:
        """
        Add/update tags
        """
        if self._mode == "disabled":
            return True

        if not self._simvue:
            self._error("Cannot update tags, run not initialised")
            return False

        data: dict[str, list[str]] = {"tags": tags}

        if self._simvue and self._simvue.update(data):
            return True

        return False

    @pydantic.validate_call
    @skip_if_failed("_aborted", "_suppress_errors", False)
    @pydantic.validate_call
    def log_event(self, message, timestamp: typing.Optional[str] = None) -> bool:
        """
        Write event
        """
        if self._mode == "disabled":
            self._error("Cannot log events in 'disabled' state")
            return True

        if not self._simvue or not self._dispatcher:
            self._error("Cannot log events, run not initialised")
            return False

        if not self._active:
            self._error("Run is not active")
            return False

        if self._status != "running":
            self._error("Cannot log events when not in the running state")
            return False

        if timestamp and not validate_timestamp(timestamp):
            self._error("Invalid timestamp format")
            return False

        _data = {"message": message, "timestamp": timestamp or self.time_stamp}
        self._dispatcher.add_item(_data, "events", self._queue_blocking)

        return True

    def _add_metrics_to_dispatch(
        self,
        metrics: dict[str, typing.Union[int, float]],
        step: typing.Optional[int] = None,
        time: typing.Optional[int] = None,
        timestamp: typing.Optional[str] = None,
        join_on_fail: bool = True,
    ) -> bool:
        if self._mode == "disabled":
            return True

        # If there are no metrics to log just ignore
        if not metrics:
            return True

        if not self._simvue or not self._dispatcher:
            self._error("Cannot log metrics, run not initialised", join_on_fail)
            return False

        if not self._active:
            self._error("Run is not active", join_on_fail)
            return False

        if self._status != "running":
            self._error(
                "Cannot log metrics when not in the running state", join_on_fail
            )
            return False

        if timestamp and not validate_timestamp(timestamp):
            self._error("Invalid timestamp format", join_on_fail)
            return False

        _data: dict[str, typing.Any] = {
            "values": metrics,
            "time": time if time is not None else self.duration,
            "timestamp": timestamp if timestamp is not None else self.time_stamp,
            "step": step if step is not None else self._step,
        }
        self._dispatcher.add_item(_data, "metrics", self._queue_blocking)

        return True

    @pydantic.validate_call
    @skip_if_failed("_aborted", "_suppress_errors", False)
    @pydantic.validate_call
    def log_metrics(
        self,
        metrics: dict[str, typing.Union[int, float]],
        step: typing.Optional[int] = None,
        time: typing.Optional[int] = None,
        timestamp: typing.Optional[str] = None,
    ) -> bool:
        """
        Write metrics
        """
        add_dispatch = self._add_metrics_to_dispatch(
            metrics, step=step, time=time, timestamp=timestamp
        )
        self._step += 1
        return add_dispatch

    @skip_if_failed("_aborted", "_suppress_errors", False)
    @pydantic.validate_call
    def save_object(
        self,
        obj: typing.Any,
        category: typing.Literal["input", "output", "code"],
        name: str,
        allow_pickle: bool = False,
    ) -> bool:
        serialized = serialize_object(obj, allow_pickle)

        if not serialized or not (pickled := serialized[0]):
            self._error(f"Failed to serialize '{obj}'")
            return False

        data_type = serialized[1]

        if not data_type and not allow_pickle:
            self._error("Unable to save Python object, set allow_pickle to True")
            return False

        data: dict[str, typing.Any] = {
            "pickled": pickled,
            "type": data_type,
            "checksum": calculate_sha256(pickled, False),
            "originalPath": "",
            "size": sys.getsizeof(pickled),
            "run": self._name,
            "category": category,
            "storage": self._storage_id,
        }

        if name:
            data["name"] = name

        # Register file
        return self._simvue is not None and self._simvue.save_file(data) is not None

    @skip_if_failed("_aborted", "_suppress_errors", False)
    @pydantic.validate_call
    def save_file(
        self,
        filename: pydantic.FilePath,
        category: typing.Literal["input", "output", "code"],
        filetype: typing.Optional[str] = None,
        preserve_path: bool = False,
        name: typing.Optional[str] = None,
    ) -> bool:
        """Upload file to the server

        Parameters
        ----------
        filename : str
            path to the file to upload
        category : Literal['input', 'output', 'code']
            category of file with respect to this run
        filetype : str, optional
            the MIME file type else this is deduced, by default None
        preserve_path : bool, optional
            whether to preserve the path during storage, by default False
        name : str, optional
            name to associate with this file, by default None

        Returns
        -------
        bool
            whether the upload was successful
        """
        if self._mode == "disabled":
            return True

        if not self._simvue:
            self._error("Cannot save files, run not initialised")
            return False

        if self._status == "created" and category == "output":
            self._error("Cannot upload output files for runs in the created state")
            return False

        mimetypes.init()
        mimetypes_valid = ["application/vnd.plotly.v1+json"]
        mimetypes_valid += list(mimetypes.types_map.values())

        if filetype and filetype not in mimetypes_valid:
            self._error(f"Invalid MIME type '{filetype}' specified")
            return False

        stored_file_name: str = f"{filename}"

        if preserve_path and stored_file_name.startswith("./"):
            stored_file_name = stored_file_name[2:]
        elif not preserve_path:
            stored_file_name = os.path.basename(filename)

        # Determine mimetype
        if not (mimetype := filetype):
            mimetype = mimetypes.guess_type(filename)[0] or "application/octet-stream"

        data: dict[str, typing.Any] = {
            "name": name or stored_file_name,
            "run": self._name,
            "type": mimetype,
            "storage": self._storage_id,
            "category": category,
            "size": (file_size := os.path.getsize(filename)),
            "originalPath": os.path.abspath(
                os.path.expanduser(os.path.expandvars(filename))
            ),
            "checksum": calculate_sha256(f"{filename}", True),
        }

        if not file_size:
            click.secho(
                "WARNING: saving zero-sized files not currently supported",
                bold=True,
                fg="yellow",
            )
            return True

        # Register file
        logger.debug(f"Saving file '{data['originalPath']}'")
        return self._simvue.save_file(data) is not None

    @pydantic.validate_call
    @skip_if_failed("_aborted", "_suppress_errors", False)
    @pydantic.validate_call
    def save_directory(
        self,
        directory: pydantic.DirectoryPath,
        category: typing.Literal["output", "input", "code"],
        contents_filetype: typing.Optional[str] = None,
        preserve_path: bool = False,
    ) -> bool:
        """Upload contents of a directory

        Parameters
        ----------
        directory : str
            path of directory from which to extract contents
        category : Literal['input', 'output', 'code']
            category of files with respect to this run
        contents_filetype : typing.Optional[str], optional
            the MIME file type of all matching files else this is deduced on
            a per file basis, by default None
        preserve_path : bool, optional
            whether to preserve the path during storage, by default False

        Returns
        -------
        bool
            if the upload of all files was successful
        """
        if self._mode == "disabled":
            return True

        if not self._simvue:
            self._error("Cannot save directory, run not initialised")
            return False

        for dirpath, _, filenames in directory.walk():
            for filename in filenames:
                # If any file fails to upload then abort
                if (
                    full_path := dirpath.joinpath(filename)
                ).is_file() and not self.save_file(
                    full_path, category, contents_filetype, preserve_path
                ):
                    return False

        return True

    @pydantic.validate_call
    @skip_if_failed("_aborted", "_suppress_errors", False)
    @pydantic.validate_call
    def save_all(
        self,
        items: list[typing.Union[pydantic.FilePath, pydantic.DirectoryPath]],
        category: typing.Literal["input", "output", "code"],
        contents_filetype: typing.Optional[str] = None,
        preserve_path: bool = False,
    ) -> bool:
        """Save the list of files and/or directories

        Parameters
        ----------
        items : list[str]
            paths to files/directories to upload
        category : Literal['input', 'output', 'code']
            category of files with respect to this run
        contents_filetype : typing.Optional[str], optional
            the MIME file type of all matching files else this is deduced on
            a per file basis, by default None
        preserve_path : bool, optional
            whether to preserve the path during storage, by default False

        Returns
        -------
        bool
            whether the upload was successful
        """
        success: bool = True

        if self._mode == "disabled":
            return success

        for item in items:
            if item.is_file():
                save_file = self.save_file(
                    item, category, contents_filetype, preserve_path
                )
            elif item.is_dir():
                save_file = self.save_directory(
                    item, category, contents_filetype, preserve_path
                )
            else:
                self._error(f"{item}: No such file or directory")
                save_file = False
            if not save_file:
                return False

        return True

    @skip_if_failed("_aborted", "_suppress_errors", False)
    @pydantic.validate_call
    def set_status(
        self, status: typing.Literal["completed", "failed", "terminated"]
    ) -> bool:
        """Set run status
            status to assign to this run

        Returns
        -------
        bool
            if status update was successful
        """
        if self._mode == "disabled":
            return True

            self._error("Cannot update run status, run is not initialised.")
            return False

        if not self._active:
            self._error("Run is not active")
            return False

        data: dict[str, str] = {"name": self._name, "status": status}
        self._status = status

        if self._simvue.update(data):
            return True

        return False

    @skip_if_failed("_aborted", "_suppress_errors", False)
    def close(self) -> bool:
        """Close the run"""
        self._executor.wait_for_completion()
        if self._mode == "disabled":
            return True

        if not self._simvue:
            self._error("Cannot close run, not initialised")
            return False

        if not self._active:
            self._error("Run is not active")
            return False

        if self._heartbeat_thread and self._heartbeat_termination_trigger:
            self._heartbeat_termination_trigger.set()
            self._heartbeat_thread.join()

        if self._shutdown_event:
            self._shutdown_event.set()

        if self._status != "failed":
            if self._dispatcher:
                self._dispatcher.join()
            self.set_status("completed")
        elif self._dispatcher:
            self._dispatcher.purge()
            self._dispatcher.join()

        if _non_zero := self.executor.exit_status:
            logger.error(
                f"Simvue process executor terminated with non-zero exit status {_non_zero}"
            )
            sys.exit(_non_zero)

        return True

    @skip_if_failed("_aborted", "_suppress_errors", False)
    @pydantic.validate_call
    def set_folder_details(
        self,
        path: str,
        metadata: typing.Optional[dict[str, typing.Union[int, str, float]]] = None,
        tags: typing.Optional[list[str]] = None,
        description: typing.Optional[str] = None,
    ) -> bool:
        """Add metadata to the specified folder

        Parameters
        ----------
        path : str
            folder path
        metadata : dict[str, int | str | float], optional
            additional metadata to attach to this folder, by default None
        tags : list[str], optional
            list of tags to assign to the folder, by default None
        description : str, optional
            description to assign to this folder, by default None

        Returns
        -------
        bool
            returns True if update was successful
        """
        if self._mode == "disabled":
            return True

        if not self._simvue:
            self._error("Cannot update folder details, run was not initialised")
            return False

        if not self._active:
            self._error("Run is not active")
            return False

        data: dict[str, typing.Any] = {"path": path}

        if metadata:
            data["metadata"] = metadata or {}

        if tags:
            data["tags"] = tags or []

        if description:
            data["description"] = description

        if self._simvue.set_folder_details(data):
            return True

        return False

    @pydantic.validate_call
    @skip_if_failed("_aborted", "_suppress_errors", False)
    @pydantic.validate_call
    def add_alerts(
        self,
        ids: typing.Optional[list[str]] = None,
        names: typing.Optional[list[str]] = None,
    ) -> bool:
        """Add a set of existing alerts to this run by name or id

        Parameters
        ----------
        ids : typing.Optional[list[str]], optional
            unique identifiers of the alerts to attach, by default None
        names : typing.Optional[list[str]], optional
            names of alerts to attach, by default None

        Returns
        -------
        bool
            returns True if successful
        """
        if not self._simvue:
            self._error("Cannot add alerts, run not initialised")
            return False

        ids = ids or []
        names = names or []

        if names and not ids:
            if alerts := self._simvue.list_alerts():
                for alert in alerts:
                    if alert["name"] in names:
                        ids.append(alert["id"])
            else:
                self._error("No existing alerts")
                return False
        elif not names and not ids:
            self._error("Need to provide alert ids or alert names")
            return False

        data: dict[str, typing.Any] = {"id": self._id, "alerts": ids}

        if self._simvue.update(data):
            return True

        return False

    @pydantic.validate_call
    @skip_if_failed("_aborted", "_suppress_errors", None)
    @pydantic.validate_call
    def create_alert(
        self,
        name: str,
        source: typing.Literal["events", "metrics", "user"] = "metrics",
        description: typing.Optional[str] = None,
        frequency: typing.Optional[pydantic.PositiveInt] = None,
        window: pydantic.PositiveInt = 5,
        rule: typing.Optional[
            typing.Literal[
                "is above", "is below", "is inside range", "is outside range"
            ]
        ] = None,
        metric: typing.Optional[str] = None,
        threshold: typing.Optional[float] = None,
        range_low: typing.Optional[float] = None,
        range_high: typing.Optional[float] = None,
        aggregation: typing.Optional[
            typing.Literal["average", "sum", "at least one", "all"]
        ] = "average",
        notification: typing.Literal["email", "none"] = "none",
        pattern: typing.Optional[str] = None,
    ) -> typing.Optional[str]:
        """Creates an alert with the specified name (if it doesn't exist)
        and applies it to the current run. If alert already exists it will
        not be duplicated.

        Note available arguments depend on the alert source:

        Event
        =====

        Alerts triggered based on the contents of an event message, arguments are:
            - frequency
            - pattern

        Metrics
        =======

        Alerts triggered based on metric value condictions, arguments are:
            - frequency
            - rule
            - window
            - aggregation
            - metric
            - threshold / (range_low, range_high)

        User
        ====

        User defined alerts, manually triggered.

        Parameters
        ----------
        name : str
            name of alert
        source : Literal['events', 'metrics', 'user'], optional
            the source which triggers this alert based on status, either
            event based, metric values or manual user defined trigger. By default "metrics".
        description : str, optional
            description for this alert
        frequency : PositiveInt, optional
            frequency at which to check alert condition in seconds, by default None
        window : PositiveInt, optional
            time period in seconds over which metrics are averaged, by default 5
        rule : Literal['is above', 'is below', 'is inside', 'is outside range'], optional
            rule defining metric based alert conditions, by default None
        metric : str, optional
            metric to monitor, by default None
        threshold : float, optional
            the threshold value if 'rule' is 'is below' or 'is above', by default None
        range_low : float, optional
            the lower bound value if 'rule' is 'is inside range' or 'is outside range', by default None
        range_high : float, optional
            the upper bound value if 'rule' is 'is inside range' or 'is outside range', by default None
        aggregation : Literal['average', 'sum', 'at least one', 'all'], optional
            method to use when aggregating metrics within time window, default 'average'.
        notification : Literal['email', 'none'], optional
            whether to notify on trigger, by default "none"
        pattern : str, optional
            for event based alerts pattern to look for, by default None

        Returns
        -------
        str | None
            returns the created alert ID if successful
        """
        if self._mode == "disabled":
            return None

        if not self._simvue:
            self._error("Cannot add alert, run not initialised")
            return None

        if rule in ("is below", "is above") and threshold is None:
            self._error("threshold must be defined for the specified alert type")
            return None

        if rule in ("is outside range", "is inside range") and (
            range_low is None or range_high is None
        ):
            self._error(
                "range_low and range_high must be defined for the specified alert type"
            )
            return None

        alert_definition = {}

        if source == "metrics":
            alert_definition["aggregation"] = aggregation
            alert_definition["metric"] = metric
            alert_definition["window"] = window
            alert_definition["rule"] = rule
            alert_definition["frequency"] = frequency
            if threshold is not None:
                alert_definition["threshold"] = threshold
            elif range_low is not None and range_high is not None:
                alert_definition["range_low"] = range_low
                alert_definition["range_high"] = range_high
        elif source == "events":
            alert_definition["pattern"] = pattern
            alert_definition["frequency"] = frequency
        else:
            alert_definition = None

        alert: dict[str, typing.Any] = {
            "name": name,
            "notification": notification,
            "source": source,
            "alert": alert_definition,
            "description": description,
        }

        # Check if the alert already exists
        alert_id: typing.Optional[str] = None
        alerts = self._simvue.list_alerts()
        if alerts:
            for existing_alert in alerts:
                if existing_alert["name"] == alert["name"]:
                    if compare_alerts(existing_alert, alert):
                        alert_id = existing_alert["id"]
                        logger.info("Existing alert found with id: %s", alert_id)
                        break

        if not alert_id:
            response = self._simvue.add_alert(alert)
            if response:
                if "id" in response:
                    alert_id = response["id"]
            else:
                self._error("unable to create alert")
                return None

        if alert_id:
            # TODO: What if we keep existing alerts/add a new one later?
            data = {"id": self._id, "alerts": [alert_id]}
            self._simvue.update(data)

        return alert_id

    @pydantic.validate_call
    @skip_if_failed("_aborted", "_suppress_errors", False)
    @pydantic.validate_call
    def log_alert(
        self, identifier: str, state: typing.Literal["ok", "critical"]
    ) -> bool:
        """Set the state of an alert

        Parameters
        ----------
        identifier : str
            identifier of alert to update
        state : Literal['ok', 'critical']
            state to set alert to

        Returns
        -------
        bool
            whether alert state update was successful
        """
        if state not in ("ok", "critical"):
            self._error('state must be either "ok" or "critical"')
            return False
        if not self._simvue:
            self._error("Cannot log alert, run not initialised")
            return False
        self._simvue.set_alert_state(identifier, state)

        return True
