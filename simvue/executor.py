"""
Simvue Client Executor
======================

Adds functionality for executing commands from the command line as part of a Simvue run, the executor
monitors the exit code of the command setting the status to failure if non-zero.
Stdout and Stderr are sent to Simvue as artifacts.
"""

__author__ = "Kristian Zarebski"
__date__ = "2023-11-15"

import typing
import subprocess
import multiprocessing
import logging
import sys
import os

if typing.TYPE_CHECKING:
    import simvue

logger = logging.getLogger(__name__)


class Executor:
    """Command Line command executor
    
    Adds execution of command line commands as part of a Simvue run, the status of these commands is monitored
    and if non-zero cause the Simvue run to be stated as 'failed'. The executor accepts commands either as a
    set of positional arguments or more specifically as components, two of these 'input_file' and 'script' then
    being used to set the relevant metadata within the Simvue run itself.
    """
    def __init__(self, simvue_runner: "simvue.Run", keep_logs: bool=False) -> None:
        """Initialise an instance of the Simvue executor attaching it to a Run.

        Parameters
        ----------
        simvue_runner : simvue.Run
            An instance of the Simvue runner used to send command execution feedback
        keep_logs : bool, optional
            whether to keep the stdout and stderr logs locally, by default False
        """
        self._runner = simvue_runner
        self._keep_logs = keep_logs
        self._manager = multiprocessing.Manager()
        self._exit_codes = self._manager.dict()
        self._std_err = self._manager.dict()
        self._std_out = self._manager.dict()
        self._command_str: typing.Dict[str, str] = {}
        self._processes: typing.Dict[str, multiprocessing.Process] = {}


    def add_process(
        self,
        identifier: str,
        *args,
        executable: typing.Optional[str]= None,
        script: typing.Optional[str]= None,
        input_file: typing.Optional[str]= None,
        print_stdout: bool=False,
        env: typing.Optional[typing.Dict[str, str]] = None,
        completion_callback: typing.Optional[typing.Callable[[int, str, str], None]]=None,
        **kwargs,
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

        Parameters
        ----------
        identifier : str
            A unique identifier for this process
        print_stdout : bool, optional
            print output of command to stdout
        executable : str | None, optional
            the main executable for the command, if not specified this is taken to be the first
            positional argument, by default None
        script : str | None, optional
            the script to run, note this only work if the script is not an option, if this is the case
            you should provide it as such and perform the upload manually, by default None
        input_file : str | None, optional
            the input file to run, note this only work if the input file is not an option, if this is the case
            you should provide it as such and perform the upload manually, by default None
        env : typing.Dict[str, str], optional
            environment variables for process
        completion_callback : typing.Callable | None, optional
            callback to run when process terminates
        """
        _alert_kwargs = {
            k.replace("__", ""): v for k, v in kwargs.items() if k.startswith("__")
        }

        _pos_args = list(args)

        if script:
            self._runner.save(filename=script, category="code")

        if input_file:
            self._runner.save(filename=input_file, category="input")

        self._runner.add_alert(
            f"{identifier} Status",
            source="events",
            pattern="non-zero exit code",
            **_alert_kwargs,
        )

        def _exec_process(
            proc_id: str,
            command: typing.List[str],
            runner: "simvue.Run",
            exit_status_dict: typing.Dict[str, int],
            std_err: typing.Dict[str, str],
            std_out: typing.Dict[str, str],
            run_on_exit: typing.Optional[typing.Callable[[int, int, str], None]]=completion_callback,
            print_out: bool=print_stdout,
            environment: typing.Optional[typing.Dict[str, str]]=env
        ) -> None:
            _logger = logging.getLogger(proc_id)
            with open(f"{runner.name}_{proc_id}.err", "w") as err:
                with open(f"{runner.name}_{proc_id}.out", "w") as out:
                    _result = subprocess.Popen(
                        command,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        universal_newlines=True,
                        env=environment
                    )
                    
                    while True:
                        _std_out_line = _result.stdout.readline()
                        _std_err_line = _result.stderr.readline()

                        if _std_out_line:
                            out.write(_std_out_line)
                            if print_out:
                                _logger.info(_std_out_line)

                        if _std_err_line:
                            err.write(_std_err_line)
                            if print_out:
                                _logger.error(_std_err_line)
                        
                        if not _std_err_line and not _std_out_line:
                            break

            _status_code = _result.wait()

            with open(f"{runner.name}_{proc_id}.err") as err:
                std_err[proc_id] = err.read()

            with open(f"{runner.name}_{proc_id}.out") as out:
                std_out[proc_id] = out.read()

            exit_status_dict[proc_id] = _status_code

            run_on_exit(
                status_code=exit_status_dict[proc_id],
                std_out=std_out[proc_id],
                std_err=std_err[proc_id]
            )

            if run_on_exit:
                run_on_exit(
                    status_code=exit_status_dict[proc_id],
                    std_out=std_out[proc_id],
                    std_err=std_err[proc_id]
                )

        _command: typing.List[str] = []

        if executable:
            _command += [executable]
        else:
            _command += [_pos_args[0]]
            _pos_args.pop(0)

        if script:
            _command += [script]

        if input_file:
            _command += [input_file]

        for arg, value in kwargs.items():
            if arg.startswith("__"):
                continue
            if len(arg) == 1:
                if isinstance(value, bool) and value:
                    _command += [f"-{arg}"]
                else:
                    _command += [f"-{arg}", f"{value}"]
            else:
                if isinstance(value, bool) and value:
                    _command += [f"--{arg}"]
                else:
                    _command += [f"--{arg}", f"{value}"]
        
        _command += _pos_args

        self._command_str[identifier] = " ".join(_command)

        self._processes[identifier] = multiprocessing.Process(
            target=_exec_process,
            args=(
                identifier,
                _command,
                self._runner,
                self._exit_codes,
                self._std_err,
                self._std_out
            ),
        )
        logger.debug(f"Executing process: {' '.join(_command)}")
        self._processes[identifier].start()

    @property
    def success(self) -> int:
        """Return whether all attached processes completed successfully"""
        return all(i == 0 for i in self._exit_codes.values())
    
    @property
    def exit_status(self) -> int:
        """Returns the first non-zero exit status if applicable"""
        _non_zero = [i for i in self._exit_codes.values() if i != 0]

        if _non_zero:
            return _non_zero[0]
        
        return 0
    
    def get_command(self, process_id: str) -> str:
        """Returns the command executed within the given process.

        Parameters
        ----------
        process_id : str
            Unique identifier for the process

        Returns
        -------
        str
            command as a string
        """
        if process_id not in self._processes:
            raise KeyError(f"Failed to retrieve '{process_id}', no such process")
        return self._command_str[process_id]

    def _log_events(self) -> None:
        """Send log events for the result of each process"""
        for proc_id, code in self._exit_codes.items():
            if code != 0:
                _err = self._std_err[proc_id]
                _msg = f"Process {proc_id} returned non-zero exit code status {code} with:\n{_err}"
            else:
                _msg = f"Process {proc_id} completed successfully."
            self._runner.log_event(_msg)

    def _save_output(self) -> None:
        """Save the output to Simvue"""
        for proc_id in self._exit_codes.keys():
            # Only save the file if the contents are not empty
            if self._std_err[proc_id]:
                self._runner.save(f"{self._runner.name}_{proc_id}.err", category="output")
            if self._std_out[proc_id]:
                self._runner.save(f"{self._runner.name}_{proc_id}.out", category="output")
            
    def kill_process(self, process_id: str) -> None:
        """Kill a running process by ID"""
        if not (_process := self._processes.get(process_id)):
            logger.error(f"Failed to terminate process '{process_id}', no such identifier.")
            return
        _process.kill()

    def kill_all(self) -> None:
        """Kill all running processes"""
        for process in self._processes.values():
            process.kill()
    
    def _clear_cache_files(self) -> None:
        """Clear local log files if required"""
        if not self._keep_logs:
            for proc_id in self._exit_codes.keys():
                os.remove(f"{self._runner.name}_{proc_id}.err")
                os.remove(f"{self._runner.name}_{proc_id}.out")

    def wait_for_completion(self) -> None:
        """Wait for all processes to finish then perform tidy up and upload"""
        for process in self._processes.values():
            process.join()
        self._log_events()
        self._save_output()
        if not self.success:
            self._runner.set_status("failed")
        self._clear_cache_files()
