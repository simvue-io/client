import os
import logging
import typing
import multiparser
import multiparser.parsing.tail as mp_tail_parse
import requests
import zipfile
import contextlib
import uuid
import multiprocessing
import logging
import pathlib
    
import simvue.run as sv_run

EXAMPLE_DIR: str = pathlib.Path(__file__).parent

APPLICATION_URL: str = "https://github.com/su2code/SU2/releases/download/v7.0.2/SU2-v7.0.2-linux64-mpi.zip"

CONFIGURATION_FILE_URL: str = "https://raw.githubusercontent.com/su2code/Tutorials/master/compressible_flow/Inviscid_ONERAM6/inv_ONERAM6.cfg"

INPUT_FILE_URL: str = "https://github.com/su2code/Tutorials/raw/master/compressible_flow/Inviscid_ONERAM6/mesh_ONERAM6_inv_ffd.su2"

TUTORIAL_URL: str = "https://su2code.github.io/tutorials/Inviscid_ONERAM6/"

APPLICATION_LOCATION: str = os.path.join(EXAMPLE_DIR, "SU2_program")

BINARY_LOCATION: str = os.path.join(APPLICATION_LOCATION, "bin", "SU2_CFD")

# Name of history file to collect metrics from
HISTORY: str = 'history.csv'

# Check the history file for new entries at this interval (in secs)
POLLING_INTERVAL: int = 5

# Store these input files
INPUT_FILES: typing.Tuple[str, ...] = ('inv_ONERAM6.cfg', 'mesh_ONERAM6_inv_ffd.su2')

# Store these output files
OUTPUT_FILES: typing.Tuple[str, ...] = ('flow.vtk', 'surface_flow.vtk', 'restart_flow.dat')

# Collect these metadata attributes from the config file
METADATA_ATTRS: typing.Tuple[str, ...] = (
    'solver',
    'math_problem',
    'mach_number',
    'aoa',
    'sideslip_angle',
    'freestream_pressure',
    'freestream_temperature'
)

logging.basicConfig()
logger = logging.getLogger("Simvue.SU2")

def parse_config_file(config_file: str) -> typing.Dict[str, str]:
    with open(config_file) as in_f:
        _lines: typing.List[str] = in_f.readlines()
        _lines = [
            line for line in _lines
            if not line.startswith("%")
            and any(m.upper() in line for m in METADATA_ATTRS)
        ]
    
    _out_data: typing.Dict[str, str] = {}

    for line in _lines:
        _key, _value = line.split("=")
        _parsed_val = None

        with contextlib.suppress(ValueError):
            _parsed_val = int(_value.strip())
        with contextlib.suppress(ValueError):
            _parsed_val = _parsed_val or float(_value.strip())
        
        _out_data[_key.strip()] = _parsed_val or _value.strip()

    return _out_data


def run_simulation() -> None:
    _config: str = os.path.join(EXAMPLE_DIR, os.path.basename(CONFIGURATION_FILE_URL))
    _input: str = os.path.join(EXAMPLE_DIR, os.path.basename(INPUT_FILE_URL))

    if not os.path.isdir(APPLICATION_LOCATION):
        print("Could not find SU2 application, downloading...")
        _request_data = requests.get(APPLICATION_URL, stream=True)
        _zip_path = os.path.join(EXAMPLE_DIR, "su2_app.zip")
        with open(_zip_path, "wb") as download:
            for chunk in _request_data.iter_content(chunk_size=258):
                download.write(chunk)
        with zipfile.ZipFile(_zip_path, "r") as zip_f:
            zip_f.extractall(APPLICATION_LOCATION)
        os.remove(_zip_path)
        os.chmod(BINARY_LOCATION, os.stat(BINARY_LOCATION).st_mode | 0o111)

    if not os.path.exists(_config):
        print("Could not find configuration file, downloading...")
        _cfg_data = requests.get(CONFIGURATION_FILE_URL)
        with open(_config, "wb") as cfg:
            cfg.write(_cfg_data.content)

    if not os.path.exists(_input):
        print("Could not find input file, downloading...")
        _in_file_data = requests.get(INPUT_FILE_URL)
        with open(_input, "wb") as in_file:
            in_file.write(_in_file_data.content)

    print("Creating Simvue run...")
            
    with sv_run.Run() as run:
        run.config(suppress_errors=False)
        os.chdir(EXAMPLE_DIR)
        _meta_data: typing.Dict[str, typing.Any] = parse_config_file(_config)

        run.init(
            name=f"SU2_{os.path.splitext(os.path.basename(_config))[0]}_{str(uuid.uuid4())}",
            metadata=_meta_data,
            tags=["SU2"],
            description=f"SU2 tutorial {TUTORIAL_URL}"
        )

        for input_file in (_config, _input):
            run.save(
                filename=input_file,
                category="input",
                filetype="text/plain" if input_file.endswith(".cfg") else None
            )

        run.add_process(
            f"{run.name}",
            _config,
            executable=BINARY_LOCATION,
        )

        def try_metric_loc(data, _, run=run):
            run.log_metrics(data)

        # Stop all file monitors and SU2 if an exception is raised in any
        # Firstly define callback to terminate SU2, then set
        # file monitor to terminate all threads if one fails
        def exception_callback(_):
            run.kill_all_processes()

        with multiparser.FileMonitor(
            per_thread_callback=try_metric_loc,
            notification_callback=run.log_event,
            exception_callback=exception_callback,
            flatten_data=True,
            interval=5,
            log_level=logging.DEBUG,
            plain_logging=True,
            terminate_all_on_fail=True
        ) as monitor:
            monitor.tail(
                os.path.join(EXAMPLE_DIR, HISTORY),
                parser_func=mp_tail_parse.record_csv
            )

            # Don't parse the output files, just upload them
            for out_file in OUTPUT_FILES:
                _path = os.path.join(EXAMPLE_DIR, out_file)
                monitor.track(
                    _path,
                    parser_func=lambda *_, **__: ({}, {}), 
                    static=True, # Only read once when detected
                    callback=lambda path=_path, *_, **__: run.save(path, "output")
                )
            
            monitor.run()



if __name__ == "__main__":
    run_simulation()
