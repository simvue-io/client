# Simvue Python client

<br/>

<p align="center">
  <img src="https://raw.githubusercontent.com/simvue-io/client/dev/simvue-black.png" width="500" alt="Simvue" />
</p>

<p align="center">
Collect metadata, metrics and artifacts from simulations, processing and AI/ML training tasks running on any platform, in real time.
</p>

<div align="center">
<a href="https://github.com/simvue-io/client/blob/main/LICENSE" target="_blank"><img src="https://img.shields.io/github/license/simvue-io/client"/></a>
<img src="https://img.shields.io/badge/python-3.9%20%7C%203.10%20%7C%203.11%20%7C%203.12-blue">
<a href="https://pypi.org/project/simvue/" target="_blank"><img src="https://img.shields.io/pypi/v/simvue.svg"/></a>
<a href="https://pepy.tech/project/simvue"><img src="https://static.pepy.tech/badge/simvue"/></a>
<a href="https://github.com/astral-sh/ruff"><img src="https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json"></a>
</div>

<h3 align="center">
 <a href="https://simvue.io"><b>Website</b></a>
  •
  <a href="https://docs.simvue.io"><b>Documentation</b></a>
</h3>

## Configuration
The service URL and token can be defined as environment variables:
```sh
export SIMVUE_URL=...
export SIMVUE_TOKEN=...
```
or a file `simvue.ini` can be created containing:
```toml
[server]
url = "..."
token = "..."
```
The exact contents of both of the above options can be obtained directly by clicking the **Create new run** button on the web UI. Note that the environment variables have preference over the config file.

## Usage example
```python
from simvue import Run

...

if __name__ == "__main__":

    ...

    # Using a context manager means that the status will be set to completed automatically,
    # and also means that if the code exits with an exception this will be reported to Simvue
    with Run() as run:

        # Specify a run name, metadata (dict), tags (list), description, folder
        run.init('example-run-name',
                 {'learning_rate': 0.001, 'training_steps': 2000, 'batch_size': 32}, # Metadaata
                 ['tensorflow'],                                                     # Tags
                 'This is a test.',                                                  # Description
                 '/Project-A/part1')                                                 # Folder full path

        # Set folder details if necessary
        run.set_folder_details('/Project-A/part1',                     # Folder full path
                               metadata={},                            # Metadata
                               tags=['tensorflow'],                    # Tags
                               description='This is part 1 of a test') # Description

        # Upload the code
        run.save_file('training.py', 'code')

        # Upload an input file
        run.save_file('params.in', 'input')

        # Add an alert (the alert definition will be created if necessary)
        run.create_alert(name='loss-too-high',   # Name
                      source='metrics',       # Source
                      rule='is above',        # Rule
                      metric='loss',          # Metric
                      frequency=1,            # Frequency
                      window=1,               # Window
                      threshold=10,           # Threshold
                      notification='email')   # Notification type

        ...

        while not converged:

            ...

            # Send metrics inside main application loop
            run.log_metrics({'loss': 0.5, 'density': 34.4})

            ...

        # Upload an output file
        run.save_file('output.cdf', 'output')

        # If we weren't using a context manager we'd need to end the run
        # run.close()
```

## License

Released under the terms of the [Apache 2](https://github.com/simvue-io/client/blob/main/LICENSE) license.
