# Simvue Python client

Collects metadata, metrics and files from simulations in real time.

## Configuration
The service URL and token can be defined as environment variables:
```
export SIMVUE_URL=...
export SIMVUE_TOKEN=...
```
or a file `simvue.ini` can be created containing:
```
[server]
url = ...
token = ...
```
The exact contents of both of the above options can be obtained directly by clicking the **Create new run** button on the web UI. Note that the environment variables have preference over the config file.

## Usage example
```
from simvue import Simvue

...

# Using a context manager means that the status will be set to completed automatically
with Simvue() as run:

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
    run.save('training.py', 'code')

    # Upload an input file
    run.save('params.in', 'input')

    # Add an alert (the alert definition will be created if necessary)
    run.add_alert('loss-too-high',      # Name
                  'is above',           # Type
                  'loss',               # Metric
                  1,                    # Frequency
                  1,                    # Window
                  threshold=10,         # Threshold
		  notification='email') # Notification type 

    ...

    while not converged:

        ...

        # Send metrics inside main application loop
        run.log({'loss': 0.5, 'density': 34.4})
	
	...

    # Upload an output file
    run.save('output.cdf', 'output')
    
    # If we weren't using a context manager we'd need to end the run
    # run.close()
```
