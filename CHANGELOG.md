# Change log

## [v1.0.1](https://github.com/simvue-io/client/releases/tag/v1.0.1) - 2024-07-16

* Fix to `add_process` with list of strings as arguments, the executable no longer returns the string `"None"`.
* Fix callbacks and triggers for `add_process` being executed only on `Run` class termination, not on process completion.

## [v1.0.0](https://github.com/simvue-io/client/releases/tag/v1.0.0) - 2024-06-14

* Refactor and re-write of codebase to align with latest developments in version 2 of the Simvue server.
* Added `Executor` to Simvue runs allowing users to start shell based processes as part of a run and handle termination of these.
* Removal of obsolete functions due to server change, and renaming of functions and parameters (see [documentation](https://docs.simvue.io)).
* Added pre-request validation to both `Client` and `Run` class methods via Pydantic.
* Separation of save functionality into `save_file` and `save_object`.
* Fixed issue whereby metrics would still have to wait for the next iteration of dispatch before being sent to the server, even if the queue was not full.
* Added support for `'user'` alerts.

## [v0.14.3](https://github.com/simvue-io/client/releases/tag/v0.14.3) - 2023-06-29

* Ensure import of the `requests` module is only done if actually used.

## [v0.14.0](https://github.com/simvue-io/client/releases/tag/v0.14.0) - 2023-04-04

* Added a method to the `Client` class for retrieving events.

## [v0.13.3](https://github.com/simvue-io/client/releases/tag/v0.13.3) - 2023-04-04

* Allow files (`input` and `code` only) to be saved for runs in the `created` state.
* Allow metadata and tags to be updated for runs in the `created` state.

## [v0.13.2](https://github.com/simvue-io/client/releases/tag/v0.13.2) - 2023-04-04

* Added `plot_metrics` method to the `Client` class to simplify plotting metrics.
* (Bug fix) `reconnect` works without a uuid being specified when `offline` mode isn't being used.
* (Bug fix) Restrict version of Pydantic to prevent V2 from accidently being used.

## [v0.13.1](https://github.com/simvue-io/client/releases/tag/v0.13.1) - 2023-03-28

* Set `sample_by` to 0 by default (no sampling) in `get_metrics_multiple`.

## [v0.13.0](https://github.com/simvue-io/client/releases/tag/v0.13.0) - 2023-03-28

* Added methods to the `Client` class for retrieving metrics.
* CPU architecture and processor obtained on Apple hardware.
* Client now reports to server when files have been successfully uploaded.
* `User-Agent` header now included in HTTP requests.

## [v0.12.0](https://github.com/simvue-io/client/releases/tag/v0.12.0) - 2023-03-13

* Add methods to the `Client` class for deleting runs and folders.
* Confusing messages about `process no longer exists` or `NVML Shared Library Not Found` no longer displayed.

## [v0.11.4](https://github.com/simvue-io/client/releases/tag/v0.11.4) - 2023-03-13

* (Bug fix) Ensure `simvue_sender` can be run when installed from PyPI.
* (Bug fix) Runs created in `offline` mode using a context manager weren't automatically closed.

## [v0.11.3](https://github.com/simvue-io/client/releases/tag/v0.11.3) - 2023-03-07

* Added logging messages for debugging when debug level set to `debug`.

## [v0.11.2](https://github.com/simvue-io/client/releases/tag/v0.11.2) - 2023-03-06
* Raise exceptions in `Client` class methods if run does not exist or artifact does not exist.
* (Bug fix) `list_artifacts` optional category restriction now works.

## [v0.11.1](https://github.com/simvue-io/client/releases/tag/v0.11.1) - 2023-03-05

* Support different runs having different metadata in `get_runs` dataframe output.
* (Bug fix) Error message when creating a duplicate run is now more clear.
* (Bug fix) Correction to stopping the worker thread in situations where the run never started.

## [v0.11.0](https://github.com/simvue-io/client/releases/tag/v0.11.0) - 2023-03-04

* Support optional dataframe output from `get_runs`.

## [v0.10.1](https://github.com/simvue-io/client/releases/tag/v0.10.1) - 2023-03-03

* The worker process now no longer gives a long delay when a run has finished (now at most ~1 second).
* The worker process ends when the `Run()` context ends or `close` is called, rather than only when the main process exits.

## [v0.10.0](https://github.com/simvue-io/client/releases/tag/v0.10.0) - 2023-02-07

* The `client` class can now be used to retrieve runs.

## [v0.9.1](https://github.com/simvue-io/client/releases/tag/v0.9.1) - 2023-01-25

* (Bug fix) Retries in POST/PUTs to REST APIs didn't happen.
* Warn users if `allow_pickle=True` is required.

## [v0.9.0](https://github.com/simvue-io/client/releases/tag/v0.9.0) - 2023-01-25

* Set status to `failed` or `terminated` if the context manager is used and there is an exception.

## [v0.8.0](https://github.com/simvue-io/client/releases/tag/v0.8.0) - 2023-01-23

* Support NumPy arrays, PyTorch tensors, Matplotlib and Plotly plots and picklable Python objects as artifacts.
* (Bug fix) Events in offline mode didn't work.

## [v0.7.2](https://github.com/simvue-io/client/releases/tag/v0.7.2) - 2023-01-08

* Pydantic model is used for input validation.
* Support NaN, -inf and inf in metadata and metrics.

## [v0.7.0](https://github.com/simvue-io/client/releases/tag/v0.7.0) - 2022-12-05

* Collect CPU, GPU and memory resource metrics.
* Automatically delete temporary files used in offline mode once runs have entered a terminal state.
* Warn users if their access token has expired.
* Remove dependency on the randomname module, instead handle name generation server side.

## [v0.6.0](https://github.com/simvue-io/client/releases/tag/v0.6.0) - 2022-11-07

* `offline` and `disabled` options replaced with single `mode` flag.

## [v0.5.0](https://github.com/simvue-io/client/releases/tag/v0.5.0) - 2022-11-03

* Added option to disable all monitoring.

## [v0.4.0](https://github.com/simvue-io/client/releases/tag/v0.4.0) - 2022-11-03

* Offline mode added, enabling tracking of simulations running on worker nodes without outgoing network access.
* Argument to `init` enabling runs to be left in the `created` state changed from `status="created"` to `running=True`.
* Improvements to error handling.

## [v0.3.0](https://github.com/simvue-io/client/releases/tag/v0.3.0) - 2022-10-31

* Update `add_alert` method to support either metrics or events based alerts.

## [v0.2.0](https://github.com/simvue-io/client/releases/tag/v0.2.0) - 2022-10-26

* The previous `Simvue` class has been split into `Run` and `Client`. When creating a run use the new `Run` class rather than `Simvue`.

## [v0.1.0](https://github.com/simvue-io/client/releases/tag/v0.1.0) - 2022-10-25

* First release.
