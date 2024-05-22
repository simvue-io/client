"""
Converter Functions
===================

Contains functions for converting objects retrieved from the server between
data types including creation of DataFrames for metrics
"""

import typing
import pandas

if typing.TYPE_CHECKING:
    from pandas import DataFrame


def aggregated_metrics_to_dataframe(
    request_response_data: dict[str, list[dict[str, float]]],
    xaxis: str,
    parse_to: typing.Literal["dict", "dataframe"] = "dict",
) -> typing.Union[
    "DataFrame", dict[str, dict[tuple[float, str], typing.Optional[float]]]
]:
    """Create data frame for an aggregate of metrics

    Returns a dataframe with columns being metrics and sub-columns being the
    minimum, average etc.

    Parameters
    ----------
    request_response_data : dict[str, list[dict[str, float]]]
        the data retrieved from the Simvue server
    xaxis : str
        the x-axis label
    parse_to : Literal["dict", "dataframe"], optional
        form of output, dictionary or a Pandas Dataframe. Note pandas
        must be installed via the 'dataset' for the latter to work.

    Returns
    -------
    DataFrame | dict
        a Pandas dataframe of the metric set or the data as a dictionary
    """

    _all_steps: list[float] = sorted(
        set(
            (
                d[xaxis]
                for sublist in request_response_data.values()
                for d in sublist
                if xaxis in d
            )
        )
    )

    # Get the keys from the aggregate which are not the xaxis label
    _first_metric_set = next(iter(request_response_data.values()))
    _value_types = next(iter(_first_metric_set)).keys()
    _value_types = list(_value_types)
    _value_types.remove(xaxis)

    result_dict: dict[str, dict[tuple[float, str], typing.Optional[float]]] = {
        metric_name: {} for metric_name in request_response_data.keys()
    }

    for metric_name, metrics in request_response_data.items():
        metrics_iterator = iter(metrics)
        _metric_steps = (d[xaxis] for d in metrics)
        for step in _all_steps:
            if step not in _metric_steps:
                for value_type in _value_types:
                    result_dict[metric_name][step, value_type] = None
            else:
                next_item = next(metrics_iterator)
                for value_type in _value_types:
                    result_dict[metric_name][step, value_type] = next_item.get(
                        value_type
                    )

    if parse_to == "dataframe":
        _data_frame = pandas.DataFrame(result_dict)
        _data_frame.index.name = xaxis
        return _data_frame
    elif parse_to == "dict":
        return result_dict
    else:
        raise ValueError(f"Unrecognised parse format '{parse_to}'")


def parse_run_set_metrics(
    request_response_data: dict[str, dict[str, list[dict[str, float]]]],
    xaxis: str,
    run_labels: list[str],
    parse_to: typing.Literal["dict", "dataframe"] = "dict",
) -> typing.Union[
    dict[str, dict[tuple[float, str], typing.Optional[float]]], "DataFrame"
]:
    """Parse JSON response metric data from the server into the specified form

    Creates either a dictionary or a pandas dataframe of the data collected
    from multiple runs and metrics

    Parameters
    ----------
    request_response_data: dict[str, dict[str, list[dict[str, float]]]]
        JSON response data
    xaxis : str
        The x-axis label/key
    run_labels : list[str]
        The labels to assign for the runs
    parse_to : Literal["dict", "dataframe"], optional
        Form in which to parse data, default is dict

    Returns
    -------
    dict[str, dict[tuple[float, str], typing.Optional[float]]] | DataFrame
        either a dictionary or Pandas DataFrame containing the results

    Raises
    ------
    ValueError
        if an unrecognised parse format is specified
    """
    _all_steps: list[float] = sorted(
        set(
            (
                d[xaxis]
                for run_data in request_response_data.values()
                for sublist in run_data.values()
                for d in sublist
                if xaxis in d
            )
        )
    )

    _all_metrics: list[str] = sorted(
        set(
            (
                key
                for run_data in request_response_data.values()
                for key in run_data.keys()
            )
        )
    )

    # Get the keys from the aggregate which are not the xaxis label
    _first_run = next(iter(request_response_data.values()))
    _first_metric_set = next(iter(_first_run.values()))
    _value_types = next(iter(_first_metric_set)).keys()
    _value_types = list(_value_types)
    _value_types.remove(xaxis)

    result_dict: dict[str, dict[tuple[float, str], typing.Optional[float]]] = {
        metric_name: {} for metric_name in _all_metrics
    }

    for run_label, run_data in zip(run_labels, request_response_data.values()):
        for metric_name in _all_metrics:
            if metric_name not in run_data:
                for step in _all_steps:
                    result_dict[metric_name][step, run_label] = None
                continue
            metrics = run_data[metric_name]
            metrics_iterator = iter(metrics)
            _metric_steps = (d[xaxis] for d in metrics)
            for step in _all_steps:
                if step not in _metric_steps:
                    result_dict[metric_name][step, run_label] = None
                else:
                    next_item = next(metrics_iterator)
                    result_dict[metric_name][step, run_label] = next_item.get("value")

    if parse_to == "dataframe":
        _data_frame = pandas.DataFrame(
            result_dict,
            index=pandas.MultiIndex.from_product(
                [_all_steps, run_labels], names=(xaxis, "run")
            ),
        )
        return _data_frame
    elif parse_to == "dict":
        return result_dict
    else:
        raise ValueError(f"Unrecognised parse format '{parse_to}'")


def to_dataframe(data):
    """
    Convert runs to dataframe
    """

    metadata = []
    for run in data:
        if "metadata" in run:
            for item in run["metadata"]:
                if item not in metadata:
                    metadata.append(item)

    columns = {}
    for run in data:
        for item in ("name", "status", "folder", "created", "started", "ended"):
            if item not in columns:
                columns[item] = []
            if item in run:
                columns[item].append(run[item])
            else:
                columns[item].append(None)

        if "system" in run:
            for section in run["system"]:
                if section in ("cpu", "gpu", "platform"):
                    for item in run["system"][section]:
                        if "system.%s.%s" % (section, item) not in columns:
                            columns["system.%s.%s" % (section, item)] = []
                        columns["system.%s.%s" % (section, item)].append(
                            run["system"][section][item]
                        )
                else:
                    if "system.%s" % section not in columns:
                        columns["system.%s" % section] = []
                    columns["system.%s" % section].append(run["system"][section])

        if "metadata" in run:
            for item in metadata:
                if "metadata.%s" % item not in columns:
                    columns["metadata.%s" % item] = []
                if item in run["metadata"]:
                    columns["metadata.%s" % item].append(run["metadata"][item])
                else:
                    columns["metadata.%s" % item].append(None)

    return pandas.DataFrame(data=columns)


def metric_time_series_to_dataframe(
    data: list[dict[str, float]],
    xaxis: typing.Literal["step", "time", "timestamp"],
    name: typing.Optional[str] = None,
) -> "DataFrame":
    """Convert a single metric value set from a run into a dataframe

    Parameters
    ----------
    data : list[dict[str, float]]
        time series data from Simvue server for a single metric and run
    xaxis : Literal["step", "time", "timestamp"]
        the x-axis type
    name : str | None, optional
        if provided, an alternative name for the 'values' column, by default None

    Returns
    -------
    DataFrame
        a Pandas DataFrame containing values for the metric and run at each
    """

    _df_dict: dict[str, list[float]] = {
        xaxis: [v[xaxis] for v in data],
        name or "value": [v["value"] for v in data],
    }

    return pandas.DataFrame(_df_dict)
