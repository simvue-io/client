"""
Offline Sender
==============

Contains functions for the later dispatch of offline data to a Simvue server
"""

import typing
import json
import logging
import os
import shutil
import time
import pathlib

import msgpack

from .factory.proxy.remote import Remote
from .utilities import create_file, get_offline_directory, remove_file

logger = logging.getLogger(__name__)


def set_details(name: str, id: str, json_file: pathlib.Path) -> None:
    """Write name & id to file

    Parameters
    ----------
    name : str
        run name
    id : str
        run ID
    json_file : pathlib.Path
        file to write output to
    """
    data: dict[str, str] = {"name": name, "id": id}
    with json_file.open() as fh:
        json.dump(data, fh)


def get_details(json_file: pathlib.Path) -> tuple[str, str]:
    """Get name & id from file

    Parameters
    ----------
    json_file : pathlib.Path
        file to open

    Returns
    -------
    tuple[str, str]
        name of run
        id of run
    """
    with json_file.open() as fh:
        data = json.load(fh)
    return data["name"], data["id"]


def update_name(id, data):
    """
    Update id in metrics/events
    """
    for item in data:
        item["id"] = id


def add_name(
    name: str, data: dict[str, typing.Any], json_file: pathlib.Path
) -> dict[str, typing.Any]:
    """Update name in JSON

    Parameters
    ----------
    name : str
        name of run
    data : dict[str, Any]
        data to write to file if name undefined
    filename : pathlib.Path
        json file to write to

    Returns
    -------
    dict[str, Any]
        updated data
    """
    if not data["name"]:
        data["name"] = name
        with json_file.open("w") as fh:
            json.dump(data, fh)

    return data


def get_json(
    json_file: pathlib.Path, run_id: typing.Optional[str] = None, artifact: bool = False
) -> dict[str, typing.Any]:
    """Get JSON from file

    Parameters
    ----------
    json_file : pathlib.Path
        _description_
    run_id : typing.Optional[str], optional
        _description_, by default None
    artifact : bool, optional
        _description_, by default False

    Returns
    -------
    dict[str, typing.Any]
        _description_
    """
    with json_file.open() as fh:
        data = json.load(fh)

    if not run_id:
        return data

    if artifact:
        if "run" in data:
            data["run"] = run_id
        return data

    if "run" in data:
        data["run"] = run_id
    else:
        data["id"] = run_id

    return data


def sender() -> typing.Optional[str]:
    """Asynchronous upload of runs to Simvue server

    Returns
    -------
    str | None
        identifier of the created run if applicable
    """
    directory = get_offline_directory()

    logger.debug(f"Using offline directory '{directory}'")

    # Clean up old runs after waiting 5 mins
    runs = directory.glob("*/sent")
    run_id = None

    for run in runs:
        logger.info("Cleaning up directory with id %s", run.parent.name)

        if time.time() - os.path.getmtime(run) > 300:
            try:
                shutil.rmtree(run.parent)
            except Exception:
                logger.error(
                    "Got exception trying to cleanup run in directory %s",
                    run.parent.name,
                )

    # Deal with runs in the created, running or a terminal state
    runs = list(
        path.resolve()
        for path in directory.glob("*/*")
        if path.name in ("created", "running", "completed", "failed", "terminated")
    )

    if not runs:
        logger.info("No runs found locally")
        return None

    logger.debug(f"Found {len(runs)} run{'s' if len(runs) != 1 else ''} locally")

    for run in runs:
        status = run.name
        current = run.parent

        if current.joinpath("sent").exists():
            logger.debug(f"Run {run} already sent skipping.")
            remove_file(run)
            continue

        run_init = get_json(run_json := current.joinpath("run.json"))
        start_time = os.path.getctime(run_json)

        if run_init["name"]:
            logger.info(
                "Considering run with name %s and id %s", run_init["name"], current.name
            )
        else:
            logger.info("Considering run with no name yet and id %s", current.name)

        # Create run if it hasn't previously been created
        created_file = current.joinpath("init")
        name = None
        if not created_file.is_file():
            remote = Remote(run_init["name"], current.name, suppress_errors=False)

            # Check token
            remote.check_token()

            name, run_id = remote.create_run(run_init)
            if name:
                logger.info("Creating run with name %s and id %s", name, current.name)
                run_init = add_name(name, run_init, f"{current}/run.json")
                set_details(name, run_id, created_file)
            else:
                logger.error("Failure creating run")
                continue
        else:
            name, run_id = get_details(created_file)
            run_init["name"] = name
            remote = Remote(run_init["name"], run_id, suppress_errors=False)

            # Check token
            remote.check_token()

        heartbeat_filename = current.joinpath("heartbeat")

        if status == "running":
            # Check for recent heartbeat
            if heartbeat_filename.is_file():
                mtime = os.path.getmtime(heartbeat_filename)
                if time.time() - mtime > 180:
                    status = "lost"

            # Check for no recent heartbeat
            if not heartbeat_filename.is_file():
                if time.time() - start_time > 180:
                    status = "lost"

        # Handle lost runs
        if status == "lost":
            logger.info(
                "Changing status to lost, name %s and id %s",
                run_init["name"],
                current.name,
            )
            status = "lost"
            create_file(current.joinpath("lost"))
            remove_file(current.joinpath("running"))

        # Send heartbeat if the heartbeat file was touched recently
        if heartbeat_filename.is_file():
            if (
                status == "running"
                and time.time() - os.path.getmtime(heartbeat_filename) < 120
            ):
                logger.info("Sending heartbeat for run with name %s", run_init["name"])
                remote.send_heartbeat()

        # Upload metrics, events, files & metadata as necessary
        files = sorted(current.glob("*"), key=os.path.getmtime)
        updates = 0
        for record in files:
            if any(
                record.name.endswith(i)
                for i in (
                    "run.json",
                    "running",
                    "completed",
                    "failed",
                    "terminated",
                    "lost",
                    "sent",
                    "-proc",
                )
            ):
                continue

            rename = False

            # Handle metrics
            if "/metrics-" in f"{record}":
                data = get_json(record, run_id)
                logger.info("Sending metrics for run %s: %s", run_init["name"], data)
                if remote.send_metrics(msgpack.packb(data, use_bin_type=True)):
                    rename = True

            # Handle events
            if "/events-" in f"{record}":
                data = get_json(record, run_id)
                logger.info("Sending events for run %s: %s", run_init["name"], data)
                if remote.send_event(msgpack.packb(data, use_bin_type=True)):
                    rename = True

            # Handle updates
            if "/update-" in f"{record}":
                data = get_json(record, run_id)
                logger.info("Sending update for run %s: %s", run_init["name"], data)
                if remote.update(data):
                    for item in data:
                        if item == "status" and data[item] in (
                            "completed",
                            "failed",
                            "terminated",
                        ):
                            create_file(f"{current}/sent")
                            remove_file(f"{current}/{status}")
                    rename = True

            # Handle folders
            if "/folder-" in f"{record}":
                data = get_json(record, run_id)
                logger.info("Sending folder details for run %s", run_init["name"], data)
                if remote.set_folder_details(data):
                    rename = True

            # Handle alerts
            if "/alert-" in f"{record}":
                data = get_json(record, run_id)
                logger.info(
                    "Sending alert details for run %s: %s", run_init["name"], data
                )
                if remote.add_alert(data):
                    rename = True

            # Handle files
            if "/file-" in f"{record}":
                data = get_json(record, run_id, True)
                logger.info("Saving file for run %s: %s", run_init["name"], data)
                if remote.save_file(data):
                    rename = True

            # Rename processed files
            if rename:
                os.rename(record, f"{record}-proc")
                updates += 1

        # If the status is completed and there were no updates, the run must have completely finished
        if updates == 0 and status in ("completed", "failed", "terminated"):
            logger.info("Finished sending run %s", run_init["name"])
            data = {"id": run_id, "status": status}
            if remote.update(data):
                create_file(current.joinpath("sent"))
                remove_file(current.joinpath(status))
        elif updates == 0 and status == "lost":
            logger.info("Finished sending run %s as it was lost", run_init["name"])
            create_file(current.joinpath("sent"))
    return run_id
