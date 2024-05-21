import json
import logging
import os
import pathlib
import time
import typing
import uuid

from simvue.factory.proxy.base import SimvueBaseClass
from simvue.utilities import (
    create_file,
    get_offline_directory,
    prepare_for_api,
    skip_if_failed,
)

logger = logging.getLogger(__name__)


class Offline(SimvueBaseClass):
    """
    Class for offline runs
    """

    def __init__(
        self, name: typing.Optional[str], uniq_id: str, suppress_errors: bool = True
    ) -> None:
        super().__init__(name, uniq_id, suppress_errors)
        os.makedirs(self.directory, exist_ok=True)

    @property
    def directory(self) -> pathlib.Path:
        return get_offline_directory().joinpath(self._uuid)

    @skip_if_failed("_aborted", "_suppress_errors", None)
    def _write_json(self, filename: pathlib.Path, data: dict[str, typing.Any]) -> None:
        """
        Write JSON to file
        """
        if not os.path.isdir(filename.parent):
            self._error(
                f"Cannot write file '{filename}', parent directory does not exist"
            )

        try:
            with filename.open("w") as fh:
                json.dump(data, fh)
        except Exception as err:
            self._error(f"Unable to write file {filename} due to {str(err)}")

    @skip_if_failed("_aborted", "_suppress_errors", None)
    def _mock_api_post(
        self, prefix: str, data: dict[str, typing.Any]
    ) -> typing.Optional[dict[str, typing.Any]]:
        unique_id = time.time()
        filename = self.directory.joinpath(f"{prefix}-{unique_id}.json")
        self._write_json(filename, data)
        return data

    @skip_if_failed("_aborted", "_suppress_errors", (None, None))
    def create_run(self, data) -> tuple[typing.Optional[str], typing.Optional[str]]:
        """
        Create a run
        """
        if not self.directory:
            self._logger.error("No directory specified")
            return (None, None)
        try:
            os.makedirs(self.directory, exist_ok=True)
        except Exception as err:
            self._logger.error(
                "Unable to create directory %s due to: %s", self.directory, str(err)
            )
            return (None, None)

        filename = self.directory.joinpath("run.json")

        logger.debug(f"Creating run in '{filename}'")

        if "name" not in data:
            data["name"] = None

        self._write_json(filename, data)

        status = data["status"]
        filename = self.directory.joinpath(status)
        create_file(filename)

        return (self._name, self._id)

    @skip_if_failed("_aborted", "_suppress_errors", None)
    def update(self, data) -> typing.Optional[dict[str, typing.Any]]:
        """
        Update metadata, tags or status
        """
        unique_id = time.time()
        filename = self.directory.joinpath(f"update-{unique_id}.json")
        self._write_json(filename, data)

        if "status" in data:
            status = data["status"]
            if not self.directory or not self.directory.exists():
                self._error("No directory defined for writing")
                return None
            filename = self.directory.joinpath(status)

            logger.debug(f"Writing API data to file '{filename}'")

            create_file(filename)

            if status == "completed":
                status_running = self.directory.joinpath("running")
                if os.path.isfile(status_running):
                    os.remove(status_running)

        return data

    @skip_if_failed("_aborted", "_suppress_errors", None)
    def set_folder_details(self, data) -> typing.Optional[dict[str, typing.Any]]:
        """
        Set folder details
        """
        unique_id = time.time()
        filename = self.directory.joinpath(f"folder-{unique_id}.json")
        self._write_json(filename, data)
        return data

    @skip_if_failed("_aborted", "_suppress_errors", None)
    def save_file(
        self, data: dict[str, typing.Any]
    ) -> typing.Optional[dict[str, typing.Any]]:
        """
        Save file
        """
        if "pickled" in data:
            temp_file = self.directory.joinpath(f"temp-{uuid.uuid4()}.pickle")
            with open(temp_file, "wb") as fh:
                fh.write(data["pickled"])
            data["pickledFile"] = temp_file
        unique_id = time.time()
        filename = self.directory.joinpath(f"file-{unique_id}.json")
        self._write_json(filename, prepare_for_api(data, False))
        return data

    def add_alert(
        self, data: dict[str, typing.Any]
    ) -> typing.Optional[dict[str, typing.Any]]:
        """
        Add an alert
        """
        return self._mock_api_post("alert", data)

    @skip_if_failed("_aborted", "_suppress_errors", None)
    def set_alert_state(
        self, alert_id: str, status: str
    ) -> typing.Optional[dict[str, typing.Any]]:
        if not os.path.exists(
            _alert_file := self.directory.joinpath(f"alert-{alert_id}.json")
        ):
            self._error(f"Failed to retrieve alert '{alert_id}' for modification")
            return None

        with open(_alert_file) as alert_in:
            _alert_data = json.load(alert_in)

        _alert_data |= {"run": self._id, "alert": alert_id, "status": status}

        self._write_json(_alert_file, _alert_data)

        return _alert_data

    @skip_if_failed("_aborted", "_suppress_errors", [])
    def list_alerts(self) -> list[dict[str, typing.Any]]:
        return [
            json.load(open(alert_file))
            for alert_file in self.directory.glob("alert-*.json")
        ]

    def send_metrics(
        self, data: dict[str, typing.Any]
    ) -> typing.Optional[dict[str, typing.Any]]:
        """
        Send metrics
        """
        return self._mock_api_post("metrics", data)

    def send_event(
        self, data: dict[str, typing.Any]
    ) -> typing.Optional[dict[str, typing.Any]]:
        """
        Send event
        """
        return self._mock_api_post("event", data)

    @skip_if_failed("_aborted", "_suppress_errors", None)
    def send_heartbeat(self) -> typing.Optional[dict[str, typing.Any]]:
        heartbeat_file = self.directory.joinpath("heartbeat")
        logger.debug(f"Creating heartbeat file: {heartbeat_file}")
        heartbeat_file.touch()
        return {"success": True}

    @skip_if_failed("_aborted", "_suppress_errors", False)
    def check_token(self) -> bool:
        return True
