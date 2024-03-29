"""
Simvue Client
=============

Contains a Simvue client class for interacting with existing objects on the
server including deletion and retrieval.
"""

import json
import logging
import os
import typing
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

from .converters import metrics_to_dataframe, to_dataframe
from .serialization import Deserializer
from .types import DeserializedContent
from .utilities import check_extra, get_auth

if typing.TYPE_CHECKING:
    from matplotlib.figure import Figure
    from pandas import DataFrame

CONCURRENT_DOWNLOADS = 10
DOWNLOAD_CHUNK_SIZE = 8192
DOWNLOAD_TIMEOUT = 30

logger = logging.getLogger(__file__)


def downloader(job: dict[str, str]) -> bool:
    """Download a job output to the location specified within the definition

    Parameters
    ----------
    job : dict[str, str]
        a dictionary containing information on URL and path for a given job
        this information is then used to perform the download

    Returns
    -------
    bool
        whether the file was created successfully
    """
    # Check to make sure all requirements have been retrieved first
    for key in ("url", "path", "filename"):
        if key not in job:
            logger.warning(f"Expected key '{key}' during job object retrieval")
            raise RuntimeError(
                "Failed to retrieve required information during job download"
            )

    try:
        response = requests.get(job["url"], stream=True, timeout=DOWNLOAD_TIMEOUT)
        response = requests.get(job["url"], stream=True, timeout=DOWNLOAD_TIMEOUT)
    except requests.exceptions.RequestException:
        return False

    total_length = response.headers.get("content-length")
    total_length = response.headers.get("content-length")

    save_location: str = os.path.join(job["path"], job["filename"])

    if not os.path.isdir(job["path"]):
        raise ValueError(f"Cannot write to '{job['path']}', not a directory.")

    logger.debug(f"Writing file '{save_location}'")

    with open(save_location, "wb") as fh:
        if total_length is None:
            fh.write(response.content)
        else:
            for data in response.iter_content(chunk_size=DOWNLOAD_CHUNK_SIZE):
                fh.write(data)

    return os.path.exists(save_location)


class Client:
    """
    Class for querying Simvue
    """

    def __init__(self) -> None:
        """Initialise an instance of the Simvue client"""
        self._url: typing.Optional[str]
        self._token: typing.Optional[str]

        self._url, self._token = get_auth()

        for label, value in zip(("URL", "API token"), (self._url, self._token)):
            if not value:
                logger.warning(f"No {label} specified")

        self._headers: dict[str, str] = {"Authorization": f"Bearer {self._token}"}

    def get_run_id_from_name(self, name: str) -> str:
        """Get Run ID from the server matching the specified name

        Assumes a unique name for this run. If multiple results are found this
        method will fail.

        Parameters
        ----------
        name : str
            the name of the run

        Returns
        -------
        str
            the unique identifier for this run

        Raises
        ------
        RuntimeError
            if either information could not be retrieved from the server,
            or multiple/no runs are found
        """
        params: dict[str, str] = {"filters": json.dumps([f"name == {name}"])}

        response: requests.Response = requests.get(
            f"{self._url}/api/runs", headers=self._headers, params=params
        )

        if response.status_code != 200:
            try:
                detail = response.json().get("detail", response.text)
            except requests.exceptions.JSONDecodeError:
                detail = response.text
            raise RuntimeError(
                "Retrieval of run ID from name failed with "
                f"status {response.status_code}: {detail}"
            )

        if not (response_data := response.json().get("data")):
            raise RuntimeError(f"No ID found for run '{name}'")

        if len(response_data) == 0:
            raise RuntimeError("Could not collect ID - no run found with this name.")
        if len(response_data) > 1:
            raise RuntimeError(
                "Could not collect ID - more than one run exists with this name."
            )
        if not (first_id := response_data[0].get("id")):
            raise RuntimeError("Failed to retrieve identifier for run.")
        return first_id

    def get_run(self, run_id: str) -> dict[str, typing.Any]:
        """Retrieve a single run

        Parameters
        ----------
        run_id : str
            the unique identifier for this run

        Returns
        -------
        dict[str, Any]
            response containing information on the given run

        Raises
        ------
        RuntimeError
            if retrieval of information from the server on this run failed
        """

        response: requests.Response = requests.get(
            f"{self._url}/api/runs/{run_id}", headers=self._headers
        )

        if response.status_code == 404 and (detail := response.json().get("detail")):
            raise RuntimeError(
                f"Retrieval of run '{run_id}' failed with status "
                f"{response.status_code}: {detail}"
            )

        if response.status_code == 200:
            return response.json()

        try:
            detail = response.json().get("detail", response.text)
        except requests.exceptions.JSONDecodeError:
            detail = response.text

        raise RuntimeError(
            f"Retrieval of run '{run_id}' failed with status "
            f"{response.status_code}: {detail}"
        )

    def get_runs(
        self,
        filters: typing.Optional[list[str]],
        system: bool = False,
        metrics: bool = False,
        alerts: bool = False,
        metadata: bool = False,
        format: typing.Literal["dict", "dataframe"] = "dict",
    ) -> typing.Union[
        "DataFrame", dict[str, typing.Union[int, str, float, None]], None
    ]:
        """Retrieve all runs matching filters

        Parameters
        ----------
        filters: list[str] | None
            set of filters to apply to query results. If None is specified
            return all results without filtering.
        metadata : bool, optional
            whether to include metadata information in the response.
            Default False.
        metrics : bool, optional
            whether to include metrics information in the response.
            Default False.
        alerts : bool, optional
            whether to include alert information in the response.
            Default False.
        format : str ('dict' | 'dataframe'), optional
            the structure of the response, either a dictionary or a dataframe.
            Default is 'dict'. Pandas must be installed for 'dataframe'.

        Returns
        -------
        dict | pandas.DataFrame
            either the JSON response from the runs request or the results in the
            form of a Pandas DataFrame

        Raises
        ------
        ValueError
            if a value outside of 'dict' or 'dataframe' is specified
        RuntimeError
            if there was a failure in data retrieval from the server
        """
        params = {
            "filters": json.dumps(filters),
            "return_basic": True,
            "return_metrics": metrics,
            "return_alerts": alerts,
            "return_system": system,
            "return_metadata": metadata,
        }

        response = requests.get(
            f"{self._url}/api/runs", headers=self._headers, params=params
        )

        response.raise_for_status()

        if format not in ("dict", "dataframe"):
            raise ValueError("Invalid format specified")

        if response.status_code != 200:
            try:
                detail = response.json().get("detail", response.text)
            except requests.exceptions.JSONDecodeError:
                detail = response.text
            raise RuntimeError(
                f"Run retrieval failed with code {response.status_code}: " f"{detail}"
            )

        if response_data := response.json().get("data"):
            return response_data
        elif format == "dataframe":
            return to_dataframe(response.json())
        else:
            raise RuntimeError("Failed to retrieve runs data")

    def delete_run(self, run_identifier: str) -> typing.Optional[dict]:
        """Delete run by identifier

        Parameters
        ----------
        run_identifier : str
            the unique identifier for the run

        Returns
        -------
        dict | None
            the request response after deletion

        Raises
        ------
        RuntimeError
            if the deletion failed due to server request error
        """

        response = requests.delete(
            f"{self._url}/api/runs/{run_identifier}", headers=self._headers
        )

        if response.status_code == 200:
            logger.debug(f"Run '{run_identifier}' deleted successfully")
            return response.json()

        try:
            error_detail = response.json().get("detail", response.text)
        except requests.exceptions.JSONDecodeError:
            error_detail = response.text

        raise RuntimeError(
            f"Deletion of run '{run_identifier}' failed with code"
            f" {response.status_code}: {error_detail}"
        )

    def _get_folder_id_from_path(self, path: str) -> typing.Optional[str]:
        """Retrieve folder identifier for the specified path if found

        Parameters
        ----------
        path : str
            the path to search for

        Returns
        -------
        str | None
            if a match is found, return the identifier of the folder
        """
        params: dict[str, str] = {"filters": json.dumps([f"path == {path}"])}

        response: requests.Response = requests.get(
            f"{self._url}/api/folders", headers=self._headers, params=params
        )

        if (
            response.status_code == 200
            and (response_data := response.json().get("data"))
            and (identifier := response_data[0].get("id"))
        ):
            return identifier

        return None

    def delete_runs(self, folder_name: str) -> typing.Optional[list]:
        """Delete runs in a named folder

        Parameters
        ----------
        folder_name : str
            the name of the folder on which to perform deletion

        Returns
        -------
        list | None
            List of deleted runs

        Raises
        ------
        RuntimeError
            if deletion fails due to server request error
        """
        folder_id = self._get_folder_id_from_path(folder_name)

        if not folder_id:
            raise ValueError(f"Could not find a folder matching '{folder_name}'")

        params: dict[str, bool] = {"runs_only": True, "runs": True}

        response = requests.delete(
            f"{self._url}/api/folders/{folder_id}", headers=self._headers, params=params
        )

        if response.status_code == 200:
            if runs := response.json().get("runs", []):
                logger.debug(f"Runs from '{folder_name}' deleted successfully: {runs}")
            else:
                logger.debug("Folder empty, no runs deleted.")
            return runs

        raise RuntimeError(
            f"Deletion of runs from folder '{folder_name}' failed"
            f"with code {response.status_code}: {response.text}"
        )

    def delete_folder(
        self,
        folder_name: str,
        recursive: bool = False,
        remove_runs: bool = False,
        allow_missing: bool = False,
    ) -> typing.Optional[list]:
        """Delete a folder by name

        Parameters
        ----------
        folder_name : str
            name of the folder to delete
        recursive : bool, optional
            if folder contains additional folders remove these, else return an
            error. Default False.
        remove_runs : bool, optional
            whether to delete runs associated with this folder, by default False
        allow_missing : bool, optional
            allows deletion of folders which do not exist, else raise exception,
            default is exception raise

        Returns
        -------
        list | None
            if a folder is identified the runs also removed during execution

        Raises
        ------
        RuntimeError
            if deletion of the folder from the server failed
        """
        folder_id = self._get_folder_id_from_path(folder_name)

        if not folder_id:
            if allow_missing:
                return None
            else:
                raise RuntimeError(
                    f"Deletion of folder '{folder_name}' failed, "
                    "folder does not exist."
                )

        params: dict[str, bool] = {"runs": True} if remove_runs else {}
        params |= {"recursive": recursive}

        response = requests.delete(
            f"{self._url}/api/folders/{folder_id}", headers=self._headers, params=params
        )
        response = requests.delete(
            f"{self._url}/api/folders/{folder_id}", headers=self._headers, params=params
        )

        if response.status_code == 200:
            runs: list[dict] = response.json().get("runs", [])
            return runs

        try:
            detail = response.json().get("detail", response.text)
        except requests.exceptions.JSONDecodeError:
            detail = response.text

        raise RuntimeError(
            f"Deletion of folder '{folder_name}' failed with"
            f" code {response.status_code}: {detail}"
        )

    def list_artifacts(self, run_id: str) -> list[dict[str, typing.Any]]:
        """Retrieve artifacts for a given run

        Parameters
        ----------
        run_id : str
            unique identifier for the run

        Returns
        -------
        list[dict[str, typing.Any]]
            list of relevant artifacts

        Raises
        ------
        RuntimeError
            if retrieval of artifacts failed when communicating with the server
        """
        params: dict[str, str] = {"runs": json.dumps([run_id])}

        response: requests.Response = requests.get(
            f"{self._url}/api/artifacts", headers=self._headers, params=params
        )

        if response.status_code != 200:
            try:
                detail = response.json().get("detail", response.text)
            except requests.exceptions.JSONDecodeError:
                detail = response.text
            raise RuntimeError(
                f"Retrieval of artifacts for run '{run_id}' failed with "
                f"status {response.status_code}: {detail}"
            )

        return response.json()

    def get_artifact(
        self, run_id: str, name: str, allow_pickle: bool = False
    ) -> DeserializedContent:
        """Return the contents of a specified artifact

        Parameters
        ----------
        run_id : str
            the unique identifier of the run from which to retrieve the artifact
        name : str
            the name of the artifact to retrieve
        allow_pickle : bool, optional
            whether to de-pickle the retrieved data, by default False

        Returns
        -------
        DataFrame | Figure | FigureWidget | ndarray | Buffer | Tensor | bytes
            de-serialized content of artifact if retrieved, else content
            of the server response

        Raises
        ------
        RuntimeError
            if retrieval of artifact from the server failed
        """
        params: dict[str, str] = {"name": name}

        response = requests.get(
            f"{self._url}/api/runs/{run_id}/artifacts",
            headers=self._headers,
            params=params,
        )

        if response.status_code == 404 and (detail := response.json().get("detail")):
            raise RuntimeError(
                f"Retrieval of artifact '{name}' for run '{run_id}' failed with "
                f"status {response.status_code}: {detail}"
            )

        if response.status_code != 200:
            return None

        url = response.json()[0]["url"]
        mimetype = response.json()[0]["type"]
        url = response.json()[0]["url"]
        mimetype = response.json()[0]["type"]

        response = requests.get(url, timeout=DOWNLOAD_TIMEOUT)
        response.raise_for_status()

        content: typing.Optional[DeserializedContent] = Deserializer().deserialize(
            response.content, mimetype, allow_pickle
        )

        return content or response.content

    def get_artifact_as_file(
        self, run_id: str, name: str, path: typing.Optional[str] = None
    ) -> None:
        """Retrieve the specified artifact in the form of a file

        Information is saved to a file as opposed to deserialized

        Parameters
        ----------
        run_id : str
            unique identifier for the run to be queried
        name : str
            the name of the artifact to be retrieved
        path : str | None, optional
            path to download retrieved content to, the default of None
            uses the current working directory.

        Raises
        ------
        RuntimeError
            if there was a failure during retrieval of information from the
            server
        """
        params: dict[str, str] = {"name": name}

        response: requests.Response = requests.get(
            f"{self._url}/api/runs/{run_id}/artifacts",
            headers=self._headers,
            params=params,
        )

        if response.status_code != 200:
            try:
                detail = response.json().get("detail", response.text)
            except requests.exceptions.JSONDecodeError:
                detail = response.text
            raise RuntimeError(
                f"Download of artifacts for run '{run_id}' failed with "
                f"status {response.status_code}: {detail}"
            )

        if not (results := response.json()):
            raise RuntimeError(
                f"Failed to download artifact '{name}' from run '{run_id}',"
                " no results found."
            )

        if not (url := results[0].get("url")):
            raise RuntimeError(
                "Failed to download artifacts, "
                "expected URL for retrieval but server "
                "did not return result"
            )

        downloader(
            {
                "url": url,
                "filename": os.path.basename(name),
                "path": path or os.getcwd(),
            }
        )

    def _assemble_artifact_downloads(
        self,
        request_response: requests.Response,
        startswith: typing.Optional[str],
        endswith: typing.Optional[str],
        contains: typing.Optional[str],
        out_path: str,
    ) -> list[dict[str, str]]:

        downloads: list[dict[str, str]] = []

        for item in request_response.json():
            for key in ("url", "name"):
                if key not in item:
                    raise RuntimeError(
                        f"Expected key '{key}' in request "
                        "response during file retrieval"
                    )

            if startswith and not item["name"].startswith(startswith):
                continue
            if contains and contains not in item["name"]:
                continue
            if endswith and not item["name"].endswith(endswith):
                continue

            file_name: str = os.path.basename(item["name"])
            file_dir: str = os.path.join(out_path, os.path.dirname(item["name"]))

            job: dict[str, str] = {
                "url": item["url"],
                "filename": file_name,
                "path": file_dir,
            }

            if os.path.isfile(file_path := os.path.join(file_dir, file_name)):
                logger.warning(f"File '{file_path}' exists, skipping")
                continue

            os.makedirs(job["path"], exist_ok=True)

            downloads.append(job)

        return downloads

    def get_artifacts_as_files(
        self,
        run_id: str,
        path: typing.Optional[str] = None,
        startswith: typing.Optional[str] = None,
        contains: typing.Optional[str] = None,
        endswith: typing.Optional[str] = None,
    ) -> None:
        """Retrieve artifacts from the given run as a set of files

        Parameters
        ----------
        run_id : str
            the unique identifier for the run
        path : str | None, optional
            location to download files to, the default of None will download
            them to the current working directory
        startswith : typing.Optional[str], optional
            only download artifacts with this prefix in their name, by default None
        contains : typing.Optional[str], optional
            only download artifacts containing this term in their name, by default None
        endswith : typing.Optional[str], optional
            only download artifacts ending in this term in their name, by default None

        Raises
        ------
        RuntimeError
            if there was a failure retrieving artifacts from the server
        """

        response: requests.Response = requests.get(
            f"{self._url}/api/runs/{run_id}/artifacts", headers=self._headers
        )

        if response.status_code != 200:
            try:
                detail = response.json().get("detail", response.text)
            except requests.exceptions.JSONDecodeError:
                detail = response.text
            raise RuntimeError(
                f"Download of artifacts for run '{run_id}' failed with "
                f"status {response.status_code}: {detail}"
            )

        downloads: list[dict[str, str]] = self._assemble_artifact_downloads(
            request_response=response,
            startswith=startswith,
            endswith=endswith,
            contains=contains,
            out_path=path or os.getcwd(),
        )

        with ThreadPoolExecutor(CONCURRENT_DOWNLOADS) as executor:
            futures = [executor.submit(downloader, item) for item in downloads]
            for future, download in zip(as_completed(futures), downloads):
                try:
                    future.result()
                except Exception as e:
                    raise RuntimeError(
                        f"Download of file {download['url']} "
                        f"failed with exception: {e}"
                    )

    def get_folder(self, folder_id: str) -> dict[str, typing.Any]:
        """Retrieve a folder by identifier

        Parameters
        ----------
        folder_id : str
            unique identifier for the folder

        Returns
        -------
        dict[str, typing.Any]
            data for the requested folder

        Raises
        ------
        RuntimeError
            if there was a failure when retrieving information from the server
        """
        params: dict[str, str] = {"filters": json.dumps([f"path == {folder_id}"])}

        response: requests.Response = requests.get(
            f"{self._url}/api/folders", headers=self._headers, params=params
        )

        if response.status_code != 200:
            try:
                detail = response.json().get("detail", response.text)
            except requests.exceptions.JSONDecodeError:
                detail = response.text
            raise RuntimeError(
                f"Retrieval of folder '{folder_id}' failed with "
                f"status {response.status_code}: {detail}"
            )

        if response.status_code == 200:
            if len(data := response.json().get("data")) == 0:
                raise RuntimeError(f"Folder '{folder_id}' does not exist")

        return data[0]

    def get_folders(
        self, filters: typing.Optional[list[str]] = None
    ) -> list[dict[str, typing.Any]]:
        """Retrieve folders from the server

        Parameters
        ----------
        filters : list[str] | None
            set of filters to apply to the search

        Returns
        -------
        list[dict[str, Any]]
            all data for folders matching the filter request

        Raises
        ------
        RuntimeError
            if there was a failure retrieving data from the server
        """
        params: dict[str, str] = {"filters": json.dumps(filters or [])}

        response: requests.Response = requests.get(
            f"{self._url}/api/folders", headers=self._headers, params=params
        )

        if response.status_code == 200:
            return response.json().get("data", [])

        try:
            detail = response.json().get("detail", response.text)
        except requests.exceptions.JSONDecodeError:
            detail = response.text

        raise RuntimeError(
            "Retrieval of folders failed with status code "
            f"{response.status_code}: {detail}"
        )

    def get_metrics_names(self, run_id: str) -> dict[str, typing.Any]:
        """Return information on all metrics within a run

        Parameters
        ----------
        run_id : str
            unique identifier of the run

        Returns
        -------
        dict[str, Any]
            metric data for the given run

        Raises
        ------
        RuntimeError
            if there was a failure retrieving information from the server
        """
        params = {"runs": json.dumps([run_id])}

        response: requests.Response = requests.get(
            f"{self._url}/api/metrics/names", headers=self._headers, params=params
        )

        if response.status_code == 200:
            return response.json()

        try:
            detail = response.json().get("detail", response.text)
        except requests.exceptions.JSONDecodeError:
            detail = response.text

        raise RuntimeError(
            f"Request for metric names for run '{run_id}' failed with "
            f"status code {response.status_code}: {detail}"
        )

    def get_metrics(
        self,
        run_id: str,
        metric_name: str,
        xaxis: typing.Literal["step", "time", "timestamp"],
        max_points: int = 0,
        format: typing.Literal["list", "dataframe"] = "list",
    ) -> typing.Union["DataFrame", list[list]]:
        """Get time series metrics for the given metric name and run

        Parameters
        ----------
        run_id : str
            the unique identifier of the run
        metric_name : str
            the name of the metric set to retrieve
        xaxis : str ('step' | 'time' | 'timestamp')
            the x axis form
        format : str ('list' | 'dataframe')
            whether to return a list of entries or dataframe

        Returns
        -------
        list[list] | pandas.DataFrame
            time series data for metric

        Raises
        ------
        ValueError
            if an invalid argument is provided
        RuntimeError
            if there was a failure retrieving data from the server
        """
        run_response: requests.Response = requests.get(
            f"{self._url}/api/runs/{run_id}", headers=self._headers
        )

        if run_response.status_code == 404 and (
            detail := run_response.json().get("detail")
        ):
            raise RuntimeError(
                f"Retrieval of metric listings for '{metric_name}' in"
                f"run '{run_id}' failed with "
                f"status {run_response.status_code}: {detail}"
            )

        run_name: typing.Optional[str] = None

        if run_response.status_code == 200 and not (
            run_name := run_response.json().get("name")
        ):
            raise RuntimeError(
                "Expected key 'name' in run_response for metric retrieval "
                f"from run '{run_id}'"
            )

        params: dict[str, typing.Union[str, int]] = {
            "runs": json.dumps([run_id]),
            "metrics": json.dumps([metric_name]),
            "xaxis": xaxis,
            "max_points": max_points,
        }

        if xaxis not in ("step", "time", "timestamp"):
            raise ValueError(
                'Invalid xaxis specified, should be either "step", "time", or "timestamp"'
            )

        if format not in ("list", "dataframe"):
            raise ValueError(
                'Invalid format specified, should be either "list" or "dataframe"'
            )

        metrics_response: requests.Response = requests.get(
            f"{self._url}/api/metrics", headers=self._headers, params=params
        )

        if metrics_response.status_code != 200:
            detail = metrics_response.json().get("detail", metrics_response.text)
            raise RuntimeError(
                f"Retrieval of metric listing for '{metric_name}' in "
                f"run '{run_id}' failed with status code {metrics_response.status_code}: "
                f"{detail}"
            )

        run_data: typing.Optional[dict[str, typing.Any]]
        metric_data: typing.Optional[list[dict[str, typing.Any]]]

        if not (run_data := metrics_response.json().get(run_id)) or not (
            metric_data := run_data.get(metric_name)
        ):
            raise RuntimeError(
                f"Expected entry for '{metric_name}' for run '{run_id}' in server "
                "response, but none found"
            )

        data: list[list] = [
            [item[xaxis], item["value"], run_name, metric_name] for item in metric_data
        ]

        if format == "dataframe":
            return metrics_to_dataframe(data, xaxis, name=metric_name)

        return data

    def get_metrics_multiple(
        self,
        run_ids: list[str],
        metric_names: list[str],
        xaxis: typing.Literal["step", "time"],
        max_points: int = -1,
        aggregate: bool = False,
        format: typing.Literal["list", "dataframe"] = "list",
    ):
        """Get time series data for multiple runs/metrics

        Parameters
        ----------
        run_ids : list[str]
            unique identifiers of runs to retrieve
        metric_names : list[str]
            labels for metrics to retrieve
        xaxis : str ('step' | 'time')
            the x axis form
        max_points : int, optional
            maximum number of points to display, by default -1 (no limit)
        aggregate : bool, optional
             to aggregate the results, by default False
        format : str ('list' | 'dataframe'), optional
            the form in which to return results, by default "list"

        Returns
        -------
        list | pandas.DataFrame
            either a list or a dataframe containing metric values for the
            runs specified

        Raises
        ------
        ValueError
            if an invalid argument is provided
        RuntimError
            if there was a failure retrieving data from the server
        """
        params: dict[str, typing.Union[int, str]] = {
            "runs": json.dumps(run_ids),
            "metrics": json.dumps(metric_names),
            "aggregate": aggregate,
            "max_points": max_points,
            "xaxis": xaxis,
        }

        if xaxis not in ("step", "time"):
            raise ValueError(
                'Invalid xaxis specified, should be either "step" or "time"'
            )

        if format not in ("list", "dataframe"):
            raise ValueError(
                'Invalid format specified, should be either "list" or "dataframe"'
            )

        response = requests.get(
            f"{self._url}/api/metrics", headers=self._headers, params=params
        )
        response = requests.get(
            f"{self._url}/api/metrics", headers=self._headers, params=params
        )

        if response.status_code != 200:
            try:
                detail = response.json().get("detail", response.text)
            except requests.exceptions.JSONDecodeError:
                detail = response.text
            raise RuntimeError(
                f"Retrieval of metrics '{metric_names}' failed for runs '{run_ids}' "
                f"with status code {response.status_code}: {detail}"
            )

        data: list[list[typing.Union[str, int, float]]] = []

        if not aggregate:
            non_agg_data: dict[
                str, dict[str, list[dict[str, typing.Union[int, float]]]]
            ] = response.json()

            for run_id, run in non_agg_data.items():
                for name, metric_entry in run.items():
                    for item in metric_entry:
                        data.append([item[xaxis], item["value"], run_id, name])
        else:
            agg_data: dict[str, list[dict[str, typing.Union[int, float]]]] = (
                response.json()
            )

            for name, entries in agg_data.items():
                for item in entries:

                    data.append(
                        [
                            item[xaxis],
                            item["min"],
                            item["average"],
                            item["max"],
                            name,
                        ]
                    )

        if format == "dataframe":
            return metrics_to_dataframe(data, xaxis)

        return data

    @check_extra("plot")
    def plot_metrics(
        self,
        run_ids: list[str],
        metric_names: list[str],
        xaxis: typing.Literal["step", "time"],
        max_points: int = -1,
    ) -> "Figure":
        """Plt the time series values for multiple metrics/runs

        Parameters
        ----------
        run_ids : list[str]
            unique identifiers for runs to plot
        metric_names : list[str]
            names of metrics to plot
        xaxis : str, ('step' | 'time' | 'timestep')
            the x axis to plot against
        max_points : int, optional
            maximum number of data points, by default -1 (all)

        Returns
        -------
        Figure
            plot figure object

        Raises
        ------
        ValueError
            if invalid arguments are provided
        """
        if not isinstance(run_ids, list):
            raise ValueError("Invalid runs specified, must be a list of run names.")

        if not isinstance(metric_names, list):
            raise ValueError("Invalid names specified, must be a list of metric names.")

        data: "DataFrame" = self.get_metrics_multiple(  # type: ignore
            run_ids, metric_names, xaxis, max_points, format="dataframe"
        )

        import matplotlib.pyplot as plt

        for run in run_ids:
            for name in metric_names:
                label = None
                if len(run_ids) > 1 and len(metric_names) > 1:
                    label = f"{run}: {name}"
                elif len(run_ids) > 1 and len(metric_names) == 1:
                    label = run
                elif len(run_ids) == 1 and len(metric_names) > 1:
                    label = name

                plt.plot(
                    data[(run, name, xaxis)], data[(run, name, "value")], label=label
                )
                plt.plot(
                    data[(run, name, xaxis)], data[(run, name, "value")], label=label
                )

        if xaxis == "step":
            plt.xlabel("steps")
        elif xaxis == "time":
            plt.xlabel("relative time")
        if xaxis == "step":
            plt.xlabel("steps")
        elif xaxis == "time":
            plt.xlabel("relative time")

        if len(metric_names) == 1:
            plt.ylabel(metric_names[0])

        return plt.figure()

    def get_events(
        self,
        run_id: str,
        message_contains: typing.Optional[str] = None,
        start_index: typing.Optional[int] = None,
        count_limit: typing.Optional[int] = None,
    ) -> list[dict[str, str]]:
        """Return events for a specified run

        Parameters
        ----------
        run_id : str
            the unique identifier of the run to query
        message_contains : typing.Optional[str], optional
            filter to events with message containing this expression, by default None
        start_index : typing.Optional[int], optional
            slice results returning only those above this index, by default None
        count_limit : typing.Optional[int], optional
            limit number of returned results, by default None

        Returns
        -------
        list[dict[str, str]]
            list of matching events containing entries with message and timestamp data

        Raises
        ------
        RuntimeError
            if there was a failure retrieving information from the server
        """

        msg_filter: str = (
            json.dumps([f"event.message contains {message_contains}"])
            if message_contains
            else ""
        )

        params: dict[str, typing.Union[str, int]] = {
            "run": run_id,
            "filters": msg_filter,
            "start": start_index or 0,
            "count": count_limit or 0,
        }

        response = requests.get(
            f"{self._url}/api/events", headers=self._headers, params=params
        )
        response = requests.get(
            f"{self._url}/api/events", headers=self._headers, params=params
        )

        if response.status_code == 200:
            return response.json().get("data", [])

        try:
            detail = response.json().get("detail", response.text)
        except requests.exceptions.JSONDecodeError:
            detail = response.text

        raise RuntimeError(
            f"Retrieval of events for run '{run_id}' failed with "
            f"status code {response.status_code}: {detail}"
        )

    def get_alerts(
        self, run_id: str, critical_only: bool = True, names_only: bool = True
    ) -> list[dict[str, typing.Any]]:
        """Retrieve alerts for a given run

        Parameters
        ----------
        run : str
            The ID of the run to find alerts for
        critical_only : bool, optional
            Whether to only return details about alerts which are currently critical, by default True
        names_only: bool, optional
            Whether to only return the names of the alerts (otherwise return the full details of the alerts), by default True

        Returns
        -------
        list[dict[str, Any]]
            a list of all alerts for this run which match the constrains specified

        Raises
        ------
        RuntimeError
            if there was a failure retrieving data from the server
        """
        response = requests.get(f"{self._url}/api/runs/{run_id}", headers=self._headers)

        if response.status_code != 200:
            try:
                detail = response.json().get("detail", response.text)
            except requests.exceptions.JSONDecodeError:
                detail = response.text
            raise RuntimeError(
                f"Retrieval of alerts for run '{run_id}' failed with "
                f"status {response.status_code}: {detail}"
            )

        if (alerts := response.json().get("alerts")) is None:
            raise RuntimeError(
                "Expected key 'alerts' in response when retrieving "
                f"alerts for run '{run_id}': {response.json()}"
            )

        if critical_only:
            if names_only:
                return [
                    alert["alert"].get("name")
                    for alert in alerts
                    if alert["status"].get("current") == "critical"
                ]
            else:
                return [
                    alert
                    for alert in alerts
                    if alert["status"].get("current") == "critical"
                ]
        elif names_only:
            return [alert["alert"].get("name") for alert in alerts]

        return alerts
