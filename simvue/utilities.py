import configparser
import datetime
import hashlib
import logging
import json
import tabulate
import pathlib
import pydantic
import importlib.util
import contextlib
import os
import typing

import jwt

CHECKSUM_BLOCK_SIZE = 4096
EXTRAS: tuple[str, ...] = ("plot", "torch", "dataset")

logger = logging.getLogger(__name__)


def check_extra(extra_name: str) -> typing.Callable:
    def decorator(
        class_func: typing.Optional[typing.Callable] = None,
    ) -> typing.Optional[typing.Callable]:
        def wrapper(self, *args, **kwargs) -> typing.Any:
            if extra_name == "plot" and not all(
                [
                    importlib.util.find_spec("matplotlib"),
                    importlib.util.find_spec("plotly"),
                ]
            ):
                raise RuntimeError(
                    f"Plotting features require the '{extra_name}' extension to Simvue"
                )
            elif extra_name == "torch" and not importlib.util.find_spec("torch"):
                raise RuntimeError(
                    "PyTorch features require the 'torch' module to be installed"
                )
            elif extra_name == "dataset" and not all(
                [
                    importlib.util.find_spec("numpy"),
                    importlib.util.find_spec("pandas"),
                ]
            ):
                raise RuntimeError(
                    f"Dataset features require the '{extra_name}' extension to Simvue"
                )
            elif extra_name not in EXTRAS:
                raise RuntimeError(f"Unrecognised extra '{extra_name}'")
            return class_func(self, *args, **kwargs) if class_func else None

        return wrapper

    return decorator


def skip_if_failed(
    failure_attr: str,
    ignore_exc_attr: str,
    on_failure_return: typing.Optional[typing.Any] = None,
) -> typing.Callable:
    """Decorator for ensuring if Simvue throws an exception any other code continues.

    If Simvue throws an exception and the user has specified that such failure
    should not abort the run but rather log errors this decorator will skip
    functionality leaving the runner in a dormant state.

    Parameters
    ----------
    failure_attr : str
        the attribute of the parent class which determines if
        Simvue has failed
    ignore_exc_attr : str
        the attribute of the parent class which defines whether
        an exception should be raised or ignore, by default
    on_failure_return : typing.Any | None, optional
        the value to return instead, by default None

    Returns
    -------
    typing.Callable
        wrapped class method
    """

    def decorator(class_func: typing.Callable) -> typing.Callable:
        def wrapper(self, *args, **kwargs) -> typing.Any:
            if getattr(self, failure_attr, None) and getattr(
                self, ignore_exc_attr, None
            ):
                logger.debug(
                    f"Skipping call to '{class_func.__name__}', "
                    f"client in fail state (see logs)."
                )
                return on_failure_return

            # Handle case where Pydantic validates the inputs
            try:
                return class_func(self, *args, **kwargs)
            except pydantic.ValidationError as e:
                out_table: list[str] = []
                for data in json.loads(e.json()):
                    out_table.append([data["loc"], data["type"], data["msg"]])
                err_table = tabulate.tabulate(
                    out_table,
                    headers=["Location", "Type", "Message"],
                    tablefmt="fancy_grid",
                )
                err_str = f"`{class_func.__name__}` Validation:\n{err_table}"
                if getattr(self, ignore_exc_attr, True):
                    setattr(self, failure_attr, True)
                    logger.error(err_str)
                    return on_failure_return
                raise RuntimeError(err_str)

        wrapper.__name__ = f"{class_func.__name__}__fail_safe"
        return wrapper

    return decorator


def get_auth():
    """
    Get the URL and access token
    """
    url = None
    token = None

    # Try reading from config file
    for filename in (
        os.path.join(os.path.expanduser("~"), ".simvue.ini"),
        "simvue.ini",
    ):
        with contextlib.suppress(Exception):
            config = configparser.ConfigParser()
            config.read(filename)
            token = config.get("server", "token")
            url = config.get("server", "url")

    # Try environment variables
    token = os.getenv("SIMVUE_TOKEN", token)
    url = os.getenv("SIMVUE_URL", url)

    if not token:
        raise ValueError("No Simvue server token was specified")
    if not url:
        raise ValueError("No Simvue server URL was specified")

    return url, token


def get_offline_directory() -> pathlib.Path:
    """
    Get directory for offline cache
    """
    directory = None

    for filename in (
        os.path.join(os.path.expanduser("~"), ".simvue.ini"),
        "simvue.ini",
    ):
        with contextlib.suppress(Exception):
            config = configparser.ConfigParser()
            config.read(filename)
            directory = config.get("offline", "cache")

    if not directory:
        directory = os.path.join(os.path.expanduser("~"), ".simvue")

    directory = os.environ.get("SIMVUE_OFFLINE_DIRECTORY", directory)

    return pathlib.Path(directory)


def create_file(filename):
    """
    Create an empty file
    """
    try:
        with open(filename, "w") as fh:
            fh.write("")
    except Exception as err:
        logger.error("Unable to write file %s due to: %s", filename, str(err))


def remove_file(filename):
    """
    Remove file
    """
    if os.path.isfile(filename):
        try:
            os.remove(filename)
        except Exception as err:
            logger.error("Unable to remove file %s due to: %s", filename, str(err))


def get_expiry(token) -> typing.Optional[int]:
    """
    Get expiry date from a JWT token
    """
    expiry: typing.Optional[int] = None
    with contextlib.suppress(jwt.DecodeError):
        expiry = jwt.decode(token, options={"verify_signature": False})["exp"]

    return expiry


def prepare_for_api(data_in, all=True):
    """
    Remove references to pickling
    """
    data = data_in.copy()
    if "pickled" in data:
        del data["pickled"]
    if "pickledFile" in data and all:
        del data["pickledFile"]
    return data


def calculate_sha256(filename: str, is_file: bool) -> typing.Optional[str]:
    """
    Calculate sha256 checksum of the specified file
    """
    sha256_hash = hashlib.sha256()
    if is_file:
        try:
            with open(filename, "rb") as fd:
                for byte_block in iter(lambda: fd.read(CHECKSUM_BLOCK_SIZE), b""):
                    sha256_hash.update(byte_block)
                return sha256_hash.hexdigest()
        except Exception:
            return None

    if isinstance(filename, str):
        sha256_hash.update(bytes(filename, "utf-8"))
    else:
        sha256_hash.update(bytes(filename))
    return sha256_hash.hexdigest()


def validate_timestamp(timestamp):
    """
    Validate a user-provided timestamp
    """
    try:
        datetime.datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S.%f")
    except ValueError:
        return False

    return True


def compare_alerts(first, second):
    """ """
    for key in ("name", "description", "source", "frequency", "notification"):
        if key in first and key in second:
            if not first[key]:
                continue

            if first[key] != second[key]:
                return False

    if "alerts" in first and "alerts" in second:
        for key in ("rule", "window", "metric", "threshold", "range_low", "range_high"):
            if key in first["alerts"] and key in second["alerts"]:
                if not first[key]:
                    continue

                if first["alerts"][key] != second["alerts"]["key"]:
                    return False

    return True
