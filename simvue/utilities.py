import configparser
import contextlib
import importlib.util
import logging
import os
import typing

import jwt

logger = logging.getLogger(__name__)


def check_extra(extra_name: str) -> typing.Callable:
    def decorator(class_func: typing.Callable) -> typing.Callable:
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
                    f"PyTorch features require the '{extra_name}' extension to Simvue"
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
            elif extra_name:
                raise RuntimeError(f"Unrecognised extra '{extra_name}'")
            return class_func(self, *args, **kwargs)

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
                    f"Skipping call to '{class_func.__name__}', client in fail state (see logs)."
                )
                return on_failure_return
            return class_func(self, *args, **kwargs)

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

    return url, token


def get_offline_directory():
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

    return directory


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


def get_expiry(token):
    """
    Get expiry date from a JWT token
    """
    expiry = 0
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
