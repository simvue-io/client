import configparser
import jwt
import logging
import os
import requests
import typing

logger = logging.getLogger(__name__)

def check_extra(extra_name: str) -> typing.Callable:
    def decorator(class_func: typing.Callable) -> typing.Callable:
        def wrapper(self, *args, **kwargs) -> typing.Any:
            if extra_name == "plot":
                try:
                    import matplotlib
                    import plotly
                except ImportError:
                    raise RuntimeError(f"Plotting features require the '{extra_name}' extension to Simvue")
            elif extra_name == "torch":
                try:
                    import torch
                except ImportError:
                    raise RuntimeError(f"PyTorch features require the '{extra_name}' extension to Simvue")
            elif extra_name == "dataset":
                try:
                    import pandas
                    import numpy
                except ImportError:
                    raise RuntimeError(f"Dataset features require the '{extra_name}' extension to Simvue")
            else:
                raise RuntimeError(f"Unrecognised extra '{extra_name}'")
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
    for filename in (os.path.join(os.path.expanduser("~"), '.simvue.ini'), 'simvue.ini'):
        try:
            config = configparser.ConfigParser()
            config.read(filename)
            token = config.get('server', 'token')
            url = config.get('server', 'url')
        except:
            pass

    # Try environment variables
    token = os.getenv('SIMVUE_TOKEN', token)
    url = os.getenv('SIMVUE_URL', url)

    return url, token

def get_offline_directory():
    """
    Get directory for offline cache
    """
    directory = None

    for filename in (os.path.join(os.path.expanduser("~"), '.simvue.ini'), 'simvue.ini'):
        try:
            config = configparser.ConfigParser()
            config.read(filename)
            directory = config.get('offline', 'cache')
        except:
            pass

    if not directory:
        directory = os.path.join(os.path.expanduser("~"), ".simvue")

    return directory

def create_file(filename):
    """
    Create an empty file
    """
    try:
        with open(filename, 'w') as fh:
            fh.write('')
    except Exception as err:
        logger.error('Unable to write file %s due to: %s', filename, str(err))

def remove_file(filename):
    """
    Remove file
    """
    if os.path.isfile(filename):
        try:
            os.remove(filename)
        except Exception as err:
            logger.error('Unable to remove file %s due to: %s', filename, str(err))

def get_expiry(token):
    """
    Get expiry date from a JWT token
    """
    expiry = 0
    try:
        expiry = jwt.decode(token, options={"verify_signature": False})['exp']
    except:
        pass
    return expiry

def prepare_for_api(data_in, all=True):
    """
    Remove references to pickling
    """
    data = data_in.copy()
    if 'pickled' in data:
        del data['pickled']
    if 'pickledFile' in data and all:
        del data['pickledFile']
    return data
