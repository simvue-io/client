import configparser
import jwt
import logging
import os
import requests

logger = logging.getLogger(__name__)

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

    if not token:
        raise AssertionError("Failed to retrieve token")

    if not url:
        raise AssertionError("Failed to retrieve URL")

    return url, token

def get_server_version():
    """
    Get the server version
    """
    url, _ = get_auth()

    try:
        response = requests.get(f"{url}/api/version")
    except:
        pass
    else:
        if response.status_code == 200:
            return 1
    return 0

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
