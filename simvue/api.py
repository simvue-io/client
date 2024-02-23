import copy
import json
import typing
import requests
from tenacity import retry, wait_exponential, stop_after_attempt

DEFAULT_API_TIMEOUT = 10
RETRY_MULTIPLIER = 1
RETRY_MIN = 4
RETRY_MAX = 10
RETRY_STOP = 5


def set_json_header(
    headers: dict[str, str]
) -> dict[str, str]: 
    """
    Return a copy of the headers with Content-Type set to
    application/json
    """
    headers = copy.deepcopy(headers)
    headers['Content-Type'] = 'application/json'
    return headers


@retry(
    wait=wait_exponential(
        multiplier=RETRY_MULTIPLIER,
        min=RETRY_MIN,
        max=RETRY_MAX
    ),
    stop=stop_after_attempt(RETRY_STOP),
    reraise=True
)
def post(
    url: str,
    headers: dict[str, str],
    data: dict[str, typing.Any],
    is_json: bool=True
) -> requests.Response:
    """HTTP POST with retries

    Parameters
    ----------
    url : str
        URL to post to
    headers : dict[str, str]
        headers for the post request
    data : dict[str, typing.Any]
        data to post
    is_json : bool, optional
        send as JSON string, by default True

    Returns
    -------
    requests.Response
        response from post to server

    """
    if is_json:
        data_sent: typing.Union[str, dict[str, typing.Any]] = json.dumps(data)
        headers = set_json_header(headers)
    else:
        data_sent = data
    
    response = requests.post(
        url,
        headers=headers,
        data=data_sent,
        timeout=DEFAULT_API_TIMEOUT
    )
    
    if response.status_code in (401, 403):
        raise RuntimeError(
            f'Authorization error [{response.status_code}]: {response.text}'
        )
        
    if response.status_code not in (200, 201, 409):
        raise RuntimeError(
            f'HTTP error [{response.status_code}]: {response.text}'
        )

    return response


@retry(
    wait=wait_exponential(
        multiplier=RETRY_MULTIPLIER,
        min=RETRY_MIN,
        max=RETRY_MAX
    ),
    stop=stop_after_attempt(RETRY_STOP)
)
def put(
    url: str,
    headers: dict[str, str],
    data: dict[str, typing.Any],
    is_json: bool=True,
    timeout=DEFAULT_API_TIMEOUT
) -> requests.Response:
    """HTTP POST with retries

    Parameters
    ----------
    url : str
        URL to put to
    headers : dict[str, str]
        headers for the post request
    data : dict[str, typing.Any]
        data to put
    is_json : bool, optional
        send as JSON string, by default True
    timeout : _type_, optional
        timeout of request, by default DEFAULT_API_TIMEOUT

    Returns
    -------
    requests.Response
        response from executing PUT
    """
    if is_json:
        data_sent: typing.Union[str, dict[str, typing.Any]] = json.dumps(data)
        headers = set_json_header(headers)
    else:
        data_sent = data
    
    response = requests.put(
        url,
        headers=headers,
        data=data_sent,
        timeout=timeout
    )

    response.raise_for_status()

    return response

def get(url, headers, timeout=DEFAULT_API_TIMEOUT):
    """
    HTTP GET
    """
    response = requests.get(url, headers=headers, timeout=timeout)
    response.raise_for_status()

    return response
