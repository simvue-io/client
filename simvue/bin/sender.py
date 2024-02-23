"""Send runs to server"""
import getpass
import os
import logging
import sys
import tempfile

from simvue.sender import sender
from simvue.utilities import create_file, remove_file

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

def run() -> None:
    lockfile = os.path.join(tempfile.gettempdir(), f"simvue-{getpass.getuser()}.lock")
    
    if os.path.isfile(lockfile):
        logger.error("Cannot initiate run, locked by other process.")
        sys.exit(1)

    create_file(lockfile)
    try:
        sender()
    except Exception as err:
        logger.critical('Exception running sender: %s', str(err))

    remove_file(lockfile)
