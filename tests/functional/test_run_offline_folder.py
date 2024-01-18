import configparser
import filecmp
import os
import random
import shutil
import time
import unittest
import uuid
from simvue import Run, Client
from simvue.sender import sender

import common


class TestRunOfflineFolder(unittest.TestCase):
    def test_basic_run_folder(self):
        """
        Check that a folder metadata can be specified in offline mode
        """
        common.update_config()
        try:
            shutil.rmtree("./offline")
        except:
            pass

        name = "test-%s" % str(uuid.uuid4())
        folder = "/test-%s" % str(uuid.uuid4())
        metadata = {str(uuid.uuid4()): 100 * random.random()}
        run = Run("offline")
        run.init(name, folder=folder)
        run.set_folder_details(path=folder, metadata=metadata)
        run.close()

        sender()

        client = Client()
        data = client.get_folders([f"path == {folder}"])
        self.assertEqual(len(data), 1)
        self.assertEqual(folder, data[0]["path"])
        self.assertEqual(metadata, data[0]["metadata"])

        runs = client.delete_folder(folder, runs=True)


if __name__ == "__main__":
    unittest.main()
