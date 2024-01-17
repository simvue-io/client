import configparser
import filecmp
import os
import shutil
import time
import unittest
import uuid
from simvue import Run, Client
from simvue.sender import sender

import common


class TestRunOffline(unittest.TestCase):
    def test_basic_run(self):
        """
        Create a run, upload it & check that it exists
        """
        common.update_config()
        try:
            shutil.rmtree("./offline")
        except:
            pass

        name = "test-%s" % str(uuid.uuid4())
        folder = "/test-%s" % str(uuid.uuid4())
        run = Run("offline")
        run.init(name, folder=folder)
        run.close()

        sender()

        client = Client()
        data = client.get_runs([f"name == {name}"])
        self.assertEqual(len(data), 1)
        self.assertEqual(name, data[0]["name"])

        runs = client.delete_folder(folder, runs=True)


if __name__ == "__main__":
    unittest.main()
