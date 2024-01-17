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


class TestRunOfflineMetadata(unittest.TestCase):
    def test_run_metadata(self):
        """
        Create a run with metadata, upload it & check that it exists
        """
        common.update_config()
        try:
            shutil.rmtree("./offline")
        except:
            pass

        name = "test-%s" % str(uuid.uuid4())
        folder = "/test-%s" % str(uuid.uuid4())
        metadata = {"a": "string", "b": 1, "c": 2.5}
        run = Run("offline")
        run.init(name, metadata=metadata, folder=folder)
        run.close()

        sender()

        client = Client()
        data = client.get_runs([f"name == {name}"], metadata=True)
        self.assertEqual(len(data), 1)
        self.assertEqual(name, data[0]["name"])
        self.assertEqual(metadata, data[0]["metadata"])

        runs = client.delete_folder(folder, runs=True)


if __name__ == "__main__":
    unittest.main()
