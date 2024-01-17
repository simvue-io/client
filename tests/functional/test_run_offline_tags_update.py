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
    def test_run_offline_tags_update(self):
        """
        Check tags can be updated & retrieved
        """
        common.update_config()
        try:
            shutil.rmtree("./offline")
        except:
            pass

        name = "test-%s" % str(uuid.uuid4())
        folder = "/test-%s" % str(uuid.uuid4())
        tags = ["a1"]
        run = Run()
        run.init(name, tags=tags, folder=folder)

        sender()

        tags = ["b2"]
        run.update_tags(tags)
        run.close()

        sender()

        client = Client()
        data = client.get_runs([f"name == {name}"], tags=True)
        self.assertEqual(len(data), 1)
        self.assertEqual(name, data[0]["name"])
        self.assertEqual(tags, data[0]["tags"])

        runs = client.delete_folder(folder, runs=True)


if __name__ == "__main__":
    unittest.main()
