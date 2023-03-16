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
    def test_run_folder(self):
        """
        Check specified folder of run
        """
        name = 'test-%s' % str(uuid.uuid4())
        run = Run()
        run.init(name, folder=common.FOLDER)
        run.close()

        client = Client()
        data = client.get_run(name)
        self.assertEqual(data['folder'], common.FOLDER)

        runs = client.delete_runs(common.FOLDER)
        self.assertEqual(len(runs), 1)

if __name__ == '__main__':
    unittest.main()