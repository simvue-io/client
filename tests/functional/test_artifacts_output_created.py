import configparser
import filecmp
import os
import shutil
import time
import unittest
import uuid
from simvue import Run, Client

import common

class TestArtifactsOutputCreated(unittest.TestCase):
    def test_artifact_output_created(self):
        """
        Create a run in the created state and check that an output file
        cannot be saved.
        """
        run = Run()
        folder = '/test-%s' % str(uuid.uuid4())
        run.init(common.RUNNAME3, folder=folder, running=False)

        content = str(uuid.uuid4())
        with open(common.FILENAME3, 'w') as fh:
            fh.write(content)

        with self.assertRaises(Exception) as context:
            run.save_file(common.FILENAME3, 'output')

        self.assertTrue('Cannot upload output files for runs in the created state' in str(context.exception))

        client = Client()
        runs = client.delete_runs(folder)
        self.assertEqual(len(runs), 1)

        os.remove(common.FILENAME3)

if __name__ == '__main__':
    unittest.main()
