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


class TestRunOfflineMetrics(unittest.TestCase):
    def test_run_metrics(self):
        """
        Try logging metrics and retrieving them from an offline run
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

        run.log_event('test-event-1', timestamp='2022-01-03 16:42:30.849617')
        run.log_event('test-event-2', timestamp='2022-01-03 16:42:31.849617')

        run.close()

        sender()

        time.sleep(5)

        client = Client()
        run_id = client.get_run_id_from_name(name)
        data = client.get_events(run_id)

        data_compare = [{'timestamp': '2022-01-03 16:42:30.849617', 'message': 'test-event-1'},
                        {'timestamp': '2022-01-03 16:42:31.849617', 'message': 'test-event-2'}]

        self.assertEqual(data, data_compare)

        runs = client.delete_runs(folder)
        self.assertEqual(len(runs), 1)


if __name__ == "__main__":
    unittest.main()
