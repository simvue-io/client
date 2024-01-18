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

        run.log_metrics({"a": 1.0})
        run.log_metrics({"a": 1.2})

        run.log_metrics({"b": 2.0}, step=10, time=2.0)
        run.log_metrics({"b": 2.3}, step=11, time=3.0)

        run.close()

        sender()

        time.sleep(5)

        client = Client()
        data = client.get_runs([f"name == {name}"])
        run_id = data[0]["id"]

        data_a = client.get_metrics(run_id, "a", "step")
        data_b = client.get_metrics(run_id, "b", "step")
        data_b_time = client.get_metrics(run_id, "b", "time")

        data_a_val = [[0, 1.0, name, "a"], [1, 1.2, name, "a"]]
        data_b_val = [[10, 2.0, name, "b"], [11, 2.3, name, "b"]]
        data_b_time_val = [[2.0, 2.0, name, "b"], [3.0, 2.3, name, "b"]]

        self.assertEqual(data_a, data_a_val)
        self.assertEqual(data_b, data_b_val)
        self.assertEqual(data_b_time, data_b_time_val)

        metrics_names = client.get_metrics_names(run_id)
        self.assertEqual(metrics_names, ["a", "b"])

        runs = client.delete_folder(folder, runs=True)


if __name__ == "__main__":
    unittest.main()
