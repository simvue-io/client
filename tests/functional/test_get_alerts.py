import time
import unittest
import uuid
from simvue import Run, Client
from simvue.sender import sender

import common

class TestGetAlerts(unittest.TestCase):
    def test_get_alerts(self):
        """
        Create a run & two alerts, make one alert be triggered
        If critical_only is True, check that only the triggered alert is returned
        IF critical_only is False, check both alerts are returned
        If names_only is False, check that full dictionary of information is returned
        """
        run = Run()
        client = Client()
        folder = '/test-%s' % str(uuid.uuid4())
        name = common.RUNNAME3
        run.init(name, folder=folder)
        run.create_alert(
            name='value_below_1',
            source='metrics',
            frequency=1,
            rule='is below',
            threshold=1,
            metric='test_metric',
            window=2
        )
        run.create_alert(
            name='value_above_1',
            source='metrics',
            frequency=1,
            rule='is above',
            threshold=1,
            metric='test_metric',
            window=2
        )

        run.log_metrics({'test_metric': 5})
        time.sleep(180)

        run_id = client.get_run_id_from_name(name)

        triggered_alerts_names = client.get_alerts(run_id)
        self.assertListEqual(triggered_alerts_names, ['value_above_1'])

        triggered_alerts_full = client.get_alerts(run_id, names_only=False)
        self.assertIsInstance(triggered_alerts_full[0], dict)
        self.assertEqual(triggered_alerts_full[0]["alert"]["name"], "value_above_1")
        self.assertEqual(triggered_alerts_full[0]["status"]["current"], "critical")

        all_alerts_names = client.get_alerts(run_id, critical_only=False)
        self.assertListEqual(all_alerts_names, ['value_above_1', 'value_below_1'])

        run.close()

if __name__ == '__main__':
    unittest.main()
