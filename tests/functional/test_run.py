import unittest
import uuid
from simvue import Run, Client

class TestRun(unittest.TestCase):
    def test_basic_run(self):
        """
        Create a run & check that it exists
        """
        name = 'test-%s' % str(uuid.uuid4())
        folder = '/test-%s' % str(uuid.uuid4())
        run = Run()
        run.init(name, folder=folder)
        run.close()

        client = Client()
        data = client.get_run(run.id)
        self.assertEqual(name, data['name'])

        runs = client.delete_runs(folder)
        self.assertEqual(len(runs), 1)

if __name__ == '__main__':
    unittest.main()
