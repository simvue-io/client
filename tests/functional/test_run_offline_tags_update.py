import unittest
import uuid
from simvue import Run, Client

import common

class TestRunOffline(unittest.TestCase):
    def test_run_tags_update(self):
        """
        Check tags can be updated & retrieved
        """
        name = 'test-%s' % str(uuid.uuid4())
        folder = '/test-%s' % str(uuid.uuid4())
        tags = ['a1']
        run = Run()
        run.init(name, tags=tags, folder=folder)

        run.update_tags(['a1', 'b2'])
        tags.append('b2')
        run.close()

        client = Client()
        data = client.get_run(run.id, tags=True)
        self.assertEqual(tags, data['tags'])

        runs = client.delete_runs(folder)
        self.assertEqual(len(runs), 1)

if __name__ == '__main__':
    unittest.main()
