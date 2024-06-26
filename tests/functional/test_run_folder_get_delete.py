import unittest
import uuid
from simvue import Run, Client

class TestRunFolderGetDelete(unittest.TestCase):
    def test_run_folder_find_delete(self):
        """
        Create a run & folder, find the folder then delete it
        """
        name = 'test-%s' % str(uuid.uuid4())
        folder = '/tests/%s' % str(uuid.uuid4())
        run = Run()
        run.init(name, folder=folder)
        run.close()

        client = Client()
        data = client.get_folder(folder)
        self.assertEqual(data['path'], folder)

        client.delete_folder(folder, remove_runs=True)

        client = Client()
        with self.assertRaises(Exception) as context:
            client.get_folder(folder)

        self.assertTrue(f"Folder '{folder}' does not exist" in str(context.exception))

if __name__ == '__main__':
    unittest.main()
