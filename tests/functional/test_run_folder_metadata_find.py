import unittest
import uuid
from simvue import Run, Client

class TestRunFolderMetadataFind(unittest.TestCase):
    def test_run_folder_metadata_find(self):
        """
        Create a run & folder with metadata, find it then delete it
        """
        name = 'test-%s' % str(uuid.uuid4())
        folder = '/tests/%s' % str(uuid.uuid4())
        run = Run()
        run.init(name, folder=folder)
        run.set_folder_details(path=folder, metadata={'atest': 5.0})
        run.close()

        client = Client()
        data = client.get_folders(['atest == 5.0'])
        found = False
        for item in data:
            if item['path'] == folder:
                found = True
        self.assertTrue(found)

        client.delete_folder(folder, remove_runs=True)

        client = Client()
        with self.assertRaises(Exception) as context:
            client.get_folder(folder)

        self.assertTrue(f"Folder '{folder}' does not exist" in str(context.exception))

if __name__ == '__main__':
    unittest.main()
