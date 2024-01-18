import random
import unittest
import uuid
from simvue import Run, Client


class TestRunFolderMetadataFind(unittest.TestCase):
    def test_run_folder_metadata_find(self):
        """
        Create a run & folder with metadata, find it then delete it
        """
        name = "test-%s" % str(uuid.uuid4())
        folder = "/tests/%s" % str(uuid.uuid4())
        key = str(uuid.uuid4())
        metadata = {key: 100 * random.random()}
        run = Run()
        run.init(name, folder=folder)
        run.set_folder_details(path=folder, metadata=metadata)
        run.close()

        client = Client()
        data = client.get_folders([f"metadata.{key} == {metadata[key]}"])
        found = False
        for item in data:
            if item["path"] == folder:
                found = True
        self.assertTrue(found)

        runs = client.delete_folder(folder, runs=True)

        client = Client()
        with self.assertRaises(Exception) as context:
            client.get_folder(folder)

        self.assertTrue("Folder does not exist" in str(context.exception))


if __name__ == "__main__":
    unittest.main()
