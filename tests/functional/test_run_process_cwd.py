import unittest
import uuid
import tempfile
import simvue
from simvue import Run
import os
import filecmp

class TestRunProcess(unittest.TestCase):
    def test_processes_cwd(self):
        """Check that cwd argument works correctly in add_process.

        Create a temporary directory, and a python file inside that directory. Check that if only the file name
        is passed to add_process as the script, and the directory is specified as the cwd argument, that the process
        runs correctly and the script is uploaded as expected.
        """
        with tempfile.TemporaryDirectory() as temp_dir:
            with tempfile.NamedTemporaryFile(dir=temp_dir, suffix=".py") as temp_file:
                with open(temp_file.name, "w") as out_f:
                    out_f.writelines([
                        "import time\n",
                        "time.sleep(10)\n"
                    ])
                    file_name = os.path.basename(temp_file.name)
                    
                with Run() as run:
                    run.init(
                        name='test-%s' % str(uuid.uuid4()),
                        folder='/test-%s' % str(uuid.uuid4())
                    )
                    run_id = run._id
                    run.add_process(
                        identifier="sleep_10_process",
                        executable="python",
                        script=file_name,
                        cwd=temp_dir
                    )

                client = simvue.Client()

                # Check that run existed for more than 10s, meaning the process ran correctly
                data = client.get_run(run_id)
                runtime = data['runtime']
                runtime_seconds = float(runtime.split(":")[-1])
                self.assertGreater(runtime_seconds, 10.0)

                # Check that the script was uploaded to the run correctly
                os.makedirs(os.path.join(temp_dir, "downloaded"))
                client.get_artifact_as_file(run_id, file_name, path=os.path.join(temp_dir, "downloaded"))
                assert filecmp.cmp(os.path.join(temp_dir, "downloaded", file_name), temp_file.name)

if __name__ == '__main__':
    unittest.main()