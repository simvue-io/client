import pytest
import time
import typing
import contextlib
import inspect
import tempfile
import uuid
import pathlib
import os
import concurrent.futures
import random

import simvue.run as sv_run
import simvue.client as sv_cl
import simvue.sender as sv_send

if typing.TYPE_CHECKING:
    from .conftest import CountingLogHandler


@pytest.fixture(scope="session")
def create_project_directories() -> (
    typing.Generator[tuple[list[pathlib.Path], list[pathlib.Path]], None, None]
):
    N_DIRECTORIES: int = 5
    N_FILES_PER_LEVEL: int = 2
    N_LEVELS: int = 3

    with tempfile.TemporaryDirectory() as tempd:
        for d in range(N_DIRECTORIES):
            subdirectories: list[str] = [f"parent_{d}"] + [
                f"dir_{i}" for i in range(N_LEVELS)
            ]
            os.makedirs(os.path.join(tempd, *subdirectories), exist_ok=True)
            directories: list[pathlib.Path] = []
            files: list[pathlib.Path] = []

            for i, _ in enumerate(subdirectories):
                for j in range(N_FILES_PER_LEVEL):
                    out_name = pathlib.Path(tempd).joinpath(
                        *subdirectories[: i + 1], f"test_file_{j}.txt"
                    )
                    if j == 0:
                        out_name.touch()
                        continue

                    with open(
                        out_name,
                        "w",
                    ) as out_f:
                        out_f.write(f"test data entry file {j}")
                    files.append(out_name)
            directories.append(pathlib.Path(tempd).joinpath(f"parent_{d}"))
        yield directories, files


@pytest.mark.run
@pytest.mark.parametrize("overload_buffer", (True, False), ids=("overload", "normal"))
@pytest.mark.parametrize("visibility", ("bad_option", "tenant", "public", ["ciuser01"], None))
def test_log_metrics(
    overload_buffer: bool,
    setup_logging: "CountingLogHandler",
    mocker,
    visibility: typing.Union[typing.Literal["public", "tenant"], list[str], None]
) -> None:
    METRICS = {"a": 10, "b": 1.2}

    setup_logging.captures = ["'a'", "resources/"]

    # Have to create the run outside of fixtures because the resources dispatch
    # occurs immediately and is not captured by the handler when using the fixture
    run = sv_run.Run()
    run.config(suppress_errors=False)

    if visibility == "bad_option":
        with pytest.raises(RuntimeError):
            run.init(
                name=f"test_run_{str(uuid.uuid4()).split('-', 1)[0]}",
                tags=["simvue_client_unit_tests"],
                folder="/simvue_unit_testing",
                retention_period="1 hour",
                visibility=visibility
            )
        return

    run.init(
        name=f"test_run_{str(uuid.uuid4()).split('-', 1)[0]}",
        tags=["simvue_client_unit_tests"],
        folder="/simvue_unit_testing",
        visibility=visibility,
        retention_period="1 hour",
    )

    run.update_tags(["simvue_client_unit_tests", "test_log_metrics"])

    # Speed up the read rate for this test
    run._dispatcher._max_buffer_size = 10
    run._dispatcher._max_read_rate *= 10

    if overload_buffer:
        for i in range(run._dispatcher._max_buffer_size * 3):
            run.log_metrics({key: i for key in METRICS.keys()})
    else:
        run.log_metrics(METRICS)
    time.sleep(1.0 if not overload_buffer else 2.0)
    run.close()
    client = sv_cl.Client()
    _data = client.get_metric_values(
        run_ids=[run._id],
        metric_names=list(METRICS.keys()),
        xaxis="step",
        aggregate=False,
    )

    with contextlib.suppress(RuntimeError):
        client.delete_run(run._id)

    assert sorted(set(METRICS.keys())) == sorted(set(_data.keys()))
    _steps = []
    for entry in _data.values():
        _steps += list(i[0] for i in entry.keys())
    _steps = set(_steps)
    assert (
        len(_steps) == 1
        if not overload_buffer
        else run._dispatcher._max_buffer_size * 3
    )

    # Check metrics have been set
    assert setup_logging.counts[0] == 1 if not overload_buffer else 3

    # Check heartbeat has been called at least once (so sysinfo sent)
    assert setup_logging.counts[1] > 0


@pytest.mark.run
def test_log_metrics_offline(
    create_plain_run_offline: tuple[sv_run.Run, dict],
) -> None:
    METRICS = {"a": 10, "b": 1.2, "c": 2}
    run, _ = create_plain_run_offline
    run.update_tags(["simvue_client_unit_tests", "test_log_metrics_offline"])
    run.log_metrics(METRICS)
    time.sleep(1.0)
    run.close()
    assert (run_id := sv_send.sender())
    client = sv_cl.Client()
    _data = client.get_metric_values(
        run_ids=[run_id],
        metric_names=list(METRICS.keys()),
        xaxis="step",
        aggregate=False,
    )

    assert _data, f"No metrics returned for run '{run_id}"

    with contextlib.suppress(RuntimeError):
        client.delete_run(run._id)

    assert sorted(set(METRICS.keys())) == sorted(set(_data.keys()))
    _steps = []
    for entry in _data.values():
        _steps += list(i[0] for i in entry.keys())
    _steps = set(_steps)


@pytest.mark.run
def test_log_events_online(create_test_run: tuple[sv_run.Run, dict]) -> None:
    EVENT_MSG = "Hello world!"
    run, _ = create_test_run
    run.update_tags(["simvue_client_unit_tests", "test_log_events"])
    run.log_event(EVENT_MSG)


@pytest.mark.run
def test_log_events_offline(create_test_run_offline: tuple[sv_run.Run, dict]) -> None:
    EVENT_MSG = "Hello world!"
    run, _ = create_test_run_offline
    run.update_tags(["simvue_client_unit_tests", "test_log_events_offline"])
    run.log_event(EVENT_MSG)
    assert sv_send.sender()


@pytest.mark.run
def test_update_metadata_online(create_test_run: tuple[sv_run.Run, dict]) -> None:
    METADATA = {"a": 10, "b": 1.2, "c": "word"}
    run, _ = create_test_run
    run.update_tags(["simvue_client_unit_tests", "test_update_metadata"])
    run.update_metadata(METADATA)


@pytest.mark.run
def test_update_metadata_offline(
    create_test_run_offline: tuple[sv_run.Run, dict],
) -> None:
    METADATA = {"a": 10, "b": 1.2, "c": "word"}
    run, _ = create_test_run_offline
    run.update_tags(["simvue_client_unit_tests", "test_update_metadata_offline"])
    run.update_metadata(METADATA)
    assert sv_send.sender()


@pytest.mark.run
@pytest.mark.parametrize("multi_threaded", (True, False), ids=("multi", "single"))
def test_runs_multiple_parallel(multi_threaded: bool) -> None:
    N_RUNS: int = 2
    if multi_threaded:

        def thread_func(index: int) -> tuple[int, list[dict[str, typing.Any]], str]:
            with sv_run.Run() as run:
                run.config(suppress_errors=False)
                run.init(
                    name=f"test_runs_multiple_{index + 1}",
                    tags=["simvue_client_unit_tests", "test_multi_run_threaded"],
                    folder="/simvue_unit_testing",
                    retention_period="1 hour",
                )
                metrics = []
                for _ in range(10):
                    time.sleep(1)
                    metric = {f"var_{index + 1}": random.random()}
                    metrics.append(metric)
                    run.log_metrics(metric)
            return index, metrics, run._id

        with concurrent.futures.ThreadPoolExecutor(max_workers=N_RUNS) as executor:
            futures = [executor.submit(thread_func, i) for i in range(N_RUNS)]

            time.sleep(1)

            client = sv_cl.Client()

            for future in concurrent.futures.as_completed(futures):
                id, metrics, run_id = future.result()
                assert metrics
                assert client.get_metric_values(
                    run_ids=[run_id],
                    metric_names=[f"var_{id + 1}"],
                    xaxis="step",
                    output_format="dict",
                    aggregate=False,
                )
                with contextlib.suppress(RuntimeError):
                    client.delete_run(run_id)
    else:
        with sv_run.Run() as run_1:
            with sv_run.Run() as run_2:
                run_1.config(suppress_errors=False)
                run_1.init(
                    name="test_runs_multiple_unthreaded_1",
                    tags=["simvue_client_unit_tests", "test_multi_run_unthreaded"],
                    folder="/simvue_unit_testing",
                    retention_period="1 hour",
                )
                run_2.config(suppress_errors=False)
                run_2.init(
                    name="test_runs_multiple_unthreaded_2",
                    tags=["simvue_client_unit_tests", "test_multi_run_unthreaded"],
                    folder="/simvue_unit_testing",
                    retention_period="1 hour",
                )
                metrics_1 = []
                metrics_2 = []
                for _ in range(10):
                    time.sleep(1)
                    for index, (metrics, run) in enumerate(
                        zip((metrics_1, metrics_2), (run_1, run_2))
                    ):
                        metric = {f"var_{index}": random.random()}
                        metrics.append(metric)
                        run.log_metrics(metric)

                time.sleep(1)

                client = sv_cl.Client()

                for i, run_id in enumerate((run_1._id, run_2._id)):
                    assert metrics
                    assert client.get_metric_values(
                        run_ids=[run_id],
                        metric_names=[f"var_{i}"],
                        xaxis="step",
                        output_format="dict",
                        aggregate=False,
                    )

        with contextlib.suppress(RuntimeError):
            client.delete_run(run_1._id)
            client.delete_run(run_2._id)


@pytest.mark.run
def test_runs_multiple_series() -> None:
    N_RUNS: int = 2

    metrics = []
    run_ids = []

    for index in range(N_RUNS):
        with sv_run.Run() as run:
            run_metrics = []
            run.config(suppress_errors=False)
            run.init(
                name=f"test_runs_multiple_series_{index}",
                tags=["simvue_client_unit_tests", "test_multi_run_series"],
                folder="/simvue_unit_testing",
                retention_period="1 hour",
            )
            run_ids.append(run._id)
            for _ in range(10):
                time.sleep(1)
                metric = {f"var_{index}": random.random()}
                run_metrics.append(metric)
                run.log_metrics(metric)
        metrics.append(run_metrics)

    time.sleep(1)

    client = sv_cl.Client()

    for i, run_id in enumerate(run_ids):
        assert metrics[i]
        assert client.get_metric_values(
            run_ids=[run_id],
            metric_names=[f"var_{i}"],
            xaxis="step",
            output_format="dict",
            aggregate=False,
        )

    with contextlib.suppress(RuntimeError):
        for run_id in run_ids:
            client.delete_run(run_id)


@pytest.mark.run
@pytest.mark.parametrize("post_init", (True, False), ids=("pre-init", "post-init"))
def test_suppressed_errors(
    setup_logging: "CountingLogHandler", post_init: bool
) -> None:
    setup_logging.captures = ["Skipping call to"]

    with sv_run.Run(mode="offline") as run:
        decorated_funcs = [
            name
            for name, method in inspect.getmembers(run, inspect.ismethod)
            if method.__name__.endswith("__fail_safe")
        ]

        if post_init:
            decorated_funcs.remove("init")
            run.init(
                name="test_suppressed_errors",
                folder="/simvue_unit_testing",
                tags=["simvue_client_unit_tests"],
                retention_period="1 hour",
            )

        run.config(suppress_errors=True)
        run._error("Oh dear this error happened :(")
        if run._dispatcher:
            assert run._dispatcher.empty
        for func in decorated_funcs:
            assert not getattr(run, func)()
    if post_init:
        assert setup_logging.counts[0] == len(decorated_funcs) + 1
    else:
        assert setup_logging.counts[0] == len(decorated_funcs)


@pytest.mark.run
def test_bad_run_arguments() -> None:
    with sv_run.Run() as run:
        with pytest.raises(RuntimeError):
            run.init("sdas", [34])


def test_set_folder_details_online() -> None:
    with sv_run.Run() as run:
        folder_name: str = "/simvue_unit_test_folder"
        description: str = "test description"
        tags: list[str] = ["simvue_client_unit_tests", "test_set_folder_details"]
        run.init(folder=folder_name)
        run.set_folder_details(path=folder_name, tags=tags, description=description)

    client = sv_cl.Client()
    assert (folder := client.get_folders([f"path == {folder_name}"])[0])["tags"] == tags
    assert folder["description"] == description


@pytest.mark.run
@pytest.mark.parametrize(
    "mimetype",
    ("text/plain", None, "text/text"),
    ids=("valid_mime", "unspecified", "invalid_mime"),
)
@pytest.mark.parametrize(
    "preserve_path", (True, False), ids=("preserve_path", "modified_path")
)
@pytest.mark.parametrize("name", ("test_file", None), ids=("named", "nameless"))
@pytest.mark.parametrize("empty_file", (True, False), ids=("empty", "content"))
def test_save_file_online(
    create_plain_run: typing.Tuple[sv_run.Run, dict],
    mimetype: typing.Optional[str],
    preserve_path: bool,
    name: typing.Optional[str],
    empty_file: bool,
    create_project_directories,
    capfd,
) -> None:
    simvue_run, _ = create_plain_run
    _, files = create_project_directories

    if not mimetype or mimetype == "text/plain":
        simvue_run.save_file(
            files[0 if empty_file else 1],
            category="input",
            filetype=mimetype,
            preserve_path=preserve_path,
            name=name,
        )
    else:
        with pytest.raises(RuntimeError):
            simvue_run.save_file(
                files[0 if empty_file else 1],
                category="input",
                filetype=mimetype,
                preserve_path=preserve_path,
            )
            return

        variable = capfd.readouterr()
        with capfd.disabled():
            # If MIME type is invalid the code will never reach this warning
            if empty_file and mimetype != "text/text":
                assert (
                    variable.out
                    == "WARNING: saving zero-sized files not currently supported\n"
                )


@pytest.mark.run
def test_set_folder_details_offline() -> None:
    with sv_run.Run(mode="offline") as run:
        folder_name: str = "/simvue_unit_test_folder"
        description: str = "test description"
        tags: list[str] = ["simvue_client_unit_tests", "test_set_folder_details_offline"]
        run.init(folder=folder_name)
        run.set_folder_details(path=folder_name, tags=tags, description=description)
    assert sv_send.sender()
    time.sleep(1)
    client = sv_cl.Client()
    assert (folder := client.get_folders([f"path == {folder_name}"])[0])["tags"] == tags
    assert folder["description"] == description


@pytest.mark.run
@pytest.mark.parametrize(
    "mimetype",
    ("text/plain", None, "text/text"),
    ids=("valid_mime", "unspecified", "invalid_mime"),
)
@pytest.mark.parametrize(
    "preserve_path", (True, False), ids=("preserve_path", "modified_path")
)
@pytest.mark.parametrize("name", ("test_file", None), ids=("named", "nameless"))
@pytest.mark.parametrize("empty_file", (True, False), ids=("empty", "content"))
def test_save_file_offline(
    create_plain_run_offline: typing.Tuple[sv_run.Run, dict],
    mimetype: typing.Optional[str],
    preserve_path: bool,
    name: typing.Optional[str],
    empty_file: bool,
    create_project_directories,
    capfd,
) -> None:
    simvue_run, _ = create_plain_run_offline
    simvue_run.update_tags(["simvue_client_unit_tests", "test_save_file_offline"])
    _, files = create_project_directories

    if not mimetype or mimetype == "text/plain":
        simvue_run.save_file(
            files[0 if empty_file else 1],
            category="input",
            filetype=mimetype,
            preserve_path=preserve_path,
            name=name,
        )
    else:
        with pytest.raises(RuntimeError):
            simvue_run.save_file(
                files[0 if empty_file else 1],
                category="input",
                filetype=mimetype,
                preserve_path=preserve_path,
            )
            return

        variable = capfd.readouterr()
        with capfd.disabled():
            # If MIME type is invalid the code will never reach this warning
            if empty_file and mimetype != "text/text":
                assert (
                    variable.out
                    == "WARNING: saving zero-sized files not currently supported\n"
                )
    assert sv_send.sender()


@pytest.mark.run
@pytest.mark.parametrize("object_type", ("DataFrame", "ndarray"))
def test_save_object_online(
    create_plain_run: typing.Tuple[sv_run.Run, dict], object_type: str
) -> None:
    simvue_run, _ = create_plain_run

    if object_type == "DataFrame":
        try:
            from pandas import DataFrame
        except ImportError:
            pytest.skip("Pandas is not installed")
        save_obj = DataFrame({"x": [1, 2, 3, 4], "y": [2, 4, 6, 8]})
    elif object_type == "ndarray":
        try:
            from numpy import array
        except ImportError:
            pytest.skip("Numpy is not installed")
        save_obj = array([1, 2, 3, 4])
    simvue_run.save_object(save_obj, "input", f"test_object_{object_type}")


@pytest.mark.run
@pytest.mark.parametrize("object_type", ("DataFrame", "ndarray"))
def test_save_object_offline(
    create_plain_run_offline: typing.Tuple[sv_run.Run, dict], object_type: str
) -> None:
    simvue_run, _ = create_plain_run_offline
    simvue_run.update_tags(["simvue_client_unit_tests", "test_save_object_offline"])

    if object_type == "DataFrame":
        try:
            from pandas import DataFrame
        except ImportError:
            pytest.skip("Pandas is not installed")
        save_obj = DataFrame({"x": [1, 2, 3, 4], "y": [2, 4, 6, 8]})
    elif object_type == "ndarray":
        try:
            from numpy import array
        except ImportError:
            pytest.skip("Numpy is not installed")
        save_obj = array([1, 2, 3, 4])
    simvue_run.save_object(save_obj, "input", f"test_object_{object_type}")
    assert sv_send.sender()


@pytest.mark.run
@pytest.mark.parametrize(
    "mimetype",
    ("text/plain", None, "text/text"),
    ids=("valid_mime", "unspecified", "invalid_mime"),
)
@pytest.mark.parametrize(
    "preserve_path", (True, False), ids=("preserve_path", "modified_path")
)
def test_save_directory_online(
    create_plain_run: typing.Tuple[sv_run.Run, dict],
    mimetype: typing.Optional[str],
    preserve_path: bool,
    create_project_directories,
) -> None:
    simvue_run, _ = create_plain_run
    directories, _ = create_project_directories

    if not mimetype or mimetype == "text/plain":
        simvue_run.save_directory(
            directories[0],
            category="input",
            contents_filetype=mimetype,
            preserve_path=preserve_path,
        )
    else:
        with pytest.raises(RuntimeError):
            simvue_run.save_directory(
                directories[0],
                category="input",
                contents_filetype=mimetype,
                preserve_path=preserve_path,
            )


@pytest.mark.run
@pytest.mark.parametrize(
    "mimetype",
    ("text/plain", None, "text/text"),
    ids=("valid_mime", "unspecified", "invalid_mime"),
)
@pytest.mark.parametrize(
    "preserve_path", (True, False), ids=("preserve_path", "modified_path")
)
def test_save_directory_offline(
    create_plain_run_offline: typing.Tuple[sv_run.Run, dict],
    mimetype: typing.Optional[str],
    preserve_path: bool,
    create_project_directories,
) -> None:
    simvue_run, _ = create_plain_run_offline
    simvue_run.update_tags(["simvue_client_unit_tests", "test_save_directory_offline"])
    directories, _ = create_project_directories

    if not mimetype or mimetype == "text/plain":
        simvue_run.save_directory(
            directories[0],
            category="input",
            contents_filetype=mimetype,
            preserve_path=preserve_path,
        )
    else:
        with pytest.raises(RuntimeError):
            simvue_run.save_directory(
                directories[0],
                category="input",
                contents_filetype=mimetype,
                preserve_path=preserve_path,
            )
    assert sv_send.sender()


@pytest.mark.run
@pytest.mark.parametrize(
    "mimetype",
    ("text/plain", None, "text/text"),
    ids=("valid_mime", "unspecified", "invalid_mime"),
)
@pytest.mark.parametrize(
    "preserve_path", (True, False), ids=("preserve_path", "modified_path")
)
def test_save_all_online(
    create_plain_run: typing.Tuple[sv_run.Run, dict],
    mimetype: typing.Optional[str],
    preserve_path: bool,
    create_project_directories,
) -> None:
    simvue_run, _ = create_plain_run
    directories, files = create_project_directories

    # Cherry pick some files and some directories
    save_files = files[len(files) // 2 :]
    save_directories = directories[len(directories) // 2 :]

    if not mimetype or mimetype == "text/plain":
        simvue_run.save_all(
            save_files + save_directories,
            category="input",
            contents_filetype=mimetype,
            preserve_path=preserve_path,
        )
    else:
        with pytest.raises(RuntimeError):
            simvue_run.save_directory(
                save_files + save_directories,
                category="input",
                contents_filetype=mimetype,
                preserve_path=preserve_path,
            )


@pytest.mark.run
@pytest.mark.parametrize(
    "mimetype",
    ("text/plain", None, "text/text"),
    ids=("valid_mime", "unspecified", "invalid_mime"),
)
@pytest.mark.parametrize(
    "preserve_path", (True, False), ids=("preserve_path", "modified_path")
)
def test_save_all_offline(
    create_plain_run_offline: typing.Tuple[sv_run.Run, dict],
    mimetype: typing.Optional[str],
    preserve_path: bool,
    create_project_directories,
) -> None:
    simvue_run, _ = create_plain_run_offline
    simvue_run.update_tags(["simvue_client_unit_tests", "test_save_all_offline"])
    directories, files = create_project_directories

    # Cherry pick some files and some directories
    save_files = files[len(files) // 2 :]
    save_directories = directories[len(directories) // 2 :]

    if not mimetype or mimetype == "text/plain":
        simvue_run.save_all(
            save_files + save_directories,
            category="input",
            contents_filetype=mimetype,
            preserve_path=preserve_path,
        )
    else:
        with pytest.raises(RuntimeError):
            simvue_run.save_directory(
                save_files + save_directories,
                category="input",
                contents_filetype=mimetype,
                preserve_path=preserve_path,
            )
    assert sv_send.sender()


@pytest.mark.run
def test_create_alerts(create_plain_run: typing.Tuple[sv_run.Run, dict]) -> None:
    pass
