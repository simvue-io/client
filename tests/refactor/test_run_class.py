import pytest
import pytest_mock
import time
import typing
import contextlib
import inspect
import tempfile
import threading
import uuid
import psutil
import pathlib
import concurrent.futures
import random

import simvue.run as sv_run
import simvue.client as sv_cl

if typing.TYPE_CHECKING:
    from .conftest import CountingLogHandler


@pytest.mark.run
def test_check_run_initialised_decorator() -> None:
    with sv_run.Run(mode="offline") as run:
        for method_name, method in inspect.getmembers(run, inspect.ismethod):
            if not method.__name__.endswith("init_locked"):
                continue
            with pytest.raises(RuntimeError) as e:
                getattr(run, method_name)()
            assert "Simvue Run must be initialised" in str(e.value)


@pytest.mark.run
@pytest.mark.parametrize("overload_buffer", (True, False), ids=("overload", "normal"))
@pytest.mark.parametrize(
    "visibility", ("bad_option", "tenant", "public", ["ciuser01"], None)
)
def test_log_metrics(
    overload_buffer: bool,
    setup_logging: "CountingLogHandler",
    mocker,
    request: pytest.FixtureRequest,
    visibility: typing.Union[typing.Literal["public", "tenant"], list[str], None],
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
                tags=[
                    "simvue_client_unit_tests",
                    request.node.name.replace("[", "_").replace("]", "_"),
                ],
                folder="/simvue_unit_testing",
                retention_period="1 hour",
                visibility=visibility,
            )
            run.config(resources_metrics_interval=1)
        return

    run.init(
        name=f"test_run_{str(uuid.uuid4()).split('-', 1)[0]}",
        tags=[
            "simvue_client_unit_tests",
            request.node.name.replace("[", "_").replace("]", "_"),
        ],
        folder="/simvue_unit_testing",
        visibility=visibility,
        retention_period="1 hour",
    )
    run.config(resources_metrics_interval=1)

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
def test_log_metrics_offline(create_test_run_offline: tuple[sv_run.Run, dict]) -> None:
    METRICS = {"a": 10, "b": 1.2, "c": 2}
    run, _ = create_test_run_offline
    run.log_metrics(METRICS)


@pytest.mark.run
def test_log_events(create_test_run: tuple[sv_run.Run, dict]) -> None:
    EVENT_MSG = "Hello world!"
    run, _ = create_test_run
    run.log_event(EVENT_MSG)


@pytest.mark.run
def test_log_events_offline(create_test_run_offline: tuple[sv_run.Run, dict]) -> None:
    EVENT_MSG = "Hello world!"
    run, _ = create_test_run_offline
    run.log_event(EVENT_MSG)


@pytest.mark.run
def test_update_metadata(create_test_run: tuple[sv_run.Run, dict]) -> None:
    METADATA = {"a": 10, "b": 1.2, "c": "word"}
    run, _ = create_test_run
    run.update_metadata(METADATA)


@pytest.mark.run
def test_update_metadata_offline(
    create_test_run_offline: tuple[sv_run.Run, dict],
) -> None:
    METADATA = {"a": 10, "b": 1.2, "c": "word"}
    run, _ = create_test_run_offline
    run.update_metadata(METADATA)


@pytest.mark.run
@pytest.mark.parametrize("multi_threaded", (True, False), ids=("multi", "single"))
def test_runs_multiple_parallel(
    multi_threaded: bool, request: pytest.FixtureRequest
) -> None:
    N_RUNS: int = 2
    if multi_threaded:

        def thread_func(index: int) -> tuple[int, list[dict[str, typing.Any]], str]:
            with sv_run.Run() as run:
                run.config(suppress_errors=False)
                run.init(
                    name=f"test_runs_multiple_{index + 1}",
                    tags=[
                        "simvue_client_unit_tests",
                        request.node.name.replace("[", "_").replace("]", "_"),
                    ],
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
                    tags=[
                        "simvue_client_unit_tests",
                        request.node.name.replace("[", "_").replace("]", "_"),
                    ],
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
def test_runs_multiple_series(request: pytest.FixtureRequest) -> None:
    N_RUNS: int = 2

    metrics = []
    run_ids = []

    for index in range(N_RUNS):
        with sv_run.Run() as run:
            run_metrics = []
            run.config(suppress_errors=False)
            run.init(
                name=f"test_runs_multiple_series_{index}",
                tags=[
                    "simvue_client_unit_tests",
                    request.node.name.replace("[", "_").replace("]", "_"),
                ],
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
    setup_logging: "CountingLogHandler", post_init: bool, request: pytest.FixtureRequest
) -> None:
    setup_logging.captures = ["Skipping call to"]

    with sv_run.Run(mode="offline") as run:
        decorated_funcs = [
            name
            for name, method in inspect.getmembers(run, inspect.ismethod)
            if hasattr(method, "__fail_safe")
        ]

        if post_init:
            decorated_funcs.remove("init")
            run.init(
                name="test_suppressed_errors",
                folder="/simvue_unit_testing",
                tags=[
                    "simvue_client_unit_tests",
                    request.node.name.replace("[", "_").replace("]", "_"),
                ],
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


def test_set_folder_details(request: pytest.FixtureRequest) -> None:
    with sv_run.Run() as run:
        folder_name: str = "/simvue_unit_tests"
        description: str = "test description"
        tags: list[str] = [
            "simvue_client_unit_tests",
            request.node.name.replace("[", "_").replace("]", "_"),
        ]
        run.init(folder=folder_name)
        run.set_folder_details(path=folder_name, tags=tags, description=description)

    client = sv_cl.Client()
    assert (folder := client.get_folders([f"path == {folder_name}"])[0])["tags"] == tags
    assert folder["description"] == description


@pytest.mark.run
@pytest.mark.parametrize(
    "valid_mimetype", (True, False), ids=("valid_mime", "invalid_mime")
)
@pytest.mark.parametrize(
    "preserve_path", (True, False), ids=("preserve_path", "modified_path")
)
@pytest.mark.parametrize("name", ("test_file", None), ids=("named", "nameless"))
@pytest.mark.parametrize("allow_pickle", (True, False), ids=("pickled", "unpickled"))
@pytest.mark.parametrize("empty_file", (True, False), ids=("empty", "content"))
def test_save_file(
    create_plain_run: typing.Tuple[sv_run.Run, dict],
    valid_mimetype: bool,
    preserve_path: bool,
    name: typing.Optional[str],
    allow_pickle: bool,
    empty_file: bool,
    capfd,
) -> None:
    simvue_run, _ = create_plain_run
    file_type: str = "text/plain" if valid_mimetype else "text/text"
    with tempfile.TemporaryDirectory() as tempd:
        with open(
            (out_name := pathlib.Path(tempd).joinpath("test_file.txt")),
            "w",
        ) as out_f:
            out_f.write("test data entry" if not empty_file else "")

        if valid_mimetype:
            simvue_run.save_file(
                out_name,
                category="input",
                filetype=file_type,
                preserve_path=preserve_path,
                name=name,
            )
        else:
            with pytest.raises(RuntimeError):
                simvue_run.save_file(
                    out_name,
                    category="input",
                    filetype=file_type,
                    preserve_path=preserve_path,
                )
            return

        variable = capfd.readouterr()
        with capfd.disabled():
            if empty_file:
                assert (
                    variable.out
                    == "[simvue] WARNING: saving zero-sized files not currently supported\n"
                )


@pytest.mark.run
def test_update_tags(
    create_plain_run: typing.Tuple[sv_run.Run, dict],
    request: pytest.FixtureRequest,
) -> None:
    simvue_run, _ = create_plain_run

    tags = [
        "simvue_client_unit_tests",
        request.node.name.replace("[", "_").replace("]", "_"),
    ]

    simvue_run.set_tags(tags)

    time.sleep(1)
    client = sv_cl.Client()
    run_data = client.get_run(simvue_run._id)
    assert run_data["tags"] == tags

    simvue_run.update_tags(["additional"])

    time.sleep(1)
    run_data = client.get_run(simvue_run._id)
    assert sorted(run_data["tags"]) == sorted(tags + ["additional"])


@pytest.mark.run
@pytest.mark.parametrize("object_type", ("DataFrame", "ndarray"))
def test_save_object(
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
def test_abort_on_alert_process(create_plain_run: typing.Tuple[sv_run.Run, dict], mocker: pytest_mock.MockerFixture) -> None:
    def testing_exit(status: int) -> None:
        raise SystemExit(status)
    mocker.patch("os._exit", testing_exit)
    N_PROCESSES: int = 3
    run, _ = create_plain_run
    run.config(resources_metrics_interval=1)
    run._heartbeat_interval = 1
    run._testing = True
    run.add_process(identifier="forever_long", executable="bash", c="&".join(["sleep 10"] * N_PROCESSES))
    process_id = list(run._executor._processes.values())[0].pid
    process = psutil.Process(process_id)
    assert len(child_processes := process.children(recursive=True)) == 3
    time.sleep(2)
    client = sv_cl.Client()
    client.abort_run(run._id, reason="testing abort")
    time.sleep(4)
    assert run._resources_metrics_interval == 1
    for child in child_processes:
        assert not child.is_running()
    if not run._status == "terminated":
        run.kill_all_processes()
        raise AssertionError("Run was not terminated")
    

@pytest.mark.run
def test_abort_on_alert_python(create_plain_run: typing.Tuple[sv_run.Run, dict], mocker: pytest_mock.MockerFixture) -> None:
    abort_set = threading.Event()
    def testing_exit(status: int) -> None:
        abort_set.set()
        raise SystemExit(status)
    mocker.patch("os._exit", testing_exit)
    run, _ = create_plain_run
    run.config(resources_metrics_interval=1)
    run._heartbeat_interval = 1
    client = sv_cl.Client()
    i = 0

    while True:
        time.sleep(1)
        if i == 4:
            client.abort_run(run._id, reason="testing abort")
        i += 1
        if abort_set.is_set() or i > 9:
            break

    assert i < 7
    assert run._status == "terminated"


@pytest.mark.run
def test_abort_on_alert_raise(create_plain_run: typing.Tuple[sv_run.Run, dict], mocker: pytest_mock.MockerFixture) -> None:
    def testing_exit(status: int) -> None:
        raise SystemExit(status)
    mocker.patch("os._exit", testing_exit)
    run, _ = create_plain_run
    run.config(resources_metrics_interval=1)
    run._heartbeat_interval = 1
    run._testing = True
    alert_id = run.create_alert("abort_test", source="user", trigger_abort=True)
    run.add_process(identifier="forever_long", executable="bash", c="sleep 10")
    time.sleep(2)
    run.log_alert(alert_id, "critical")
    time.sleep(4)
    if not run._status == "terminated":
        run.kill_all_processes()
        raise AssertionError("Run was not terminated")


@pytest.mark.run
def test_kill_all_processes(create_plain_run: typing.Tuple[sv_run.Run, dict]) -> None:
    run, _ = create_plain_run
    run.config(resources_metrics_interval=1)
    run.add_process(identifier="forever_long_1", executable="bash", c="sleep 10000")
    run.add_process(identifier="forever_long_2", executable="bash", c="sleep 10000")
    processes = [
        psutil.Process(process.pid)
        for process in run._executor._processes.values()
    ]
    time.sleep(2)
    run.kill_all_processes()
    time.sleep(4)
    for process in processes:
        assert not process.is_running()
        assert all(not child.is_running() for child in process.children(recursive=True))
