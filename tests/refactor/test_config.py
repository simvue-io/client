import pytest
import uuid
import os.path
import pydantic
import pytest_mock
import tempfile
import simvue.config as sv_config


@pytest.mark.config
@pytest.mark.parametrize(
    "use_env", (True, False),
    ids=("use_env", "no_env")
)
@pytest.mark.parametrize(
    "use_file", (None, "basic", "extended"),
    ids=("no_file", "basic_file", "extended_file")
)
@pytest.mark.parametrize(
    "use_args", (True, False),
    ids=("args", "no_args")
)
def test_config_setup(
    use_env: bool,
    use_file: str | None,
    use_args: bool,
    monkeypatch: pytest.MonkeyPatch,
    mocker: pytest_mock.MockerFixture
) -> None:
    _token: str = f"{uuid.uuid4()}".replace('-', '')
    _other_token: str = f"{uuid.uuid4()}".replace('-', '')
    _arg_token: str = f"{uuid.uuid4()}".replace('-', '')
    _url: str = "https://simvue.example.com/"
    _other_url: str = "http://simvue.example.com/"
    _arg_url: str = "http://simvue.example.io/"
    _description: str = "test case for runs"
    _folder: str = "/test-case"
    _tags: list[str] = ["tag-test", "other-tag"]

    if use_env:
        monkeypatch.setenv("SIMVUE_TOKEN", _other_token)
        monkeypatch.setenv("SIMVUE_URL", _other_url)
    else:
        monkeypatch.delenv("SIMVUE_TOKEN", False)
        monkeypatch.delenv("SIMVUE_URL", False)

    with tempfile.TemporaryDirectory() as temp_d:
        mocker.patch("pathlib.Path.home", lambda: temp_d)
        if use_file:
            with open(_config_file := os.path.join(temp_d, "simvue.toml"), "w") as out_f:
                _lines: str = f"""
[server]
url = "{_url}"
token = "{_token}"
"""
                if use_file == "extended":
                    _lines += f"""
[run]
description = "{_description}"
folder = "{_folder}"
tags = {_tags}
"""
                out_f.write(_lines)
        os.chdir(temp_d)

        if not use_file and not use_env and not use_args:
            with pytest.raises(pydantic.ValidationError):
                sv_config.SimvueConfiguration.fetch()
            return
        elif use_args:
            _config = sv_config.SimvueConfiguration.fetch(
                server_url=_arg_url,
                server_token=_arg_token
            )
        else:
            _config = sv_config.SimvueConfiguration.fetch()

        if use_file:
            assert _config.config_file() == _config_file

        if use_env:
            assert _config.server.url == _other_url
            assert _config.server.token == _other_token
        elif use_args:
            assert _config.server.url == _arg_url
            assert _config.server.token == _arg_token
        elif use_file:
            assert _config.server.url == _url
            assert _config.server.token == _token

        if use_file == "extended":
            assert _config.run.description == _description
            assert _config.run.folder == _folder
            assert _config.run.tags == _tags
        elif use_file:
            assert _config.run.folder == "/"
            assert not _config.run.description
            assert not _config.run.tags