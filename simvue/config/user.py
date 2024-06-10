import functools
import logging
import os
import typing

import pydantic
import toml

import simvue.utilities as sv_util
from simvue.config.parameters import (
    CONFIG_FILE_NAMES,
    ClientGeneralOptions,
    DefaultRunSpecifications,
    ServerSpecifications,
)

logger = logging.getLogger(__file__)


class SimvueConfiguration(pydantic.BaseModel):
    client: ClientGeneralOptions = ClientGeneralOptions()
    server: ServerSpecifications = pydantic.Field(
        ..., description="Specifications for Simvue server"
    )
    run: DefaultRunSpecifications = DefaultRunSpecifications()

    @classmethod
    def fetch(
        cls,
        server_url: typing.Optional[str] = None,
        server_token: typing.Optional[str] = None,
    ) -> "SimvueConfiguration":
        _config_dict: dict[str, dict[str, str]] = {}

        try:
            logger.info(f"Using config file '{cls.config_file()}'")
            _config_dict = toml.load(cls.config_file())
        except FileNotFoundError:
            if not server_token or not server_url:
                _config_dict = {"server": {}}
                logger.warning("No config file found, checking environment variables")

        _config_dict["server"] = _config_dict.get("server", {})

        _server_url = os.environ.get("SIMVUE_URL", _config_dict["server"].get("url"))

        if not _server_url:
            raise RuntimeError("No server URL was specified")

        _config_dict["server"]["url"] = _server_url

        _server_token = os.environ.get(
            "SIMVUE_TOKEN", _config_dict["server"].get("token")
        )

        if not _server_token:
            raise RuntimeError("No server token was specified")

        _config_dict["server"]["token"] = _server_token

        return SimvueConfiguration(**_config_dict)

    @classmethod
    @functools.lru_cache
    def config_file(cls) -> str:
        _config_file: typing.Optional[str] = sv_util.find_first_instance_of_file(
            CONFIG_FILE_NAMES, check_user_space=True
        )
        if not _config_file:
            raise FileNotFoundError("Failed to find Simvue configuration file")
        else:
            logger.debug(f"Using config file '{_config_file}'")
        return _config_file
