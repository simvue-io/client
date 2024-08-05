import functools
import logging
import os
import typing
import pathlib
import configparser
import contextlib
import warnings

import pydantic
import toml

import simvue.utilities as sv_util
from simvue.config.parameters import (
    CONFIG_FILE_NAMES,
    CONFIG_INI_FILE_NAMES,
    ClientGeneralOptions,
    DefaultRunSpecifications,
    ServerSpecifications,
)

logger = logging.getLogger(__file__)


class SimvueConfiguration(pydantic.BaseModel):
    # Hide values as they contain token and URL
    model_config = pydantic.ConfigDict(hide_input_in_errors=True)
    client: ClientGeneralOptions = ClientGeneralOptions()
    server: ServerSpecifications = pydantic.Field(
        ..., description="Specifications for Simvue server"
    )
    run: DefaultRunSpecifications = DefaultRunSpecifications()

    @classmethod
    def _parse_ini_config(cls, ini_file: pathlib.Path) -> dict[str, dict[str, str]]:
        """Parse a legacy INI config file if found."""
        # NOTE: Legacy INI support, this will be removed
        warnings.warn(
            "Support for legacy INI based configuration files will be dropped in simvue>=1.2, "
            "please switch to TOML based configuration.",
            DeprecationWarning,
            stacklevel=2,
        )

        config_dict: dict[str, dict[str, str]] = {"server": {}}

        with contextlib.suppress(Exception):
            parser = configparser.ConfigParser()
            parser.read(f"{ini_file}")
            if token := parser.get("server", "token"):
                config_dict["server"]["token"] = token
            if url := parser.get("server", "url"):
                config_dict["server"]["url"] = url

        return config_dict

    @classmethod
    def fetch(
        cls,
        server_url: typing.Optional[str] = None,
        server_token: typing.Optional[str] = None,
    ) -> "SimvueConfiguration":
        _config_dict: dict[str, dict[str, str]] = {}

        try:
            logger.info(f"Using config file '{cls.config_file()}'")

            # NOTE: Legacy INI support, this will be removed
            if cls.config_file().suffix == ".toml":
                _config_dict = toml.load(cls.config_file())
            else:
                _config_dict = cls._parse_ini_config(cls.config_file())

        except FileNotFoundError:
            if not server_token or not server_url:
                _config_dict = {"server": {}}
                logger.warning("No config file found, checking environment variables")

        _config_dict["server"] = _config_dict.get("server", {})

        # Ranking of configurations for token and URl is:
        # Envionment Variables > Run Definition > Configuration File

        _server_url = os.environ.get(
            "SIMVUE_URL", server_url or _config_dict["server"].get("url")
        )

        _server_token = os.environ.get(
            "SIMVUE_TOKEN", server_token or _config_dict["server"].get("token")
        )

        if not _server_url:
            raise RuntimeError("No server URL was specified")

        if not _server_token:
            raise RuntimeError("No server token was specified")

        _config_dict["server"]["token"] = _server_token
        _config_dict["server"]["url"] = _server_url

        return SimvueConfiguration(**_config_dict)

    @classmethod
    @functools.lru_cache
    def config_file(cls) -> pathlib.Path:
        _config_file: typing.Optional[pathlib.Path] = (
            sv_util.find_first_instance_of_file(
                CONFIG_FILE_NAMES, check_user_space=True
            )
        )

        # NOTE: Legacy INI support, this will be removed
        if not _config_file:
            _config_file: typing.Optional[pathlib.Path] = (
                sv_util.find_first_instance_of_file(
                    CONFIG_INI_FILE_NAMES, check_user_space=True
                )
            )

        if not _config_file:
            raise FileNotFoundError("Failed to find Simvue configuration file")

        return _config_file
