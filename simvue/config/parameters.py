import logging
import time
import pydantic
import typing
import pathlib

import simvue.models as sv_models
from simvue.utilities import get_expiry

CONFIG_FILE_NAMES: list[str] = ["simvue.toml", ".simvue.toml"]

CONFIG_INI_FILE_NAMES: list[str] = [
    f'{pathlib.Path.cwd().joinpath("simvue.ini")}',
    f'{pathlib.Path.home().joinpath(".simvue.ini")}',
]

logger = logging.getLogger(__file__)


class ServerSpecifications(pydantic.BaseModel):
    url: pydantic.AnyHttpUrl
    token: pydantic.SecretStr

    @pydantic.field_validator("url")
    @classmethod
    def url_to_str(cls, v: typing.Any) -> str:
        return f"{v}"

    @pydantic.field_validator("token")
    def check_token(cls, v: pydantic.SecretStr) -> str:
        if not (expiry := get_expiry(v.get_secret_value())):
            raise AssertionError("Failed to parse Simvue token - invalid token form")
        if time.time() - expiry > 0:
            raise AssertionError("Simvue token has expired")
        return v.get_secret_value()


class DefaultRunSpecifications(pydantic.BaseModel):
    description: typing.Optional[str] = None
    tags: list[str] | None = None
    folder: str = pydantic.Field("/", pattern=sv_models.FOLDER_REGEX)


class ClientGeneralOptions(pydantic.BaseModel):
    debug: bool = False
