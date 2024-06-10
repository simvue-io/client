import logging
import pydantic
import typing

import simvue.models as sv_models

CONFIG_FILE_NAMES: list[str] = [
    "simvue.toml",
    ".simvue.toml"
]

logger = logging.getLogger(__file__)

class ServerSpecifications(pydantic.BaseModel):
    url: pydantic.AnyHttpUrl
    token: str

    @pydantic.field_validator("url")
    @classmethod
    def url_to_str(cls, v: typing.Any) -> str:
        return f"{v}"


class DefaultRunSpecifications(pydantic.BaseModel):
    description: typing.Optional[str]=None
    tags: list[str] | None=None
    folder: str = pydantic.Field(
        "/",
        pattern=sv_models.FOLDER_REGEX
    )


class ClientGeneralOptions(pydantic.BaseModel):
    debug: bool = False