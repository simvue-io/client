"""
Proxy
=====

Selects whether to use offline or online processing depending on configuration.
"""

import typing

if typing.TYPE_CHECKING:
    from .base import SimvueBaseClass
    from simvue.config import SimvueConfiguration

from .offline import Offline
from .remote import Remote


def Simvue(
    name: typing.Optional[str],
    uniq_id: str,
    mode: str,
    config: "SimvueConfiguration",
    suppress_errors: bool = True,
) -> "SimvueBaseClass":
    if mode == "offline":
        return Offline(name=name, uniq_id=uniq_id, suppress_errors=suppress_errors)
    else:
        return Remote(
            name=name, uniq_id=uniq_id, config=config, suppress_errors=suppress_errors
        )
