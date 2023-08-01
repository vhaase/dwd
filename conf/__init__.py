"""dwd config classes."""

from dataclasses import dataclass

from common.conf import CommonConfig


@dataclass
class Config(CommonConfig):
    """Config class for dwd."""

    raise NotImplementedError
