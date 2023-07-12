import typing

import pydantic
import pydantic_settings

from .log import get_logger

logger = get_logger()

byte_sizes = {
    'kb': 1_000,
    'mb': 1_000**2,
    'gb': 1_000**3,
    'kib': 1_024,
    'mib': 1_024**2,
    'gib': 1_024**3,
    'b': 1,
    'k': 1_000,
    'm': 1_000**2,
    'g': 1_000**3,
    'ki': 1024,
    'mi': 1_024**2,
    'gi': 1_024**3,
}


def format_bytes(num: int) -> str:
    """Format bytes as a human readable string"""
    return next(
        (
            f"{num / value:.2f} {prefix}B"
            for prefix, value in (
                ("Gi", 2**30),
                ("Mi", 2**20),
                ("ki", 2**10),
            )
            if num >= value * 0.9
        ),
        f"{num} B",
    )


class Settings(pydantic_settings.BaseSettings):
    zarr_proxy_payload_size_limit: int = '2 mb'

    @pydantic.field_validator('zarr_proxy_payload_size_limit', mode='before')
    def _validate_zarr_proxy_payload_size_limit(cls, value: typing.Union[int, str]) -> int:
        if isinstance(value, int):
            return value
        if isinstance(value, str):
            # convert from human readable format to bytes
            value = value.lower()
            for key in byte_sizes.keys():
                if value.endswith(key):
                    return int(value[: -len(key)]) * byte_sizes[key]
            raise ValueError(
                f"Invalid zarr_proxy_payload_size_limit: {value}. Must be an integer or a string with a unit (e.g. '1GB') and valid units are: {', '.join(byte_sizes.keys())}"
            )


def get_settings() -> Settings:
    logger.info("Loading settings from environment variables")
    return Settings()
