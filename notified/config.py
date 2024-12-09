import dataclasses
import logging
import os
import typing as t

_ENV_PREFIX: t.Final[str] = "NOTIFIED_"

_EVENTS_TABLE_NAME: str = os.getenv(f"{_ENV_PREFIX}EVENTS_TABLE_NAME", "events")
_ID_FIELD_NAME: str = os.getenv(f"{_ENV_PREFIX}ID_FIELD_NAME", "id")

_CHANNEL_SELECT_TIMEOUT_DEFAULT: t.Final[int] = 5
_CHANNEL_SELECT_TIMEOUT = _CHANNEL_SELECT_TIMEOUT_DEFAULT
if (select_timeout := os.getenv(f"{_ENV_PREFIX}CHANNEL_SELECT_TIMEOUT")) is not None:
    try:
        _CHANNEL_SELECT_TIMEOUT = int(select_timeout)
    except ValueError:
        logging.warning(
            f"Invalid value for {_ENV_PREFIX}CHANNEL_SELECT_TIMEOUT: {select_timeout}, "
            f"using default value {_CHANNEL_SELECT_TIMEOUT}"
        )


@dataclasses.dataclass
class NotifiedConfig:
    events_table: str = _EVENTS_TABLE_NAME
    id_field: str = _ID_FIELD_NAME
    channel_select_timeout: int = _CHANNEL_SELECT_TIMEOUT

    @classmethod
    def defaults(cls) -> t.Self:
        return cls()
