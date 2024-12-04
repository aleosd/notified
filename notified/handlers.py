import logging
import typing as t
import enum
import dataclasses

import httpx
import orjson


DEFAULT_TIMEOUT = 240


class HandleStatus(enum.Enum):
    SUCCESS = enum.auto()
    FAILURE = enum.auto()


@dataclasses.dataclass
class HandleResult:
    status: HandleStatus
    payload: dict[str, t.Any]

    @property
    def success(self) -> bool:
        return self.status == HandleStatus.SUCCESS

    @property
    def failure(self) -> bool:
        return self.status == HandleStatus.FAILURE


class HTTPHandler:
    def __init__(self, url: str, method: str, timeout: int = DEFAULT_TIMEOUT) -> None:
        self.url = url
        self.method = method
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        self.timeout = timeout

    def handle(self, payload: dict[str, t.Any], timeout: int | None = None) -> HandleResult:
        timeout = timeout or self.timeout
        self.logger.debug(f"Handling an event: {payload}")
        response = httpx.request(self.method, self.url, content=orjson.dumps(payload), timeout=timeout)
        if not response.is_success:
            self.logger.warning(f"Failed to send an event: {response.status_code} {response.text}")
            return HandleResult(status=HandleStatus.FAILURE, payload={"status_code": response.status_code, "text": response.text})
        return HandleResult(status=HandleStatus.SUCCESS, payload=response.json())
