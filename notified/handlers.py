import dataclasses
import enum
import json
import sys
import typing as t
import urllib.request
from http import HTTPStatus

from nanos.logging import LoggerMixin

DEFAULT_TIMEOUT = 240


class HandlerStatus(enum.Enum):
    SUCCESS = enum.auto()
    FAILURE = enum.auto()


@dataclasses.dataclass
class HandlerResult:
    status: HandlerStatus
    payload: dict[str, t.Any]

    @property
    def success(self) -> bool:
        return self.status == HandlerStatus.SUCCESS

    @property
    def failure(self) -> bool:
        return self.status == HandlerStatus.FAILURE


class ConsoleHandler(LoggerMixin):
    def handle(self, payload: dict[str, t.Any]) -> HandlerResult:
        self.logger.info(f"Handling an event: {payload}")
        sys.stdout.write(f"{payload}\n")
        return HandlerResult(status=HandlerStatus.SUCCESS, payload=payload)


class HTTPHandler(LoggerMixin):
    def __init__(self, url: str, method: str, timeout: int = DEFAULT_TIMEOUT) -> None:
        self.url = url
        self.method = method
        self.timeout = timeout

    @staticmethod
    def encode_payload(payload: dict[str, t.Any]) -> bytes:
        return json.dumps(payload).encode("utf-8")

    def handle(self, payload: dict[str, t.Any]) -> HandlerResult:
        self.logger.debug(f"Handling an event: {payload}")
        request = urllib.request.Request(
            self.url, data=self.encode_payload(payload), method=self.method
        )
        request.add_header("Content-Type", "application/json")
        with urllib.request.urlopen(request, timeout=self.timeout) as response:
            if response.status != HTTPStatus.OK:
                self.logger.warning(
                    f"Failed to send an event: {response.status_code} "
                    f"{response.text}"
                )
                return HandlerResult(
                    status=HandlerStatus.FAILURE,
                    payload={
                        "status_code": response.code,
                        "text": response.read().decode("utf-8"),
                    },
                )
            payload = json.loads(response.read().decode("utf-8"))
            return HandlerResult(status=HandlerStatus.SUCCESS, payload=payload)
