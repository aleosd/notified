import select
import threading
import typing as t
from collections import defaultdict

import psycopg2
from nanos.logging import LoggerMixin
from psycopg2 import sql
from psycopg2.extras import DictCursor

from notified import config
from notified.client import NotifyClient
from notified.handlers import HandlerResult
from notified.utils import get_connection

EMPTY_SELECT: tuple[list[t.Any], list[t.Any], list[t.Any]] = ([], [], [])


class Server(LoggerMixin):
    def __init__(self, channel: str, connection_string: str) -> None:
        self.channel = channel
        self.connection_string = connection_string
        self.client = NotifyClient(self.channel, self.connection_string)

        self._connection: psycopg2.extensions.connection | None = None
        self._handlers: dict[
            str, list[t.Callable[[dict[str, t.Any]], HandlerResult]]
        ] = defaultdict(list)
        self.stopped = False

    @property
    def connection(self) -> psycopg2.extensions.connection:
        if self._connection is None:
            self._connection = get_connection(self.connection_string)
        return self._connection

    def register_handler(
        self, event_name: str, handler: t.Callable[[dict[str, t.Any]], HandlerResult]
    ) -> None:
        self._handlers[event_name].append(handler)

    def listen(self) -> None:
        self.logger.info(f"Running event service on channel '{self.channel}'")
        for message in self._run_loop():
            event = self.fetch_event(message)
            self.logger.info(f"Got an event: {event['name']} from message {message}")
            self.handle(event)
        self.logger.info(
            f"Client stopped on channel '{self.channel}', closing server connection."
        )
        self.connection.close()

    def _run_loop(self) -> t.Generator[str, None, None]:
        cursor = self.connection.cursor()
        cursor.execute(f"LISTEN {self.channel}")
        self.logger.info(f"Listening on channel '{self.channel}'")
        while True:
            if self.stopped:
                self.logger.info(f"Stopped on channel '{self.channel}'")
                cursor.execute(f"UNLISTEN {self.channel}")
                self.connection.close()
                self.logger.info(f"Connection on channel '{self.channel}' is closed")
                break
            if self._channel_is_empty(wait_timeout=config.SELECT_TIMEOUT):
                self.logger.debug(f"Nothing to read on channel '{self.channel}'")
                continue
            self.connection.poll()
            while self.connection.notifies:
                message = self.connection.notifies.pop()
                self.logger.info(
                    f"Got a message from the '{message.channel}' channel: {message.payload}"
                )
                yield message.payload

    def _channel_is_empty(self, wait_timeout: int) -> bool:
        return select.select([self.connection], [], [], wait_timeout) == EMPTY_SELECT

    def fetch_event(self, event_id: str) -> dict[str, t.Any]:
        cursor = self.connection.cursor(cursor_factory=DictCursor)
        cursor.execute(self.query, (event_id,))
        if (db_record := cursor.fetchone()) is None:
            raise RuntimeError(f"Event {event_id} not found")
        return dict(db_record)

    def handle(self, event: dict[str, t.Any]) -> None:
        event_name = event["name"]
        if (handlers := self._handlers.get(event_name)) is None:
            self.logger.info(f"No handlers defined for event {event_name}")
            return None
        for handler in handlers:
            self.logger.info(
                f"Scheduling handler {handler.__name__} for event {event_name}"
            )
            threading.Thread(target=handler, args=(event,)).start()
        return None

    @property
    def query(self) -> sql.Composable:
        return sql.SQL("select * from {table} where {pkey} = %s").format(
            table=sql.Identifier(config.EVENTS_TABLE_NAME),
            pkey=sql.Identifier(config.ID_FIELD_NAME),
        )

    def shutdown(self) -> None:
        self.logger.info(f"Shutting down event server on channel '{self.channel}'")
        self.stopped = True
