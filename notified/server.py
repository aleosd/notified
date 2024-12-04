from collections import defaultdict
import threading
import typing as t

import psycopg2
from psycopg2 import sql
from psycopg2.extras import DictCursor
from nanos.logging import LoggerMixin

from notified.client import NotifyClient
from notified import config
from notified.handlers import HandleResult
from notified.utils import get_connection


class Server(LoggerMixin):
    def __init__(self, channel: str, connection_string: str) -> None:
        self.channel = channel
        self.connection_string = connection_string
        self.client = NotifyClient(self.channel, self.connection_string)

        self._connection: psycopg2.extensions.connection | None = None
        self._handlers: dict[
            str, list[t.Callable[[dict[str, t.Any]], HandleResult]]
        ] = defaultdict(list)

    @property
    def connection(self) -> psycopg2.extensions.connection:
        if self._connection is None:
            self._connection = get_connection(self.connection_string)
        return self._connection

    def register_handler(
        self, event_name: str, handler: t.Callable[[dict[str, t.Any]], HandleResult]
    ) -> None:
        self._handlers[event_name].append(handler)

    def listen(self):
        self.logger.info(f"Running event service on channel '{self.channel}'")
        for message in self.client.listen():
            event = self.fetch_event(message)
            self.logger.info(f"Got an event: {event['name']} from message {message}")
            self.handle(event)
        self.logger.info(
            f"Client stopped on channel '{self.channel}', closing server connection."
        )
        self.connection.close()

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

    def shutdown(self):
        self.logger.info(f"Shutting down event server on channel '{self.channel}'")
        self.client.shutdown()
