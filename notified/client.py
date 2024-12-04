import psycopg2
from nanos.logging import LoggerMixin

from notified.utils import get_connection


class NotifyClient(LoggerMixin):
    def __init__(self, channel: str, connection_string: str) -> None:
        self.channel = channel
        self.connection_string = connection_string
        self._connection: psycopg2.extensions.connection | None = None

    @property
    def connection(self) -> psycopg2.extensions.connection:
        if self._connection is None:
            self._connection = get_connection(self.connection_string)
        return self._connection

    def notify(self, data: str) -> None:
        self.logger.info(f"Sending a message to the '{self.channel}' channel: {data}")
        cursor = self.connection.cursor()
        cursor.execute(f"NOTIFY {self.channel}, %s", (data,))
