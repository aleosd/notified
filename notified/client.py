import select
import logging

import psycopg2

SELECT_TIMEOUT = 5
EMPTY_SELECT = ([], [], [])


class NotifyClient:
    def __init__(self, channel: str, connection_string: str) -> None:
        self.channel = channel
        self.connection_string = connection_string
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        self._connection: psycopg2.extensions.connection | None = None

        self.stopped = False

    @property
    def connection(self) -> psycopg2.extensions.connection:
        if self._connection is None:
            connection = psycopg2.connect(self.connection_string)
            connection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
            self._connection = connection
        return self._connection

    def notify(self, data: str) -> None:
        self.logger.info(f"Sending a message to the '{self.channel}' channel: {data}")
        cursor = self.connection.cursor()
        cursor.execute(f"NOTIFY {self.channel}, %s", (data,))

    def listen(self):
        cursor = self.connection.cursor()
        cursor.execute(f"LISTEN {self.channel}")
        self.logger.info(f"Listening on channel '{self.channel}'")
        while True:
            if self.stopped:
                self.logger.info(f"Client marked as stopped on channel '{self.channel}'")
                cursor.execute(f"UNLISTEN {self.channel}")
                self.connection.close()
                self.logger.info(f"Client connection on channel '{self.channel}' is closed")
                break
            if select.select([self.connection], [], [], SELECT_TIMEOUT) == EMPTY_SELECT:
                self.logger.debug(f"Nothing to read on channel '{self.channel}'")
                continue
            self.connection.poll()
            while self.connection.notifies:
                message = self.connection.notifies.pop()
                self.logger.info(
                    f"Got a message from the '{message.channel}' channel: {message.payload}"
                )
                yield message.payload

    def shutdown(self):
        self.logger.info(f"Shutting down event client on channel '{self.channel}'")
        self.stopped = True
