from notified import server
from notified import config


if __name__ == '__main__':
    server = server.Server(config.EVENT_CHANNEL_NAME, config.CONNECTION_STRING)
