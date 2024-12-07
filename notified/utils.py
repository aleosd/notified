import psycopg2


def get_connection(connection_string: str) -> psycopg2.extensions.connection:
    connection = psycopg2.connect(connection_string)
    connection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    return connection
