# Notified 

**Notified** is highly opinionated python library that provides a pub/sub-like
functionality. It is designed to handle event notifications using PostgreSQL
`notify`/`listen` events system.

- [Installation](#installation)
- [Setup](#setup)
- [Usage](#usage)
- [Configuration](#configuration)

## Installation

```bash
pip install notified
```

## Setup

Currently, for the service to work, the following setup is expected:

1. A PostgreSQL database is running.
2. Events table exists in the database and contains event id and name 
information. The id field is used to fetch event data, name is needed to
determine which handlers to call.
3. Handlers are to be implemented by developers. There are basic examples
included in the `notified/handlers` module.
4. To avoid sending big amounts of data over the channel, it is required to
store event in a table and only include id of the record as event payload.

## Usage

Server is the "manager" component: it listens for events and dispatches them to
handlers. Each handler is executed in a separate thread. The following conde
snipped shows how to run the server:

```python
import typing as t

from notified.server import Server
from notified.handlers import HandlerResult, HandlerStatus


def my_handler(event_data: dict[str, t.Any]) -> HandlerResult:
    return HandlerResult(status=HandlerStatus.SUCCESS, payload=event_data)


connection_string = "postgresql://username:password@localhost:5432/database"
listen_channel = "events"

server = Server(listen_channel, connection_string)
server.register_handler("my_event", my_handler)
server.listen()
```

Client is the "publisher" component: it sends events to the server. The following
code snippet shows how to send an event:

```python
from notified.client import NotifyClient


connection_string = "postgresql://username:password@localhost:5432/database"
listen_channel = "events"

event_id = 42  # assuming that there's an event record in a table with id 42

client = NotifyClient(listen_channel, connection_string)
client.notify(str(event_id))
```

Alternatively, instead of explicitly defining client, the DB-level triggers
can be used to send events every time new event is added to the table. The
following is code snippet shows how to create a trigger, assuming that table
with events is named `events`:

```sql
CREATE or REPLACE FUNCTION notify_event()
    RETURNS TRIGGER
    LANGUAGE 'plpgsql'
AS $BODY$
declare
begin
    if (TG_OP = 'INSERT') then
        PERFORM pg_notify('events', NEW.id::text);
    end if;
    return null;
end
$BODY$;
    
CREATE OR REPLACE TRIGGER notify_event
    AFTER INSERT
    ON events
    FOR EACH ROW
    EXECUTE PROCEDURE notify_event();
```

The "subscriber" (or "consumer") part of the workflow is expected to be
implemented by developers using the library.

## Configuration

There are following configuration options:

- Events table name (default: `events`, environment variable:
  `NOTIFIED_EVENTS_TABLE_NAME`)
- ID field name in the events table (default: `id`, environment variable:
  `NOTIFIED_ID_FIELD_NAME`)
- Events channel polling timeout (default: 5 second, environment variable:
  `NOTIFIED_CHANNEL_SELECT_TIMEOUT`)

Each value can be configured either explicitly via code or via environment
variable. For explicit configuration, see the following example:

```python
from notified.config import NotifiedConfig
from notified.server import Server

config = NotifiedConfig(
    events_table="my_events", 
    id_field="pk",
    channel_select_timeout=10
)


server = Server(
    "events_channel",
    "postgresql://username:password@localhost:5432/database",
    conf=config
)
server.listen()
```

## License

This project is licensed under the Apache License.
