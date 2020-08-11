# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from infinite_queue.utils import (
    VERSION_TABLE,
    QUEUE,
    SUBSCRIBER,
    MESSAGES,
    UNCONFIRMED,
    BLOCKS)
from jx_sqlite.sqlite import (
    sql_create,
    version_table,
)


def setup(broker):
    version_table(db=broker.db, version_table=VERSION_TABLE)

    with broker.db.transaction() as t:
        t.execute(
            sql_create(
                table=QUEUE,
                properties={
                    "id": "INTEGER PRIMARY KEY NOT NULL",
                    "name": "TEXT NOT NULL",
                    "next_serial": "LONG NOT NULL",
                    "block_size_mb": "LONG NOT NULL",
                    "block_start": "LONG NOT NULL",
                    "block_end": "LONG NOT NULL"
                },
                unique="name",
            )
        )

        t.execute(
            sql_create(
                table=BLOCKS,
                properties={
                    "queue": "INTEGER NOT NULL",
                    "serial": "LONG NOT NULL",
                    "path": "TEXT NOT NULL",
                    "last_used": "DOUBLE NOT NULL",
                },
                primary_key=("queue", "serial"),
                foreign_key={"queue": {"table": QUEUE, "column": "id"}},
            )
        )

        t.execute(
            sql_create(
                table=SUBSCRIBER,
                properties={
                    "id": "INTEGER PRIMARY KEY NOT NULL",
                    "queue": "INTEGER NOT NULL",
                    "confirm_delay_seconds": "LONG NOT NULL",
                    "look_ahead_serial": "LONG NOT NULL",
                    "last_confirmed_serial": "LONG NOT NULL",
                    "next_emit_serial": "LONG NOT NULL",
                    "last_emit_timestamp": "DOUBLE NOT NULL",
                },
                foreign_key={"queue": {"table": QUEUE, "column": "id"}},
            )
        )

        t.execute(
            sql_create(
                table=MESSAGES,
                properties={
                    "queue": "INTEGER NOT NULL",
                    "serial": "LONG NOT NULL",
                    "content": "TEXT",
                },
                primary_key=("queue", "serial"),
                foreign_key={"queue": {"table": QUEUE, "column": "id"}},
            )
        )

        t.execute(
            sql_create(
                table=UNCONFIRMED,
                properties={
                    "subscriber": "LONG NOT NULL",
                    "serial": "LONG NOT NULL",
                    "deliver_time": "DOUBLE NOT NULL",
                },
                foreign_key={
                    "subscriber": {"table": SUBSCRIBER, "column": "id"},
                    "serial": {"table": MESSAGES, "column": "id"},
                },
            )
        )
