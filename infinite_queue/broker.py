# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from pyLibrary.aws import s3

from infinite_queue.queue import Queue
from infinite_queue.utils import VERSION_TABLE, QUEUE, SUBSCRIBER, MESSAGES, UNCONFIRMED
from jx_sqlite.sqlite import (
    Sqlite,
    sql_create,
    sql_insert,
    sql_query,
    version_table,
    id_generator,
    sql_alias,
    quote_column,
)
from mo_files import File
from mo_future import first
from mo_kwargs import override
from mo_sql import (
    ConcatSQL,
    SQL_SELECT,
    SQL,
    SQL_FROM,
    SQL_LEFT_JOIN,
    JoinSQL,
    SQL_AND,
    SQL_DELETE,
    SQL_WHERE,
    SQL_IN,
)
from mo_threads import Till, Thread
from mo_times import Date, Duration
from vendor.mo_logs import Log

WRITE_INTERVAL = "minute"


class Broker:
    @override
    def __init__(self, backing, database, kwargs=None):
        if backing.directory:
            self.backing = DirectoryBacking(backing)
        else:
            self.backing = s3.Bucket(backing)
        self.db = Sqlite(database)

        # ENSURE DATABASE IS SETUP
        if not self.db.about(VERSION_TABLE):
            self._setup()
        self.next_id = id_generator(db=self.db, version_table=VERSION_TABLE)
        self.queues = []
        self.cleaner = Thread.run("cleaner", self._cleaner)

    def _setup(self):
        version_table(db=self.db, version_table=VERSION_TABLE)

        with self.db.transaction() as t:
            t.execute(
                sql_create(
                    table=QUEUE,
                    properties={
                        "id": "INTEGER PRIMARY KEY NOT NULL",
                        "name": "TEXT NOT NULL",
                        "bucket": "TEXT NOT NULL",
                        "next_serial": "LONG NOT NULL",
                        "block_size_mb": "LONG NOT NULL",
                        "block_start": "LONG NOT NULL",
                        "block_end": "LONG NOT NULL",
                        "block_write": "DOUBLE NOT NULL",
                    },
                    primary_key="id",
                    unique="name",
                )
            )

            t.execute(
                sql_create(
                    table=SUBSCRIBER,
                    properties={
                        "id": "INTEGER PRIMARY KEY NOT NULL",
                        "queue": "INTEGER NOT NULL",
                        "last_confirmed_serial": "LONG NOT NULL",
                        "next_emit_serial": "LONG NOT NULL",
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

    @override
    def get_or_create_queue(self, name, block_size_mb=8, kwargs=None):
        # VERIFY QUEUE EXISTS
        with self.db.transaction() as t:
            result = t.query(
                sql_query({"from": QUEUE, "where": {"eq": {"name": name}}})
            )

            if not result.data:
                t.execute(
                    sql_insert(
                        table=QUEUE,
                        records={**kwargs, "next_serial": 1, "id": self.next_id(),},
                    )
                )
            else:
                kwargs = first(result.data)

        output = Queue(broker=self, kwargs=kwargs)
        self.queues.append(output)
        return output

    def delete_queue(self, name):
        Log.error("You do not need to do this")

    def add_subscription(self, queue, serial_start=0):
        pass

    def _push_to_s3(self):
        now = Date.now()

        # ANY BLOCKS TO FLUSH?
        with self.db.transaction() as t:
            result = t.query(
                sql_query({
                    "select": [
                        "id",
                        "block_size_mb",
                        "block_start"
                    ],
                    "from": QUEUE,
                    "where": {"lt": {"block_write": now - Duration(WRITE_INTERVAL)}}
                })
            )

        for stale in result.data:
            queue = first(q for q in self.queues if q.id == stale.id)
            queue._flush(**stale)

        # REMOVE UNREACHABLE MESSAGES
        with self.db.transaction() as t:
            t.execute(
                ConcatSQL(
                    SQL_DELETE,
                    SQL_FROM,
                    quote_column(MESSAGES),
                    SQL_WHERE,
                    quote_column("id"),
                    SQL_IN,
                    ConcatSQL(
                        SQL_SELECT,
                        quote_column("m", "id"),
                        SQL_FROM,
                        sql_alias(MESSAGES, "m"),
                        SQL_LEFT_JOIN,
                        sql_alias(UNCONFIRMED, "u"),
                        SQL(" ON u.serial = m.serial"),
                        SQL_LEFT_JOIN,
                        sql_alias(SUBSCRIBER, "s"),
                        SQL(" ON u.subscriber = s.id AND m.queue=s.queue"),
                        SQL_LEFT_JOIN,
                        sql_alias(QUEUE, "q"),
                        SQL(" ON m.queue=q.id and m.serial >= q.block_start"),
                        SQL_WHERE,
                        JoinSQL(
                            SQL_AND,
                            (
                                "s.id IS NULL",  # NOT USED BY ANY SUBSCRIBER
                                "q.id IS NULL",  # NOT WRITTEN TO S3 YET
                            ),
                        ),
                    ),
                )
            )

    def _cleaner(self, please_stop):
        while not please_stop:
            (please_stop | Till(seconds=Duration(WRITE_INTERVAL).total_seconds())).wait()
            self._push_to_s3()


class DirectoryBacking:
    @override
    def __init__(self, directory):
        self.dir = File(directory)


    def write_lines(self, key, lines):
        self.dir/key.write_lines(lines)

