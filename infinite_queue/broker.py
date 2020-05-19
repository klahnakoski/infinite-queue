# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from infinite_queue.queue import Queue
from infinite_queue.subscription import Subscription
from infinite_queue.utils import (
    VERSION_TABLE,
    QUEUE,
    SUBSCRIBER,
    MESSAGES,
    UNCONFIRMED,
    DirectoryBacking,
)
from jx_sqlite.sqlite import (
    Sqlite,
    sql_create,
    sql_insert,
    sql_query,
    version_table,
    id_generator,
    quote_value,
)
from jx_sqlite.utils import first_row, rows
from mo_future import first
from mo_kwargs import override
from mo_sql import (
    ConcatSQL,
    SQL,
)
from mo_threads import Till
from mo_times import Date, Duration
from pyLibrary.aws import s3
from vendor.mo_logs import Log

WRITE_INTERVAL = "minute"


class Broker:
    @override
    def __init__(self, backing, database, kwargs=None):
        if backing.directory:
            self.backing = DirectoryBacking(kwargs=backing)
        else:
            self.backing = s3.Bucket(kwargs=backing)
        self.db = Sqlite(database)

        # ENSURE DATABASE IS SETUP
        if not self.db.about(VERSION_TABLE):
            self._setup()
        self.next_id = id_generator(db=self.db, version_table=VERSION_TABLE)
        self.queues = []
        # self.cleaner = Thread.run("cleaner", self._cleaner)

    def _setup(self):
        version_table(db=self.db, version_table=VERSION_TABLE)

        with self.db.transaction() as t:
            t.execute(
                sql_create(
                    table=QUEUE,
                    properties={
                        "id": "INTEGER PRIMARY KEY NOT NULL",
                        "name": "TEXT NOT NULL",
                        "next_serial": "LONG NOT NULL",
                        "block_size_mb": "LONG NOT NULL",
                        "block_start": "LONG NOT NULL",
                        "block_end": "LONG NOT NULL",
                        "block_write": "DOUBLE NOT NULL",
                    },
                    unique="name",
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
        for q in self.queues:
            if q.name == name:
                return q

        # VERIFY QUEUE EXISTS
        with self.db.transaction() as t:
            result = t.query(
                sql_query({"from": QUEUE, "where": {"eq": {"name": name}}})
            )

            if not result.data:
                id = self.next_id()
                t.execute(
                    sql_insert(
                        table=QUEUE,
                        records={
                            "id": id,
                            "name": name,
                            "next_serial": 1,
                            "block_size_mb": block_size_mb,
                            "block_start": 1,
                            "block_end": 1,
                            "block_write": Date.now(),
                        },
                    )
                )
                t.execute(
                    sql_insert(
                        table=SUBSCRIBER,
                        records={
                            "id": id,
                            "queue": id,
                            "confirm_delay_seconds": 60,
                            "look_ahead_serial": 1000,
                            "last_confirmed_serial": 0,
                            "next_emit_serial": 1,
                        },
                    )
                )

                output = Queue(id=id, broker=self, kwargs=kwargs)
            else:
                kwargs = first_row(result)
                output = Queue(broker=self, kwargs=kwargs)

        self.queues.append(output)
        return output

    def get_listener(self, name):
        with self.db.transaction() as t:
            result = t.query(
                SQL(
                    f"""
                SELECT
                    MIN(s.id) as id
                FROM
                    {SUBSCRIBER} AS s
                LEFT JOIN 
                    {QUEUE} as q on q.id = s.queue
                WHERE
                    q.name = {quote_value(name)}
                GROUP BY 
                    s.queue
            """
                )
            )
            if not result:
                Log.error("not expected")

            queue = self.get_or_create_queue(name)
            sub_info = t.query(
                sql_query(
                    {"from": SUBSCRIBER, "where": {"eq": {"id": first_row(result).id}}}
                )
            )

            return Subscription(queue=queue, kwargs=first_row(sub_info))

    def delete_queue(self, name):
        Log.error("You do not need to do this")

    def add_subscription(self, queue, serial_start=0):
        pass

    def _push_to_s3(self):
        now = Date.now()

        # ANY BLOCKS TO FLUSH?
        with self.db.transaction() as t:
            result = t.query(
                sql_query(
                    {
                        "select": ["id", "block_size_mb", "block_start"],
                        "from": QUEUE,
                        "where": {
                            "lt": {"block_write": now - Duration(WRITE_INTERVAL)}
                        },
                    }
                )
            )

        for stale in rows(result):
            queue = first(q for q in self.queues if q.id == stale.id)
            queue._flush(**stale)

        # REMOVE UNREACHABLE MESSAGES
        with self.db.transaction() as t:
            t.execute(
                ConcatSQL(
                    SQL(
                        f"""
                    DELETE FROM {MESSAGES}
                    WHERE serial, queue IN (
                        SELECT m.serial, m.queue
                        FROM {MESSAGES} AS m 
                        LEFT JOIN {UNCONFIRMED} as u ON u.serial = m.serial
                        LEFT JOIN {SUBSCRIBER} as s  ON s.queue = m.queue and s.id = u.subscriber 
                        LEFT JOIN {QUEUE} as q ON m.queue=q.id and m.serial >= q.block_start
                        LEFT JOIN {SUBSCRIBER} as la ON 
                            la.queue = m.queue AND 
                            la.last_confirmed_serial < m.serial AND 
                            m.serial < la.next_emit_serial+la.look_ahead_serial
                        WHERE 
                            s.id IS NULL AND -- NOT USED BY ANY SUBSCRIBER
                            q.id IS NULL AND -- NOT WRITTEN TO S3 YET
                            la.id IS NULL    -- NOT USED BY SUBSCRIBER           
                    )
                """
                    )
                )
            )

    def _cleaner(self, please_stop):
        while not please_stop:
            (
                please_stop | Till(seconds=Duration(WRITE_INTERVAL).total_seconds())
            ).wait()
            self._push_to_s3()
