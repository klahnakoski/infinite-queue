# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from infinite_queue import schema
from infinite_queue.queue import Queue, DEBUG
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
    sql_insert,
    sql_query,
    id_generator,
    quote_value,
)
from jx_sqlite.utils import first_row, rows
from mo_future import first
from mo_kwargs import override
from mo_sql import ConcatSQL, SQL, JoinSQL, SQL_OR
from mo_threads import Till, Signal, Thread
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
            schema.setup(self)
        self.next_id = id_generator(db=self.db, version_table=VERSION_TABLE)
        self.queues = []
        self.please_stop = Signal()
        self.cleaner = Thread.run("cleaner", self._cleaner)


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
                            "last_emit_timestamp": Date.now(),
                        },
                    )
                )

                output = Queue(id=id, broker=self, kwargs=kwargs)
            else:
                kwargs = first_row(result)
                output = Queue(broker=self, kwargs=kwargs)

        self.queues.append(output)
        return output

    def get_subscriber(self, name=None):
        """
        GET SUBSCRIBER BY id, OR BY QUEUE name
        """
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

    def replay(
        self,
        name,
        confirm_delay_seconds=60,
        next_emit_serial=1,
        look_ahead_serial=1000
    ):
        """
        A SUBSCRIBER FOR REVIEWING THE QUEUE CONTENTS, IN ORDER
        """
        queue = self.get_or_create_queue(name)
        id = self.next_id()

        with self.db.transaction() as t:
            t.execute(
                sql_insert(
                    table=SUBSCRIBER,
                    records={
                        "id": id,
                        "queue": queue.id,
                        "last_emit_timestamp": Date.now(),
                        "confirm_delay_seconds": confirm_delay_seconds,
                        "last_confirmed_serial": next_emit_serial - 1,
                        "next_emit_serial": next_emit_serial,
                        "look_ahead_serial": look_ahead_serial,
                    },
                )
            )

        return Subscription(
            id=id, queue=queue, confirm_delay_seconds=confirm_delay_seconds
        )

    def delete_queue(self, name):
        Log.error("You do not need to do this")

    def add_subscription(self, queue, serial_start=0):
        pass

    def clean(self):
        """
        REMOVE ANY RECORDS THAT ARE NOT NEEDED BY QUEUE OR SUBSCRIBERS
        """
        now = Date.now()

        # ANY BLOCKS TO FLUSH?
        with self.db.transaction() as t:
            result = t.query(
                SQL(
                    f"""
                    SELECT id, block_size_mb, block_start
                    FROM {QUEUE}
                    WHERE block_write < {quote_value(now-Duration(WRITE_INTERVAL))}            
                    """
                )
            )

        for stale in rows(result):
            queue = first(q for q in self.queues if q.id == stale.id)
            queue._flush(**stale)

        # REMOVE UNREACHABLE MESSAGES
        conditions = []
        for q in self.queues:
            conditions.append(
                SQL(f"(queue = {quote_value(q.id)} AND serial IN (")
                + SQL(
                    f"""
                    SELECT m.serial
                    FROM {MESSAGES} AS m 
                    LEFT JOIN {UNCONFIRMED} as u ON u.serial = m.serial
                    LEFT JOIN {SUBSCRIBER} as s  ON s.queue = m.queue and s.id = u.subscriber 
                    LEFT JOIN {QUEUE} as q ON q.id = m.queue and m.serial >= q.block_start
                    LEFT JOIN {SUBSCRIBER} as la ON 
                        la.queue = m.queue AND 
                        la.last_confirmed_serial < m.serial AND 
                        m.serial < la.next_emit_serial+la.look_ahead_serial
                    WHERE 
                        m.queue = {q.id} AND
                        s.id IS NULL AND -- STILL UNCONFIRMED POP
                        q.id IS NULL AND -- NOT WRITTEN TO S3 YET
                        la.id IS NULL    -- NOT IN LOOK-AHEAD FOR SUBSCRIBER           
                    """
                )
                + SQL("))")
            )

        with self.db.transaction() as t:
            if DEBUG:
                result = t.query(
                    ConcatSQL(
                        SQL(f"SELECT count(1) AS `count` FROM {MESSAGES} WHERE "),
                        JoinSQL(SQL_OR, conditions),
                    )
                )
                Log.note(
                    "Delete {{num}} messages from database", num=first_row(result).count
                )

            t.execute(
                ConcatSQL(
                    SQL(f"DELETE FROM {MESSAGES} WHERE "), JoinSQL(SQL_OR, conditions)
                )
            )

    def close(self):
        self.please_stop.go()
        for q in self.queues:
            q.flush()
        self.db.close()

    def _cleaner(self, please_stop):
        while not please_stop:
            (
                please_stop | Till(seconds=Duration(WRITE_INTERVAL).total_seconds())
            ).wait()
            self.clean()
