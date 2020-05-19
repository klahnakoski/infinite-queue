# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from infinite_queue.utils import MESSAGES, QUEUE
from jx_sqlite.sqlite import sql_insert, sql_query, sql_update
from mo_dots import listwrap, Data
from mo_future import first, text
from mo_json import value2json
from mo_kwargs import override
from mo_times import MINUTE, Date
from vendor.mo_logs import Log


class Queue:
    @override
    def __init__(self, id, broker, name, bucket, confirm_delay=MINUTE):
        self.id = id
        self.broker = broker
        self.name = name
        self.bucket = bucket
        self.confirm_delay = confirm_delay

    def push(self, message):
        # DETERMINE ULTIMATE LOCATION
        #
        now = Date.now()

        with self.broker.db.transaction() as t:
            serial = self._next_serial(t)
            message.etl = listwrap(message.etl)
            message.etl.add(
                {
                    "queue": {
                        "url": self._bucket_url(),
                        "timestamp": now,
                        "date/time": now.format(),
                        "serial": serial,
                    }
                }
            )
            content = value2json(message, sort_keys=True)
            t.execute(
                sql_insert(
                    MESSAGES, {"queue": self.id, "serial": serial, "content": content}
                )
            )
        return serial

    def flush(self):
        # ANY BLOCKS TO FLUSH?
        with self.broker.db.transaction() as t:
            result = t.query(
                sql_query(
                    {
                        "select": ["block_size_mb", "block_start"],
                        "from": QUEUE,
                        "where": {"and": [{"eq": {"id": self.id}}]},
                    }
                )
            )
        self._flush(kwargs=result.data[0])

    @override
    def _flush(self, block_size_mb, block_start):
        if id != self.id:
            Log.error("logic error")

        with self.broker.db.transaction() as t:
            result = t.query(
                sql_query(
                    {
                        "select": ["serial", "content"],
                        "from": MESSAGES,
                        "where": {
                            "and": [
                                {"gte": {"serial": block_start}},
                                {"eq": {"queue": self.id}},
                            ]
                        },
                        "sort": "serial",
                    }
                )
            )

        if not result.data:
            return

        def chunk():
            acc = []
            size = 0
            start = result.data[0].serial
            for r in result.data:
                s = len(r.content) + 1
                if acc and s + size > block_size_mb:
                    yield acc, start, False
                    acc = []
                    start = r.serial
                acc.append(r.content)
                size += s
            if acc:
                yield acc, start, True

        for lines, start, is_last in chunk():
            etl_first = lines[0].etl[0].queue
            etl_last = lines[-1].etl[0].queue
            key = self._key(kwargs=etl_first)
            self.bucket.write_lines(self, key, lines)
            result = Data(block_end=etl_last.serial + 1, block_write=Date.now())
            if not is_last:
                # UPDATE start TO MARK MESSAGES FOR DB REMOVAL
                result.block_start = etl_first.serial

            with self.broker.db.transaction() as t:
                t.execute(sql_update(QUEUE, {"set": result}))

    @override
    def _key(self, timestamp, serial):
        if not timestamp or not serial:
            Log.error("expecting parameters")
        return Date(timestamp).format("%Y/%m/%d") + "/" + text(serial)

    def _bucket_url(self):
        Log.warning("no url yet")
        return None

    def _next_serial(self, t):
        """
        EXPECTING AN OPEN TRANSACTION t
        """
        next_id = first(
            t.query(
                sql_query(
                    {
                        "select": "next_serial",
                        "from": QUEUE,
                        "where": {"eq": {"id": self.id}},
                    }
                )
            ).data
        ).next_serial

        t.execute(
            sql_update(
                QUEUE,
                {
                    "set": {"next_serial": next_id + 1},
                    "where": {"eq": {"id": self.id}},
                },
            )
        )
        return next_id
