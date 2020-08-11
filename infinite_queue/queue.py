# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http:# mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from mo_sql import SQL

from infinite_queue.utils import MESSAGES, QUEUE, BLOCKS, _path
from jx_sqlite.sqlite import sql_insert, sql_query, sql_update, quote_value
from jx_sqlite.utils import first_row, rows
from mo_dots import listwrap, Data, wrap
from mo_future import first, text
from mo_json import value2json, json2value
from mo_kwargs import override
from mo_times import Date, Timer
from vendor.mo_logs import Log

DEBUG = True


class Queue:
    @override
    def __init__(self, id, broker, name):
        self.id = id
        self.broker = broker
        self.name = name

    def push(self, message):
        # DETERMINE ULTIMATE LOCATION
        #
        now = Date.now()
        message = wrap(message)

        with self.broker.db.transaction() as t:
            serial = self._next_serial(t)
            key = self._key(path=_path(now), serial=serial)
            message.etl = listwrap(message.etl)
            message.etl.append(
                {
                    "queue": {
                        "url": self.broker.backing.url(key),
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

    add = push

    def flush(self):
        # ANY BLOCKS TO FLUSH?
        with self.broker.db.transaction() as t:
            result = t.query(
                sql_query(
                    {
                        "select": ["block_size_mb", "block_start"],
                        "from": QUEUE,
                        "where": {"eq": {"id": self.id}},
                    }
                )
            )
        self._flush(kwargs=first_row(result))

    @override
    def _flush(self, block_size_mb, block_start):
        with self.broker.db.transaction() as t:
            result = t.query(
                SQL(
                    f"""
                SELECT
                    serial,
                    content
                FROM
                    {MESSAGES}
                WHERE
                    serial >= {quote_value(block_start)} AND
                    queue = {quote_value(self.id)}
                ORDER BY
                    serial
            """
                )
            )

        if not result.data:
            return

        def chunk():
            max_size = block_size_mb * 1024 * 1024
            acc = []
            size = 0
            start = first_row(result).serial
            for r in rows(result):
                s = len(r.content) + 1
                if acc and s + size > max_size:
                    yield acc, start, False
                    acc = []
                    start = r.serial
                acc.append(r.content)
                size += s
            if acc:
                if size > max_size:
                    yield acc, start, False  # EXACT BLOCK SIZE
                else:
                    yield acc, start, True

        for lines, start, is_last in chunk():
            etl_first = json2value(lines[0]).etl.last().queue
            etl_last = json2value(lines[-1]).etl.last().queue
            path = _path(etl_first.timestamp)
            key = self._key(path=path, serial=etl_first.serial)
            Log.note("flush {{num}} lines to {{key}}", key=key, num=len(lines))
            self.broker.backing.write_lines(key, lines)
            result = Data(block_end=etl_last.serial + 1)
            if not is_last:
                # UPDATE start TO MARK MESSAGES FOR DB REMOVAL
                result.block_start = etl_last.serial + 1

            with self.broker.db.transaction() as t:
                t.execute(sql_update(QUEUE, {"set": result}))
                result = t.query(
                    sql_query(
                        {
                            "select": "path",
                            "from": BLOCKS,
                            "where": {"eq": {
                                "queue": self.id,
                                "serial": etl_first.serial,
                            }}
                        }
                    )
                )
                if result.data:
                    t.execute(
                        sql_update(
                            BLOCKS,
                            {
                                "set": {"last_used": Date.now()},
                                "where": {"eq": {"queue": self.id, "serial": etl_first.serial}},
                            },
                        )
                    )
                else:
                    t.execute(
                        sql_insert(
                            BLOCKS,
                            {
                                "queue": self.id,
                                "serial": etl_first.serial,
                                "path": path,
                                "last_used": Date.now(),
                            },
                        )
                    )

    def load(self, path, start):
        key = self._key(path=path, serial=start)
        with self.broker.db.transaction() as t:
            with Timer("load lines from {{key}}", param={"key": key}):
                for line in self.broker.backing.read_lines(key):
                    s = json2value(line).etl.last().queue.serial
                    t.execute(
                        sql_insert(
                            MESSAGES, {"serial": s, "queue": self.id, "content": line}
                        )
                    )

    def _key(self, serial, path):
        return self.name + "/" + path + "/" + text(serial)

    def _next_serial(self, t):
        """
        EXPECTING AN OPEN TRANSACTION t
        """
        result = t.query(
            sql_query(
                {
                    "select": "next_serial",
                    "from": QUEUE,
                    "where": {"eq": {"id": self.id}},
                }
            )
        )
        (next_id,) = first(result.data)

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
