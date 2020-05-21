from mo_dots import listwrap, wrap

from infinite_queue.utils import UNCONFIRMED, SUBSCRIBER, MESSAGES, QUEUE
from jx_sqlite.sqlite import (
    sql_update,
    quote_value,
    sql_insert,
    sql_query)
from jx_sqlite.utils import first_row
from mo_json import json2value
from mo_kwargs import override
from mo_logs import Log
from mo_sql import (
    SQL,
)
from mo_times import Date


class Subscription:
    """
    USE A SUBSCRIPTION TO LISTEN TO A QUEUE
    """

    @override
    def __init__(self, id, queue, confirm_delay_seconds=60):
        self.id = id
        self.queue = queue
        self.confirm_delay_seconds = confirm_delay_seconds

    def pop(self):
        serial, content = self.pop_text()
        if not content:
            return serial, None

        try:
            return serial, json2value(content)
        except Exception as e:
            Log.error("not expected", cause=e)

    def pop_text(self):
        with self.queue.broker.db.transaction() as t:
            # CHECK IF SOME MESSAGES CAN BE RESENT
            result = t.query(
                SQL(
                    f"""
                    SELECT
                        m.serial,
                        m.content
                    FROM
                        {UNCONFIRMED} AS u
                    LEFT JOIN
                        {MESSAGES} AS m ON m.serial=u.serial
                    WHERE
                        m.queue = {quote_value(self.queue.id)} AND
                        u.subscriber = {quote_value(self.id)} AND 
                        u.deliver_time <= {quote_value(Date.now().unix - self.confirm_delay_seconds)}
                    ORDER BY
                        u.deliver_time
                    LIMIT
                        1
                    """
                )
            )


            if result.data:
                record = first_row(result)
                # RECORD IT WAS SENT AGAIN
                now = Date.now()
                t.execute(
                    sql_update(
                        UNCONFIRMED,
                        {
                            "set": {"deliver_time": now},
                            "where": {
                                "eq": {"subscriber": self.id, "serial": record.serial}
                            },
                        },
                    )
                )
                t.execute(
                    sql_update(
                        SUBSCRIBER,
                        {"set": {"last_emit_timestamp": now}}
                    )
                )
                return record.serial, record.content

            # IS THERE A FRESH MESSAGE?
            serial = self._next_serial(t)
            if not serial:
                return 0, None

            result = t.query(sql_query(
                {
                    "select": "content",
                    "from": MESSAGES,
                    "where": {"eq": {"queue": self.queue.id, "serial": serial}},
                }
            ))

            if not result.data:
                # IS IT THE NEXT BLOCK?
                
                # FIND DATE FOR GIVEN SERIAL
                    # WHAT IS CURRENT SERIAl/DATE
                    # WHAT IS SERIAL A WEEK AGO?
                    # EXTRAPOLATE/INTERPOLATE, REPEAT
                Log.error("not handled yet, load block")

            content = first_row(result).content

            # RECORD IT WAS SENT
            now = Date.now()
            t.execute(
                sql_insert(
                    UNCONFIRMED,
                    {
                        "subscriber": self.id,
                        "serial": serial,
                        "deliver_time": now,
                    },
                )
            )
            t.execute(
                sql_update(
                    SUBSCRIBER,
                    {"set": {"last_emit_timestamp": now}}
                )
            )

            return serial, content

    def confirm(self, serial):
        with self.queue.broker.db.transaction() as t:
            t.execute(SQL(f"""
                DELETE FROM {UNCONFIRMED}
                WHERE
                    subscriber = {quote_value(self.id)} AND
                    serial = {quote_value(serial)}
            """))
            t.execute(SQL(f"""
                UPDATE {SUBSCRIBER} SET last_confirmed_serial=COALESCE(
                    (
                    SELECT min(serial) 
                    FROM {UNCONFIRMED} AS u
                    WHERE u.subscriber={quote_value(self.id)}
                    ), 
                    next_emit_serial               
                )-1
            """))

    def _next_serial(self, t):
        """
        EXPECTING OPEN TRANSACTION t
        """

        # TEST IF THERE ARE MESSAGES TO EMIT
        result = t.query(SQL(f"""
            SELECT
                s.next_emit_serial
            FROM
                {SUBSCRIBER} AS s 
            LEFT JOIN
                {QUEUE} AS q ON q.id = s.queue
            WHERE
                s.id = {quote_value(self.id)} AND
                q.next_serial > s.next_emit_serial
        """))

        if not result.data:
            # NO NEW MESSAGES
            return None

        next_id = first_row(result).next_emit_serial
        t.execute(
            sql_update(
                SUBSCRIBER,
                {
                    "set": {"next_emit_serial": next_id + 1},
                    "where": {"eq": {"id": self.id}},
                },
            )
        )
        return next_id
