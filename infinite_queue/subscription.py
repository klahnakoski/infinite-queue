from infinite_queue.utils import UNCONFIRMED, SUBSCRIBER, MESSAGES, QUEUE
from jx_sqlite.sqlite import sql_update, quote_column, sql_alias, quote_value, sql_insert
from mo_future import first
from mo_logs import Log
from mo_sql import ConcatSQL, SQL_SELECT, SQL_FROM, SQL_LEFT_JOIN, SQL, SQL_WHERE, JoinSQL, SQL_AND, \
    sql_list, SQL_ORDERBY, SQL_LIMIT, SQL_ONE
from mo_times import Date


class Subscription:
    """
    USE A SUBSCRIPTION TO LISTEN TO A QUEUE
    """

    def __init__(self, id, queue):
        self.id = id
        self.queue = queue

    def pop(self):
        with self.queue.broker.db.transaction() as t:
            # CHECK IF SOME MESSAGES CAN BE RESENT
            result = t.query(
                ConcatSQL(
                    SQL_SELECT,
                    sql_list((
                        quote_column("m", "serial"),
                        quote_column("m", "content"),
                    )),
                    SQL_FROM,
                    sql_alias(UNCONFIRMED, "u"),
                    SQL_LEFT_JOIN,
                    sql_alias(MESSAGES, "m"),
                    SQL(" ON m.serial=u.serial"),
                    SQL_WHERE,
                    JoinSQL(SQL_AND, (
                        SQL("m.queue = ") + quote_value(self.queue.id),
                        SQL("u.subscriber = ") + quote_value(self.id),
                        SQL("u.deliver_time >= ") + quote_column(Date.now() - self.queue.confirm_delay)
                    )),
                    SQL_ORDERBY,
                    quote_column("u", "deliver_time"),
                    SQL_LIMIT,
                    SQL_ONE
                )
            )

            if result.data:
                # RECORD IT WAS SENT AGAIN
                record = first(result.data)
                t.execute(sql_update(
                    UNCONFIRMED,
                    {
                        "set": {"deliver_time": Date.now},
                        "where": {"eq": {
                            "subscriber": self.id,
                            "serial": record.serial
                        }}
                    }
                ))
                return record.serial, record.content

            # IS THERE A FRESH MESSAGE?
            serial = self._next_serial(t)
            if not serial:
                return 0, None

            result = t.query({
                "select": "content",
                "from": MESSAGES,
                "where": {"eq": {
                    "queue": self.queue.id,
                    "serial": serial
                }}
            })

            if not result.data:
                Log.error("not handled yet, load block")

            content = first(result.data).content

            # RECORD IT WAS SENT
            t.execute(sql_insert(
                UNCONFIRMED,
                {
                    "subscriber": self.id,
                    "serial": serial,
                    "deliver_time": Date.now()
                }
            ))
            return serial, content

    def confirm(self, serial):
        with self.queue.broker.db.transaction() as t:
            t.execute(sql_update(
                UNCONFIRMED,
                {
                    "set": {"deliver_time": Date.now},
                    "where": {"eq": {
                        "subscriber": self.id,
                        "serial": serial
                    }}
                }
            ))

    def _next_serial(self, t):
        """
        EXPECTING OPEN TRANSACTION t
        """

        # TEST IF THERE ARE MESSAGES TO EMIT
        result = t.query(ConcatSQL(
            SQL_SELECT,
            quote_column("s", "next_emit_serial"),
            SQL_FROM,
            sql_alias(SUBSCRIBER, "s"),
            SQL_LEFT_JOIN,
            sql_alias(QUEUE, "q"),
            SQL(" ON q.id = s.queue"),
            SQL_WHERE,
            JoinSQL(
                SQL_AND,
                (
                    SQL("s.id = ") + quote_value(self.id),
                    SQL("q.next_serial > s.next_emit_serial")
                )
            )
        ))

        if not result.data:
            # NO NEW MESSAGES
            return None

        next_id = first(result.data).next_emit_serial
        t.execute(sql_update(
            SUBSCRIBER,
            {
                "set": {"next_emit_serial": next_id + 1},
                "where": {"eq": {"id": self.id}}
            }))
        return next_id
