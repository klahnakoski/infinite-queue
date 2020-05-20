from infinite_queue.broker import Broker
from infinite_queue.utils import MESSAGES, SUBSCRIBER
from jx_base.expressions import NULL
from jx_sqlite.sqlite import sql_query, sql_update
from jx_sqlite.utils import first_row
from mo_files import File
from mo_json import json2value
from mo_logs import startup, constants, Log
from mo_testing.fuzzytestcase import FuzzyTestCase
from mo_times import Date

config = None
broker = None


class TestDirectory(FuzzyTestCase):
    @classmethod
    def setUpClass(cls):
        global config, broker
        try:
            config = startup.read_settings(filename="tests/config/file.json")
            constants.set(config.constants)
            Log.start(config.debug)

            File(config.broker.backing.directory).delete()
            broker = Broker(kwargs=config.broker)
        except Exception as e:
            Log.error("could not setup for testing", cause=e)

    @classmethod
    def tearDownClass(cls):
        broker.close()
        Log.stop()

    def test_push_pop(self):
        queue = broker.get_or_create_queue("test1")
        data = {"a": 1, "b": 2}
        queue.add(data)

        subscriber = broker.get_subscriber("test1")
        serial, content = subscriber.pop()
        subscriber.confirm(serial)

        self.assertAlmostEqual(content, data)
        queue.flush()

        content = (
            File(config.broker.backing.directory)
            / queue.name
            / Date.now().format("%Y/%m/%d")
            / "1.json"
        ).read()
        for line in content.split("/n"):
            self.assertAlmostEqual(json2value(line), data)

    def test_repeat(self):
        queue = broker.get_or_create_queue("test2")
        data = {"a": 1, "b": 2}
        queue.add(data)

        subscriber = broker.replay(
            name="test2",
            confirm_delay_seconds=0,  # WE GET SAME MESSAGE OVER AND OVER UNTIL confirm()
        )
        serial, content = subscriber.pop()
        for i in range(9):
            serial, content = subscriber.pop()
            self.assertAlmostEqual(content, data)
        subscriber.confirm(serial)
        serial, content = subscriber.pop()
        self.assertAlmostEqual(content, NULL)
        queue.flush()

        content = (
            File(config.broker.backing.directory)
            / queue.name
            / Date.now().format("%Y/%m/%d")
            / "1.json"
        ).read()
        for line in content.split("/n"):
            self.assertAlmostEqual(json2value(line), data)

    def test_message_lifecycle(self):
        queue = broker.get_or_create_queue("test3", block_size_mb=0)
        data = {"a": 1, "b": 2}

        # MESSAGE IS NOT EMITTED
        serial = queue.add(data)
        queue.flush()
        broker.clean()
        self.assertMessageExists(serial, queue.id)

        subscriber = broker.get_subscriber("test3")
        # ENSURE THERE IS NO LOOK-AHEAD
        with broker.db.transaction() as t:
            t.execute(
                sql_update(
                    SUBSCRIBER,
                    {
                        "set": {"look_ahead_serial": 0},
                        "where": {"eq": {"id": subscriber.id}},
                    },
                )
            )

        # MESSAGE SENT, BUT NOT CONFIRMED
        serial, _ = subscriber.pop()
        queue.flush()
        broker.clean()
        self.assertMessageExists(serial, queue.id)

        # MESSAGE SENT, AND CONFIRMED
        subscriber.confirm(serial)
        queue.flush()
        broker.clean()
        self.assertRaises(Exception, self.assertMessageExists, serial, queue.id)

    def test_two_subscribers_lifecycle(self):
        queue = broker.get_or_create_queue("test4", block_size_mb=0)
        # ENSURE THERE IS NO LOOK-AHEAD
        with broker.db.transaction() as t:
            t.execute(
                sql_update(
                    SUBSCRIBER,
                    {
                        "set": {"look_ahead_serial": 0},
                        "where": {"eq": {"id": queue.id}},
                    },
                )
            )

        data1 = {"a": 1, "b": 2}
        data2 = {"c": 3, "d": 4}

        # MESSAGE IS NOT EMITTED
        serial1 = queue.add(data1)
        serial2 = queue.add(data2)

        subscriber1 = broker.replay("test4", look_ahead_serial=1)
        subscriber1.confirm(subscriber1.pop()[0])
        subscriber1.confirm(subscriber1.pop()[0])

        subscriber2 = broker.replay("test4", look_ahead_serial=1)
        serial1, _ = subscriber2.pop()
        subscriber2.confirm(serial1)

        queue.flush()
        broker.clean()
        self.assertRaises(Exception, self.assertMessageExists, serial1, queue.id)
        self.assertMessageExists(serial2, queue.id)

    def test_recover_messages_from_backing(self):
        queue = broker.get_or_create_queue("test5", block_size_mb=0)
        # ENSURE THERE IS NO LOOK-AHEAD
        with broker.db.transaction() as t:
            t.execute(
                sql_update(
                    SUBSCRIBER,
                    {
                        "set": {"look_ahead_serial": 0},
                        "where": {"eq": {"id": queue.id}},
                    },
                )
            )

        data1 = {"a": 1, "b": 2}
        data2 = {"c": 3, "d": 4}

        # MESSAGE IS NOT EMITTED
        serial1 = queue.add(data1)
        serial2 = queue.add(data2)

        subscriber1 = broker.replay("test5", look_ahead_serial=0)
        subscriber1.confirm(subscriber1.pop()[0])
        subscriber1.confirm(subscriber1.pop()[0])
        queue.flush()
        broker.clean()
        self.assertRaises(Exception, self.assertMessageExists, serial1, queue.id)
        self.assertRaises(Exception, self.assertMessageExists, serial2, queue.id)
        # DATABASE NOW EMPTY

        subscriber2 = broker.replay("test5", look_ahead_serial=0)
        subscriber2.confirm(subscriber2.pop()[0])
        queue.flush()
        broker.clean()
        self.assertRaises(Exception, self.assertMessageExists, serial1, queue.id)
        self.assertMessageExists(serial2, queue.id)

    def assertMessageExists(self, serial, queue_id):
        """
        ENSURE GIVEN MESSAGE, aka (serial, queue) PAIR, STILL EXISTS IN DATABASE
        THROW ERROR IF NOT
        """
        with broker.db.transaction() as t:
            result = t.query(
                sql_query(
                    {
                        "select": "serial",
                        "from": MESSAGES,
                        "where": {"eq": {"serial": serial, "queue": queue_id}},
                    }
                )
            )
            self.assertAlmostEqual(first_row(result).serial, serial)

