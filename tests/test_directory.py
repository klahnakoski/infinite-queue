from infinite_queue.broker import Broker
from mo_files import File
from mo_json import json2value
from mo_logs import startup, constants, Log
from mo_testing.fuzzytestcase import FuzzyTestCase
from mo_threads import Till
from mo_times import Date

config=None
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
        Log.stop()

    def test_push(self):
        queue = broker.get_or_create_queue("test1")
        data = {"a": 1, "b": 2}
        queue.add(data)

        listener = broker.get_listener("test1")
        serial, content = listener.pop()
        listener.confirm(serial)

        self.assertAlmostEqual(content, data)
        queue.flush()

        content = (File(config.broker.backing.directory)/Date.now().format("%Y/%m/%d")/"1.json").read()
        for line in content.split("/n"):
            self.assertAlmostEqual(json2value(line), data)

