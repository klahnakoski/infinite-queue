from infinite_queue.broker import Broker
from mo_files import File
from mo_logs import startup, constants, Log
from mo_testing.fuzzytestcase import FuzzyTestCase
from mo_threads import Till

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

    def test_push_speed(self):
        queue = broker.get_or_create_queue("test_push")

        duration = 10
        rate = None
        data = [
            {chr(i): i for i in range(65, 91)}
            for _ in range(1000)
        ]

        done = Till(seconds=duration)
        for i, d in enumerate(data):
            queue.push(d)
            if done:
                rate = i / duration
                break
        else:
            Log.error("not enough data to run test")
        Log.note("push rate of {{rate}}/second", rate=rate)
