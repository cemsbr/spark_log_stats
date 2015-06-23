import unittest
from sparklogstats import LogParser


class TestMetrics(unittest.TestCase):
    @staticmethod
    def parse_file():
        parser = LogParser()
        parser.parse_file('app-20150427122457-0000')
        return parser.app

    def test_gc_time(self):
        app = TestMetrics.parse_file()
        actual = app.stages[1].tasks[27].metrics.gc
        self.assertEqual(actual, 51)

    def test_deserialization_time(self):
        app = TestMetrics.parse_file()
        actual = app.stages[1].tasks[27].metrics.deserialization
        self.assertEqual(actual, 1)

    def test_blocked_io_time(self):
        app = TestMetrics.parse_file()
        actual = app.stages[1].tasks[30].metrics.blocked_io
        self.assertEqual(actual, 29)

    def test_scheduler_delay_serialize(self):  # (and deserialization)
        app = TestMetrics.parse_file()
        actual = app.stages[1].tasks[0].scheduler_delay
        self.assertEqual(actual, 29)

    def test_scheduler_delay_gc(self):
        app = TestMetrics.parse_file()
        actual = app.stages[1].tasks[27].scheduler_delay
        self.assertEqual(actual, 19)

    def test_scheduler_delay_blocked_io(self):
        app = TestMetrics.parse_file()
        actual = app.stages[1].tasks[30].scheduler_delay
        self.assertEqual(actual, 18)


if __name__ == '__main__':
    unittest.main()
