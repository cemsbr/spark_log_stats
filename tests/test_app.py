import unittest
from sparklogstats import LogParser


class TestApp(unittest.TestCase):
    def test_total_time(self):
        log = LogParser('app-20150427122457-0000')
        # History Server shows 33s
        rounded_secs = round(log.duration / 1000)
        self.assertEqual(rounded_secs, 33)

    def test_worker_amount(self):
        log = LogParser('app-20150427122457-0000')
        self.assertEqual(len(log.workers), 2)


if __name__ == '__main__':
    unittest.main()
