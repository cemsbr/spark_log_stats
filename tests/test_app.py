import unittest
from sparklogstats import LogParser


class TestApp(unittest.TestCase):
    def setUp(self):
        parser = LogParser()
        parser.parse_file('app-20150427122457-0000')
        self.app = parser.app

    def test_total_time(self):
        # History Server shows 33s
        rounded_secs = round(self.app.duration / 1000)
        self.assertEqual(rounded_secs, 33)

    def test_worker_amount(self):
        self.assertEqual(len(self.app.workers), 2)


if __name__ == '__main__':
    unittest.main()
