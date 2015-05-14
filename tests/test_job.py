import unittest
from sparklogstats import LogParser


class TestJob(unittest.TestCase):
    def test_duration_first_job(self):
        log = LogParser('app-20150427122457-0000')
        # History Server shows 26s
        rounded_secs = round(log.jobs[0].duration / 1000)
        self.assertEqual(rounded_secs, 26)

    def test_duration_second_job(self):
        log = LogParser('app-20150427122457-0000')
        # History Server shows 60ms
        msec = log.jobs[1].duration
        self.assertEqual(msec, 60)


if __name__ == '__main__':
    unittest.main()
