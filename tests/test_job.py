import unittest
from sparklogstats import LogParser, ParserException


class TestJob(unittest.TestCase):
    @staticmethod
    def parse_file():
        parser = LogParser()
        parser.parse_file('app-20150427122457-0000')
        return parser.app

    def test_duration_first_job(self):
        app = TestJob.parse_file()
        # History Server shows 26s
        rounded_secs = round(app.jobs[0].duration / 1000)
        self.assertEqual(rounded_secs, 26)

    def test_duration_second_job(self):
        app = TestJob.parse_file()
        # History Server shows 60ms
        msec = app.jobs[1].duration
        self.assertEqual(msec, 60)

    def test_job_id_order_exception(self):
        """Assume job IDs are ordered. Otherwise, raise exception."""
        job1 = '{"Event": "SparkListenerJobStart", "Job ID": 1}'
        parser = LogParser()
        self.assertRaises(ParserException, parser.parse_lines, [job1])


if __name__ == '__main__':
    unittest.main()
