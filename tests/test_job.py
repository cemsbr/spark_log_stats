from sparklogstats.logparser import LogParser, ParserException
from tests.logfiletest import LogFileTest


class TestJob(LogFileTest):
    def test_duration_first_job(self):
        # History Server shows 26s
        rounded_secs = round(self.app.jobs[0].duration / 1000)
        self.assertEqual(rounded_secs, 26)

    def test_duration_second_job(self):
        # History Server shows 60ms
        msec = self.app.jobs[1].duration
        self.assertEqual(msec, 60)

    def test_job_id_order_exception(self):
        """Assume job IDs are ordered. Otherwise, raise exception."""
        job1 = '{"Event": "SparkListenerJobStart", "Job ID": 1}'
        parser = LogParser()
        self.assertRaises(ParserException, parser.parse_lines, [job1])
