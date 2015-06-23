import unittest
from sparklogstats import LogParser


class TestStage(unittest.TestCase):
    @staticmethod
    def parse_file():
        parser = LogParser()
        parser.parse_file('app-20150427122457-0000')
        return parser.app

    def test_one_job_has_two_stages(self):
        app = TestStage.parse_file()
        actual = len(app.jobs[0].stages)
        self.assertEqual(actual, 2)

    def test_one_job_has_one_stage(self):
        app = TestStage.parse_file()
        actual = len(app.jobs[1].stages)
        self.assertEqual(actual, 1)
