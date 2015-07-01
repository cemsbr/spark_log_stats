import unittest
from sparklogstats import LogParser


class TestTask(unittest.TestCase):
    @staticmethod
    def parse_file():
        parser = LogParser()
        parser.parse_file('app-20150427122457-0000')
        return parser.app

    def test_task_amount(self):
        app = TestTask.parse_file()
        for job in app.jobs:
            for stage in job.stages:
                actual = len(stage.tasks)
                expected = stage.total_tasks
                self.assertEqual(actual, expected)


if __name__ == '__main__':
    unittest.main()
