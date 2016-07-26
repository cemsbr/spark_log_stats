import unittest
from sparklogstats import LogParser


class TestMetrics(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        parser = LogParser()
        parser.parse_file('app-20150427122457-0000')
        cls.app = parser.app

    @classmethod
    def get_task(cls, stage_id, task_id):
        """Return the task given its ID as it is in the log file."""
        offset = sum(len(cls.app.stages[i].tasks) for i in range(stage_id))
        return cls.app.stages[stage_id].tasks[task_id - offset]

    def test_gc_time(self):
        task = self.get_task(1, 43)
        actual = task.metrics.gc
        self.assertEqual(actual, 51)

    def test_deserialization_time(self):
        task = self.get_task(1, 43)
        actual = task.metrics.deserialization
        self.assertEqual(actual, 1)

    def test_blocked_io_time(self):
        task = self.get_task(1, 46)
        actual = task.metrics.blocked_io
        self.assertEqual(actual, 29)

    def test_scheduler_delay_serialize(self):  # (and deserialization)
        task = self.get_task(1, 16)
        actual = task.scheduler_delay
        self.assertEqual(actual, 29)

    def test_scheduler_delay_gc(self):
        task = self.get_task(1, 43)
        actual = task.scheduler_delay
        self.assertEqual(actual, 19)

    def test_scheduler_delay_blocked_io(self):
        task = self.get_task(1, 46)
        actual = task.scheduler_delay
        self.assertEqual(actual, 18)

    def test_input_0_bytes_read(self):
        task = self.get_task(0, 0)
        actual = task.metrics.bytes_read
        self.assertEqual(actual, 0)

    def test_input_non0_bytes_read(self):
        task = self.get_task(0, 1)
        actual = task.metrics.bytes_read
        self.assertEqual(actual, 134283264)

    def test_shuffle_bytes_read(self):
        task = self.get_task(1, 16)
        actual = task.metrics.bytes_read
        self.assertEqual(actual, 2103 + 1837)  # local + remote

    def test_shuffle_bytes_written(self):
        task = self.get_task(0, 1)
        actual = task.metrics.bytes_written
        self.assertEqual(actual, 47622)


if __name__ == '__main__':
    unittest.main()
