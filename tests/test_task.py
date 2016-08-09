from tests.logfiletest import LogFileTest


class TestTask(LogFileTest):
    def test_task_amount(self):
        for job in self.app.jobs:
            for stage in job.stages:
                actual = len(stage.tasks)
                expected = stage.total_tasks
                self.assertEqual(actual, expected)

    def get_task(self, stage_id, task_id):
        """Return the task given its ID as it is in the log file."""
        offset = sum(len(self.app.stages[i].tasks) for i in range(stage_id))
        return self.app.stages[stage_id].tasks[task_id - offset]

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

    def test_input_records_read(self):
        task = self.get_task(0, 0)
        actual = task.metrics.records_read
        self.assertEqual(actual, 10377)
