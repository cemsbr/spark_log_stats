from tests.logfiletest import LogFileTest


class TestStage(LogFileTest):
    def test_one_job_has_two_stages(self):
        actual = len(self.app.jobs[0].stages)
        self.assertEqual(actual, 2)

    def test_one_job_has_one_stage(self):
        actual = len(self.app.jobs[1].stages)
        self.assertEqual(actual, 1)

    def test_shuffle_bytes_written(self):
        stage = self.app.stages[0]
        self.assertEqual(stage.bytes_written, 916974)

    def test_input_records_read(self):
        stage = self.app.stages[0]
        self.assertEqual(stage.records_read, 284108)
