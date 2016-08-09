from tests.logfiletest import LogFileTest


class TestApp(LogFileTest):
    def test_total_time(self):
        # History Server shows 33s
        rounded_secs = round(self.app.duration / 1000)
        self.assertEqual(rounded_secs, 33)

    def test_worker_amount(self):
        self.assertEqual(len(self.app.slaves), 2)

    def test_2_files_create_2_apps(self):
        self.parser.parse_file(self.LOG_FILE)
        app2 = self.parser.app
        self.assertIsNot(self.app, app2)
