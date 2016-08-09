"""Deal with log files."""
from unittest import TestCase

from sparklogstats.logparser import LogParser


class LogFileTest(TestCase):
    """Parse log file to ``self.app``."""

    LOG_FILE = 'tests/app-20150427122457-0000'

    def setUp(self):
        """Parse log file."""
        self.parser = LogParser()
        self.parser.parse_file(self.LOG_FILE)
        self.app = self.parser.app
