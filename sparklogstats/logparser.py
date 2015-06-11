from json import loads


class LogParser:
    def __init__(self):
        self._reset()

    def _reset(self):
        self.app = Application()

    def parse_file(self, filename):
        self._reset()
        with open(filename) as file:
            for line in file.readlines():
                json = loads(line)
                self.parse_json(json)

    def parse_lines(self, lines):
        for line in lines:
            json = loads(line)
            self.parse_json(json)

    def parse_json(self, json):
        event = json["Event"]
        if event == "SparkListenerApplicationStart":
            self.app.start = json["Timestamp"]
        elif event == "SparkListenerApplicationEnd":
            self.app.end = json["Timestamp"]
        elif event == "SparkListenerExecutorAdded":
            self.app.workers.add(json["Executor Info"]["Host"])
        elif event == "SparkListenerJobStart":
            if json['Job ID'] != len(self.app.jobs):
                # Not sure if it can happen, but it's better to check
                raise ParserException('Job IDs are not in order!')
            else:
                job = Job()
                job.start = json["Submission Time"]
                self.app.jobs.append(job)
        elif event == "SparkListenerJobEnd":
            job_id = json["Job ID"]
            self.app.jobs[job_id].end = json["Completion Time"]


# pylint: disable=R0903
class Timed:
    def __init__(self):
        self.start = self.end = -1

    @property
    def duration(self):
        return self.end - self.start


class Application(Timed):
    def __init__(self):
        super().__init__()
        self.workers = set()
        self.jobs = []


class Job(Timed):
    pass


class ParserException(Exception):
    pass
