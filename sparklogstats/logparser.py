from json import loads


class LogParser:
    def __init__(self, filename):
        self.start = self.end = -1
        self.workers = set()
        self.jobs = []

        self._parse_file(filename)

    def _parse_file(self, filename):
        with open(filename) as f:
            for line in f.readlines():
                json = loads(line)
                self._parse_json(json)

    def _parse_json(self, json):
        event = json["Event"]
        if event == "SparkListenerApplicationStart":
            self.start = json["Timestamp"]
        elif event == "SparkListenerApplicationEnd":
            self.end = json["Timestamp"]
        elif event == "SparkListenerExecutorAdded":
            self.workers.add(json["Executor Info"]["Host"])
        elif event == "SparkListenerJobStart":
            if json['Job ID'] != len(self.jobs):
                # Not sure if it can happen, but it's better to check
                raise Exception('Job IDs are not in order!')
            else:
                job = Job()
                job.start = json["Submission Time"]
                self.jobs.append(job)
        elif event == "SparkListenerJobEnd":
            job_id = json["Job ID"]
            self.jobs[job_id].end = json["Completion Time"]

    @property
    def duration(self):
        return self.end - self.start


class Job:
    def __init__(self):
        self.start = self.end = -1

    @property
    def duration(self):
        return self.end - self.start
