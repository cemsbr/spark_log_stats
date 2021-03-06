import glob
from json import loads


class LogParser:
    def __init__(self):
        self._reset()
        self._json = None

    def _reset(self):
        self.app = Application()

    def parse_folder(self, folder):
        logs = sorted(glob.glob(folder + '/app-*'))
        return (self.parse_file(log) for log in logs)

    def parse_file(self, filename):
        self._reset()
        with open(filename) as file:
            self.app.filename = filename
            self.parse_lines(file.readlines())
        return self.app

    def parse_lines(self, lines):
        for line in lines:
            self._json = loads(line)
            self.parse_json()

    def parse_json(self):
        json = self._json
        event = json["Event"]
        if event == "SparkListenerTaskStart":
            self._parse_task_start()
        elif event == "SparkListenerTaskEnd":
            self._parse_task_end()
        elif event == "SparkListenerJobStart":
            self._parse_job()
        elif event == "SparkListenerJobEnd":
            job_id = json["Job ID"]
            self.app.jobs[job_id].end = json["Completion Time"]
        elif event == "SparkListenerExecutorAdded":
            self.app.slaves.add(json["Executor Info"]["Host"])
        elif event == "SparkListenerApplicationStart":
            self.app.start = json["Timestamp"]
            self.app.name = json["App Name"]
        elif event == "SparkListenerApplicationEnd":
            self.app.end = json["Timestamp"]
        elif event == "SparkListenerStageCompleted":
            info = json['Stage Info']
            stage = self.app.stages[int(info['Stage ID'])]
            stage.start = info['Submission Time']
            stage.end = info['Completion Time']

    def _parse_job(self):
        if self._json["Job ID"] != len(self.app.jobs):
            # Not sure if it can happen, but it's better to check
            raise ParserException('Job IDs are not in order!')
        job = Job()
        job.start = self._json["Submission Time"]
        job.stages = self._parse_stages()
        self.app.jobs.append(job)

    def _parse_stages(self):
        stages = []
        for info in sorted(
                self._json["Stage Infos"], key=lambda x: x['Stage ID']):
            stage_id = int(info['Stage ID'])
            if stage_id != len(self.app.stages):
                # Not sure if it can happen, but it's better to check
                raise ParserException("Stage IDs are not in order!")
            stage = Stage()
            stage.id = stage_id
            stage.name = info["Stage Name"]
            stage.total_tasks = int(info["Number of Tasks"])
            stages.append(stage)
            self.app.stages.append(stage)
        return stages

    def _parse_task_start(self):
        stage_id = self._json["Stage ID"]
        task_json = self._json["Task Info"]

        task_id = int(task_json["Task ID"])
        if task_id != len(self.app.tasks):
            # Not sure if it can happen, but it's better to check
            raise ParserException("Tasks IDs are not in order or multiple "
                                  "attempts found.")

        task = Task()
        task.id = task_id
        task.index = task_json["Index"]
        task.start = task_json["Launch Time"]

        self.app.tasks.append(task)
        self.app.stages[stage_id].tasks.append(task)

    def _parse_task_end(self):
        info_json = self._json["Task Info"]
        task_id = int(info_json["Task ID"])
        task = self.app.tasks[task_id]
        assert task_id == task.id

        task.host = info_json["Host"]
        task.speculative = info_json["Speculative"]
        task.locality = info_json["Locality"]
        task.failed = info_json["Failed"]
        task.end = info_json["Finish Time"]
        if not task.failed:
            task.metrics = self._parse_metrics()

    def _parse_metrics(self):
        info = self._json["Task Metrics"]
        metrics = Metrics()
        metrics.gc = info["JVM GC Time"]
        metrics.deserialization = info["Executor Deserialize Time"]
        metrics.executor = info["Executor Run Time"]
        metrics.serialization = info["Result Serialization Time"]

        if "Shuffle Read Metrics" in info:
            shuffle_info = info["Shuffle Read Metrics"]
            metrics.blocked_io = shuffle_info["Fetch Wait Time"]
            metrics.bytes_read = shuffle_info["Local Bytes Read"] + \
                shuffle_info["Remote Bytes Read"]
        elif "Input Metrics" in info:
            input_info = info["Input Metrics"]
            metrics.bytes_read = input_info["Bytes Read"]
            metrics.data_read_method = input_info["Data Read Method"]
            metrics.records_read = input_info["Records Read"]
            metrics.blocked_io = 0
        else:
            metrics.blocked_io = 0

        if "Shuffle Write Metrics" in info:
            shuffle_info = info["Shuffle Write Metrics"]
            metrics.bytes_written = shuffle_info["Shuffle Bytes Written"]
        else:
            metrics.bytes_written = 0

        return metrics


# pylint: disable=invalid-name
class Timed:
    def __init__(self):
        self._start = self._end = -1

    @property
    def start(self):
        return self._start

    @start.setter
    def start(self, value):
        self._start = value

    @property
    def end(self):
        return self._end

    @end.setter
    def end(self, value):
        self._end = value

    @property
    def duration(self):
        return self.end - self.start


class Application(Timed):
    def __init__(self):
        super().__init__()
        self.slaves = set()
        self.jobs = []
        self.stages = []
        self.tasks = []
        self.name = None
        self.filename = None

    @property
    def bytes_read(self):
        """Input size."""
        return self.stages[0].bytes_read

    @property
    def records_read(self):
        """Number of lines read from input."""
        return self.stages[0].records_read


class Job(Timed):
    def __init__(self):
        super().__init__()
        self.stages = []


class Stage(Timed):
    def __init__(self):
        super().__init__()
        self.id = None
        self.name = None
        self.total_tasks = None
        self.tasks = []

    @property
    def bytes_read(self):
        return sum(t.metrics.bytes_read for t in self.successful_tasks
                   if t.metrics.bytes_read)

    @property
    def bytes_written(self):
        return sum(t.metrics.bytes_written for t in self.successful_tasks)

    @property
    def records_read(self):
        return sum(t.metrics.records_read for t in self.successful_tasks)

    @property
    def successful_tasks(self):
        return (t for t in self.tasks if not t.failed)


class Metrics:
    def __init__(self):
        self.gc = None
        self.deserialization = None
        self.executor = None  # seems to include gc and blocked-io times
        self.blocked_io = None
        self.serialization = None
        self.bytes_read = None
        self.bytes_written = None
        self.data_read_method = None
        self.records_read = None

    @property
    def non_scheduler(self):
        return self.deserialization + self.executor + self.serialization

    @property
    def computation(self):
        return self.executor - self.gc - self.blocked_io


class Task(Timed):
    # pylint: disable=too-many-instance-attributes
    def __init__(self):
        super().__init__()
        self.id = None
        self.index = None
        self.host = None
        self.speculative = None
        self.locality = None
        self.failed = None
        self.metrics = None

    @property
    def scheduler_delay(self):
        return self.duration - self.metrics.non_scheduler


class ParserException(Exception):
    pass
