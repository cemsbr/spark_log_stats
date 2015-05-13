from json import loads


class LogParser:
    def __init__(self, filename):
        self.start = -1
        self.end = -1

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

    @property
    def duration(self):
        return self.end - self.start
