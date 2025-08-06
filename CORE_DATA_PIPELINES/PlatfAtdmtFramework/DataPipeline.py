class PipelineLogType:
    INFO = "INFO"
    WARNING = "WARNING"
    DEBUG = "DEBUG"
    ERROR = "ERROR"
    FATAL_ERROR = "FATAL_ERROR"

class DataPipeline:
    log_path = ''
    log_print = True

    def __init__(self, job_id, job_name, pipeline_id, params=None):
        self.pipeline_id = pipeline_id
        self.job_id = job_id
        self.job_name = job_name
        self.pipeline_name = self.__class__.__name__
        self.start_time = pydt.datetime.now()
        self._params = params or {}

        self._log(PipelineLogType.INFO, f"Initializing pipeline: {self.pipeline_name}")

    def run(self):
        raise NotImplementedError("Subclasses must implement the run() method.")

    def _log(self, log_type, message, event=None):
        log_timestamp = pydt.datetime.now()
        event = event or f"{self.pipeline_name}.run"

        if DataPipeline.log_print:
            print(f"[{log_timestamp}] [{log_type}] [{self.pipeline_name}] {event} - {message}")
