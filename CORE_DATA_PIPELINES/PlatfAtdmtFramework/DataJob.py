class DataJobStatus(Enum):
    NOT_STARTED = "NOT_STARTED"
    IN_PROGRESS = "IN_PROGRESS"
    SUCCESS = "SUCCESS"
    ERROR = "ERROR"
    FATAL_ERROR = "FATAL_ERROR"

class JobLogType(Enum):
    INFO = "INFO"
    WARNING = "WARNING"
    DEBUG = "DEBUG"
    ERROR = "ERROR"
    FATAL_ERROR = "FATAL_ERROR"

class DataJob:
    log_print = True
    pipeline_name = ""
    params = {}

    _status = {}
    _start_time = None
    _end_time = None

    @staticmethod
    def run():
        pipeline_name = DataJob.pipeline_name
        DataJob._start_time = pydt.datetime.now()

        DataJob._status = {
            "pipeline_name": pipeline_name,
            "status": DataJobStatus.IN_PROGRESS.value,
            "start_time": DataJob._start_time,
            "end_time": None,
            "message": ""
        }

        try:
            DataJob._log(JobLogType.INFO, f"Starting pipeline: {pipeline_name}")
            DataJob._run_pipeline(pipeline_name)
            DataJob._status["status"] = DataJobStatus.SUCCESS.value
            DataJob._status["message"] = "Pipeline executed successfully."

        except Exception as ex:
            DataJob._status["status"] = DataJobStatus.FATAL_ERROR.value
            DataJob._status["message"] = f"Pipeline failed: {str(ex)}"
            DataJob._log(JobLogType.FATAL_ERROR, f"Exception: {str(ex)}")
            DataJob._log(JobLogType.DEBUG, traceback.format_exc())
            raise

        finally:
            DataJob._end_time = pydt.datetime.now()
            DataJob._status["end_time"] = DataJob._end_time
            DataJob._log(JobLogType.INFO, f"Pipeline finished with status: {DataJob._status['status']}")

    @staticmethod
    def _run_pipeline(pipeline_name):
        from DataPipeline import DataPipeline
        try:
            module = __import__('pipelines.' + pipeline_name, fromlist=[pipeline_name])
            pipeline_class = getattr(module, pipeline_name)
        except Exception as e:
            raise ImportError(f"Failed to load pipeline class '{pipeline_name}': {e}")

        pipeline = pipeline_class(
            job_id=0,
            job_name=DataJob.pipeline_name,
            pipeline_id=0,
            params=DataJob.params
        )

        result = pipeline.run()
        if not result:
            raise Exception(f"Pipeline '{pipeline_name}' returned failure flag.")

    @staticmethod
    def _log(log_type, message):
        if DataJob.log_print:
            timestamp = pydt.datetime.now()
            print(f"[{timestamp}] [{log_type.value}] {message}")
