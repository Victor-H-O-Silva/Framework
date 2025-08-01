import datetime as pydt
import traceback as pytrace
import enum as pyenum
from pyspark.sql import functions as F, types as T

try:
    dbutils
except NameError:
    from pyspark.dbutils import DBUtils
    dbutils = DBUtils(spark)

class DataJobStatus(pyenum.Enum):
    NOT_STARTED = pyenum.auto()
    IN_PROGRESS = pyenum.auto()
    SUCCESS = pyenum.auto()
    ERROR = pyenum.auto()
    FATAL_ERROR = pyenum.auto()

class DataJobPipelineStatus(pyenum.Enum):
    NOT_STARTED = pyenum.auto()
    IN_PROGRESS = pyenum.auto()
    SUCCESS = pyenum.auto()
    ERROR = pyenum.auto()
    FATAL_ERROR = pyenum.auto()

class JobLogType(pyenum.Enum):
    INFO = pyenum.auto()
    WARNING = pyenum.auto()
    DEBUG = pyenum.auto()
    ERROR = pyenum.auto()
    FATAL_ERROR = pyenum.auto()

class DataJob:
    log_path = ""
    log_print = True
    name = ""
    pipelines = []
    pipeline_retry_threshold = 3
    ignore_dependencies = False

    # Email configuration (set by runner)
    email = {
        'send_success': False,
        'send_fail': False,
        'fail_recipients': [''],
        'success_recipients': [''],
        'pipeline_send_fail': False,
        'pipeline_send_success': False,
        'pipeline_fail_recipients': [''],
        'pipeline_success_recipients': [''],
        'application_id': '',
        'application_name': '',
        'environment': '',
        'origin': '',
        'sender': '',
        'smtp_host': '',
        'smtp_port': '',
        'smtp_login': '',
        'smtp_password': ''
    }

    job_control_database = ""
    job_control_table = ""
    job_control_table_path = ""
    pipeline_control_database = ""
    pipeline_control_table = ""
    pipeline_control_table_path = ""

    _job_status = {}
    _start_time = None
    _end_time = None

    @staticmethod
    def _get_dbutils_context():
        ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
        run_id = ctx.runId().get() if ctx.runId().isDefined() else None
        job_id = ctx.jobId().get() if ctx.jobId().isDefined() else None
        job_name = ctx.jobName().get() if ctx.jobName().isDefined() else None
        task_key = ctx.currentRunId().get() if ctx.currentRunId().isDefined() else None
        return run_id, job_id, job_name, task_key

    @staticmethod
    def run():
        run_id, job_id, job_name, task_key = DataJob._get_dbutils_context()
        pipeline_name = DataJob.pipelines[0] if DataJob.pipelines else ""
        DataJob._start_time = pydt.datetime.now()

        DataJob._job_status = {
            "run_id": run_id,
            "job_id": job_id,
            "job_name": job_name,
            "pipeline_name": pipeline_name,
            "task_key": task_key,
            "status": DataJobStatus.IN_PROGRESS.name,
            "start_time": DataJob._start_time,
            "end_time": None,
            "message": ""
        }

        DataJob._update_job_status()

        try:
            DataJob._log(JobLogType.INFO, run_id, job_name, pipeline_name, "DataJob.run", f"Starting pipeline '{pipeline_name}' with run_id {run_id}")

            DataJob._run_single_pipeline()

            DataJob._job_status["status"] = DataJobStatus.SUCCESS.name
            DataJob._job_status["message"] = "Pipeline executed successfully."
            DataJob._log(JobLogType.INFO, run_id, job_name, pipeline_name, "DataJob.run", "Pipeline completed successfully.")
        except Exception as ex:
            DataJob._job_status["status"] = DataJobStatus.FATAL_ERROR.name
            DataJob._job_status["message"] = f"Pipeline failed: {str(ex)}"
            DataJob._log(JobLogType.FATAL_ERROR, run_id, job_name, pipeline_name, "DataJob.run", f"Pipeline failed with exception: {ex}")
            DataJob._log(JobLogType.DEBUG, run_id, job_name, pipeline_name, "DataJob.run", pytrace.format_exc())
            raise
        finally:
            DataJob._end_time = pydt.datetime.now()
            DataJob._job_status["end_time"] = DataJob._end_time
            DataJob._update_job_status()
            DataJob._job_end_notification()

    @staticmethod
    def _run_single_pipeline():
        pipeline_name = DataJob.pipelines[0]
        run_id, job_id, job_name, task_key = DataJob._get_dbutils_context()
        status = {
            "run_id": run_id,
            "job_id": job_id,
            "job_name": job_name,
            "pipeline_name": pipeline_name,
            "task_key": task_key,
            "status": DataJobPipelineStatus.NOT_STARTED.name,
            "start_time": None,
            "end_time": None,
            "message": ""
        }
        try:
            DataJob._log(JobLogType.INFO, run_id, job_name, pipeline_name, "DataJob._run_single_pipeline", "Starting pipeline execution.")
            status["start_time"] = pydt.datetime.now()
            status["status"] = DataJobPipelineStatus.IN_PROGRESS.name
            DataJob._update_pipeline_status(status)
            # Dynamically import the pipeline class (must be registered/imported by name)
            from DataPipeline import DataPipeline  # Must be in sys.path or via %run
            pipeline_class = globals()[pipeline_name] if pipeline_name in globals() else getattr(__import__('DataPipeline'), pipeline_name)
            pipe = pipeline_class(
                job_id=job_id,
                job_name=job_name,
                pipeline_id=run_id
            )
            result = pipe.run()
            status["end_time"] = pydt.datetime.now()
            if result:
                status["status"] = DataJobPipelineStatus.SUCCESS.name
                status["message"] = f"Pipeline '{pipeline_name}' executed successfully."
                DataJob._log(JobLogType.INFO, run_id, job_name, pipeline_name, "DataJob._run_single_pipeline", status["message"])
            else:
                status["status"] = DataJobPipelineStatus.ERROR.name
                status["message"] = f"Pipeline '{pipeline_name}' finished with ERROR."
                DataJob._log(JobLogType.ERROR, run_id, job_name, pipeline_name, "DataJob._run_single_pipeline", status["message"])
            DataJob._update_pipeline_status(status)
        except Exception as ex:
            status["end_time"] = pydt.datetime.now()
            status["status"] = DataJobPipelineStatus.FATAL_ERROR.name
            status["message"] = f"Pipeline '{pipeline_name}' failed: {str(ex)}"
            DataJob._log(JobLogType.FATAL_ERROR, run_id, job_name, pipeline_name, "DataJob._run_single_pipeline", status["message"])
            DataJob._log(JobLogType.DEBUG, run_id, job_name, pipeline_name, "DataJob._run_single_pipeline", pytrace.format_exc())
            DataJob._update_pipeline_status(status)
            DataJob._pipeline_end_notification(pipeline_name)
            raise

    @staticmethod
    def _update_job_status():
        """Write job status to control table."""
        df = spark.createDataFrame([DataJob._job_status])
        path = DataJob.job_control_table_path
        DataJob._create_table_from_df(path, DataJob.job_control_database, DataJob.job_control_table, df)
        from delta.tables import DeltaTable
        deltaTable = DeltaTable.forPath(spark, path)
        deltaTable.alias("ctrl").merge(
            df.alias("new_status"),
            "ctrl.run_id = new_status.run_id AND ctrl.pipeline_name = new_status.pipeline_name"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

    @staticmethod
    def _update_pipeline_status(status):
        """Write pipeline status to control table."""
        df = spark.createDataFrame([status])
        path = DataJob.pipeline_control_table_path
        DataJob._create_table_from_df(path, DataJob.pipeline_control_database, DataJob.pipeline_control_table, df)
        from delta.tables import DeltaTable
        deltaTable = DeltaTable.forPath(spark, path)
        deltaTable.alias("ctrl").merge(
            df.alias("new_status"),
            "ctrl.run_id = new_status.run_id AND ctrl.pipeline_name = new_status.pipeline_name"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

    @staticmethod
    def _create_table_from_df(path, database, name, df):
        """Create Delta table from dataframe if not exists."""
        table = f"{database}.{name}"
        df.createOrReplaceTempView('temp_create_table')
        spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {table}
            USING DELTA
            OPTIONS (path '{path}')
            AS SELECT * FROM temp_create_table
            """
        )

    @staticmethod
    def _log(log_type, run_id, job_name, pipeline_name, event, message):
        log_timestamp = pydt.datetime.now()
        log_str = f"[{log_timestamp}] [{log_type.name}] [run_id: {run_id}] [job: {job_name}] [pipeline: {pipeline_name}] [{event}] {message}"
        if DataJob.log_print:
            print(log_str)
        # Optionally: write to file or Delta table

    @staticmethod
    def _job_end_notification():
        # Optional: send email at end of job
        if DataJob.email.get('send_success', False) or DataJob.email.get('send_fail', False):
            subject = f"Pipeline Job Finished: {DataJob._job_status.get('pipeline_name', '')} (Run {DataJob._job_status.get('run_id', '')})"
            body = f"Status: {DataJob._job_status.get('status', '')}\nMessage: {DataJob._job_status.get('message', '')}\nStart: {DataJob._job_status.get('start_time', '')}\nEnd: {DataJob._job_status.get('end_time', '')}"
            recipients = DataJob.email.get('success_recipients', []) if DataJob._job_status.get('status', '') == 'SUCCESS' else DataJob.email.get('fail_recipients', [])
            DataJob.send_email(subject, body, recipients)

    @staticmethod
    def _pipeline_end_notification(pipeline_name):
        # Optional: send pipeline-level notifications
        pass

    @staticmethod
    def send_email(subject, body, recipients):
        # Placeholder for actual email logic
        print(f"Sending email to {recipients}\nSubject: {subject}\n{body}")
        # Use SMTP or integration as needed

# End of DataJob.py
