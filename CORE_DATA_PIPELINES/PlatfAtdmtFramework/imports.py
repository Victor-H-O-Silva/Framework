from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, concat_ws, sha2, expr
from pyspark.sql.types import StringType
import pyspark.sql.functions as F

# Delta Lake
from delta.tables import DeltaTable

# Databricks Utilities (optional for context injection)
spark_context = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
job_id = spark_context.jobId().getOrElse(None)
run_id = spark_context.runId().getOrElse(None)
notebook_path = spark_context.notebookPath().getOrElse("Unknown")