# Databricks notebook source

# 1. Pipeline parameter widget
dbutils.widgets.removeAll()
dbutils.widgets.text("pipeline_name", "")

pipeline_name = dbutils.widgets.get("pipeline_name")
print('Pipeline name:', pipeline_name)
if not pipeline_name:
    raise Exception("Please, provide a pipeline_name to run.")

# 2. (Optional) Import catalog/classes/utilities as needed
# %run ../MainClasses/catalog

# 3. Validate pipeline is enabled and fetch send_email flag
control_db = "TBD_ctrl_db"
ctrl_job_pipelines_table = f"{control_db}.ctrl_job_pipelines"

query = f"""
    SELECT pipeline_name, send_email
    FROM {ctrl_job_pipelines_table}
    WHERE pipeline_name = '{pipeline_name}' AND status = 'ENABLE'
"""
df_pipeline = spark.sql(query)
if df_pipeline.count() == 0:
    raise Exception(f"Pipeline '{pipeline_name}' is not enabled/configured.")

send_email = df_pipeline.collect()[0].send_email

# 4. Setup log path and monitoring tables
fileSystemName = "TBD_fileSystemName"
storageAccountName = "TBD_storageAccountName"
base_path = f"abfss://{fileSystemName}@{storageAccountName}.dfs.core.windows.net"

from DataPipeline import DataPipeline
DataPipeline.log_path = f"{base_path}/log_lyr/pipeline/"
DataPipeline.log_print = True

from DataJob import DataJob
DataJob.log_path = f"{base_path}/log_lyr/job/"
DataJob.log_print = True

# Monitoring tables for job/pipeline status
DataJob.job_control_database = control_db
DataJob.job_control_table = 'ctrl_job_genl'
DataJob.job_control_table_path = f"{base_path}/jobctrl_lyr/control/ctrl_job_genl/"

DataJob.pipeline_control_database = control_db
DataJob.pipeline_control_table = 'ctrl_ppln_genl'
DataJob.pipeline_control_table_path = f"{base_path}/jobctrl_lyr/control/ctrl_ppln_genl/"

# 5. Email config (controlled by table flag)
DataJob.email['send_success'] = bool(send_email)
DataJob.email['send_fail'] = bool(send_email)
DataJob.email['fail_recipients'] = ['']  # Set as needed
DataJob.email['success_recipients'] = ['']  # Set as needed
DataJob.email['pipeline_send_fail'] = bool(send_email)
DataJob.email['pipeline_send_success'] = bool(send_email)
DataJob.email['pipeline_fail_recipients'] = ['']  # Set as needed
DataJob.email['pipeline_success_recipients'] = ['']  # Set as needed
DataJob.email['application_id'] = 'TBD_appId'
DataJob.email['application_name'] = 'TBD_appName'
DataJob.email['environment'] = 'TBD_appEnv'
DataJob.email['origin'] = 'Job Control'
DataJob.email['sender'] = 'no-reply@tbd.com'
DataJob.email['smtp_host'] = 'smtp.tbd.com'
DataJob.email['smtp_port'] = '25'
DataJob.email['smtp_login'] = 'no-reply@tbd.com'
DataJob.email['smtp_password'] = '*******'

# 6. Run only the requested pipeline
DataJob.pipelines = [pipeline_name]
DataJob.pipeline_retry_threshold = 3
DataJob.ignore_dependencies = False
DataJob.name = pipeline_name

# 7. Run the job! (runs only one pipeline per invocation)
DataJob.run()
