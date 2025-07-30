# Databricks notebook source
dbutils.widgets.removeAll()
dbutils.widgets.text("JobName", "")
job_name = dbutils.widgets.get("JobName")
print('Job name:',job_name)
if not job_name: raise Exception('Please, provide a job name to run.')

# COMMAND ----------

# MAGIC %run ../MainClasses/catalog

# COMMAND ----------

script = f'select parm_nm,parm_vl from TBD where job_name = "{job_name}"'
df_setup = spark.sql(script)
setup = dict(df_setup.collect())
print(setup)

# COMMAND ----------

script = f'select pipeline_name from TBD where job_name = "{job_name}" and status = "ENABLE"'
df_pipelines = spark.sql(script)
pipelines = [row.pipeline_name for row in df_pipelines.collect()]
print(pipelines)

# COMMAND ----------

script = f'select hierarchy,config_path from TBD where job_name = "{job_name}" order by 1'
df_config = spark.sql(script)
config_paths = [row.config_path for row in df_config.collect()]
print(config_paths) 

# COMMAND ----------

# Check if there is an instance of the data job running in control table. Case yes, abort the new instance. Otherwise, start a new instance.
DataJob.pipelines = pipelines
# running_status_pipelines_chk(DataJob.pipelines)

# COMMAND ----------

from databricksapi import Workspace
DOMAIN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().getOrElse(None)
TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)
ws = Workspace.Workspace(DOMAIN, TOKEN)
get_code_notebook = lambda notebook_path: ws.exportWorkspace(notebook_path, 'SOURCE', 'true').decode('UTF-8')

for config_path in config_paths:
  exec(get_code_notebook(config_path))

# COMMAND ----------

# Pipeline Log setup
base_path              = f'abfss://{fileSystemName}@{storageAccountName}.dfs.core.windows.net'
DataPipeline.log_path  = f'{base_path}/log_lyr/pipeline/'
DataPipeline.log_print = True

# Job Log setup
DataJob.log_path  = f'{base_path}/log_lyr/job/'
DataJob.log_print = True

# Job Result Email options
DataJob.email['send_fail'] = False
DataJob.email['send_success'] = False
DataJob.email['fail_recipients'] = ['']
DataJob.email['success_recipients'] = ['']

# Pipeline Result Email options
DataJob.email['pipeline_send_fail'] = False
DataJob.email['pipeline_send_success'] = False
DataJob.email['pipeline_fail_recipients'] = ['']
DataJob.email['pipeline_success_recipients'] = ['']

# Other Email config
DataJob.email['application_id'] = appId
DataJob.email['application_name'] = appName
DataJob.email['environment'] = appEnv
DataJob.email['origin'] = 'Job Control'
DataJob.email['sender'] = 'TBD'

# Email SMTP credentials and connection
DataJob.email['smtp_host'] = 'TBD'
DataJob.email['smtp_port'] = 'TBD'
DataJob.email['smtp_login'] = 'TBD'
DataJob.email['smtp_password'] = '*******'

# job setup
DataJob.name = job_name

# set path, database and table for job status monitoring
DataJob.job_control_database        = 'TBD'
DataJob.job_control_table           = 'TBD'
DataJob.job_control_table_path      = f'{base_path}/jobctrl_lyr/control/TBD/'

# set path, database and table for pipeline status monitoring
DataJob.pipeline_control_database   = 'TBD'
DataJob.pipeline_control_table      = 'TBD'
DataJob.pipeline_control_table_path = f'{base_path}/jobctrl_lyr/control/TBD/'

# set execution mode
DataJob.execution_mode = setup['EXECUTION_MODE']

# how many times should DataJob retry running a pipeline fisished with DataJobPipelineStatus.ERROR (When the pipeline returns False on the method "run()")
DataJob.pipeline_retry_threshold = int(setup['PIPELINE_RETRY_THRESHOLD'])

# set this option to True in order to run pipelines without waiting for its dependencies to finish 
# Warning: do it only if you fully understand the implications to the data model) 
# Default value: False
DataJob.ignore_dependencies = eval(setup['IGNORE_DEPENDENCIES'])

# run the job!
DataJob.run()
    
