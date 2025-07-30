class PipelineLogType(pyenum.Enum):
  INFO = pyenum.auto()
  WARNING = pyenum.auto()
  DEBUG = pyenum.auto()
  ERROR = pyenum.auto()
  FATAL_ERROR = pyenum.auto()

# COMMAND ----------

class DataPipeline():
  
  log_path = 'abfss://'+fileSystemName+'@'+storageAccountName+'.dfs.core.windows.net/log_lyr/pipeline/'
  log_print = True

  def __init__(self, **log_params):
    # log_params: pipeline_id=0, job_id=0, job_name='default_job'
    self.pipeline_id = log_params['pipeline_id'] if 'pipeline_id' in log_params else 0
    self.job_id      = log_params['job_id'] if 'job_id' in log_params else 0
    self.job_name    = log_params['job_name'] if 'job_name' in log_params else 'DefaultJob'
    self.start_time  = pydt.datetime.now()
    self.pipeline_name = self.__class__.__name__

    self._params = {}
    params_df = DataWrapper.fromQuery("SELECT * FROM default.pipeline_params WHERE pipeline = '{}'".format(self.pipeline_name))
    params = params_df.collect()
    
    for row in params:
      value = row.param_value
      if row.param_type == 'int':
        value = int(value)
      elif row.param_type == 'decimal':
        value = pydecimal.Decimal(value)
      elif row.param_type == 'datetime':
        value = pydt.datetime.strptime(value, '%Y-%m-%d %H:%M:%S.%f')
      elif row.param_type == 'date':
        value = pydt.datetime.strptime(value, '%Y-%m-%d')
        value = value.date()
      elif row.param_type == 'time':
        value = pydt.datetime.strptime(value, '%H:%M:%S.%f')
        value = value.time()  
      self._params[row.param_name] = value
      
    self._log(PipelineLogType.INFO, self.pipeline_name + '.init','Initializing Pipeline')

  def run(self):
    pass
  
  def _log(self, log_type=PipelineLogType.INFO, event=None, message=None): 
    log_date = pydt.date.today()
    log_timestamp = pydt.datetime.now()
    log_type = str(log_type.name)
    job_id = self.job_id
    job = str(self.job_name)
    pipeline_id = self.pipeline_id
    pipeline = self.pipeline_name
    pipeline_start = self.start_time
    event = str(event)
    message = str(message)
    
    self._log_filename = '{}_{}_{}.log'.format(
      pipeline.lower(), 
      str(pipeline_id), 
      pipeline_start.strftime('%Y-%m-%d_%H-%M-%S')
    )
    
    self._formatted_log_path = '{}{}/{}/{}/{}/{}/{}/{}/{}'.format(
      DataPipeline.log_path, 
      pipeline_start.strftime('%Y'), 
      pipeline_start.strftime('%m'), 
      pipeline_start.strftime('%d'),
      str(job_id), job.lower(), 
      str(pipeline_id), 
      pipeline.lower(), 
      self._log_filename
    )
    
    fields = sqltype.StructType([
      sqltype.StructField("log_timestamp",sqltype.TimestampType(), True),
      sqltype.StructField("log_type",sqltype.StringType(), True),
      sqltype.StructField("job_id",sqltype.IntegerType(), True),
      sqltype.StructField("job",sqltype.StringType(), True),
      sqltype.StructField("pipeline_id",sqltype.IntegerType(), True),
      sqltype.StructField("pipeline",sqltype.StringType(), True),
      sqltype.StructField("event",sqltype.StringType(), True),
      sqltype.StructField("message",sqltype.StringType(), True)
    ])
        
    def unbreak(string): return string.replace('\n', '; ') if string else None
    if(log_type == JobLogType.DEBUG):
      log_line_list = [(log_timestamp, log_type, job_id, job, pipeline_id, pipeline, unbreak(event), message)]
    else:
      log_line_list = [(log_timestamp, log_type, job_id, job, pipeline_id, pipeline, unbreak(event), unbreak(message))]
    
    if DataPipeline.log_print: 
      print('{} - {} - JOB ID: "{}" - JOB: "{}" - PIPELINE ID: "{}" - PIPELINE: "{}" - EVENT: "{}" - MESSAGE: "{}"'.format(str(log_timestamp), log_type, str(job_id), job, str(pipeline_id), pipeline, event, message))
