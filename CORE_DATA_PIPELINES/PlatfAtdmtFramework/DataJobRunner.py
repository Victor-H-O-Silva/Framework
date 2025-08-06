dbutils.widgets.removeAll()
dbutils.widgets.text("pipeline_name", "")
dbutils.widgets.text("params_json", "{}")

pipeline_name = dbutils.widgets.get("pipeline_name")
params = json.loads(dbutils.widgets.get("params_json"))

if not pipeline_name:
    raise Exception("Missing required parameter: pipeline_name")

print(f"Running pipeline: {pipeline_name}")

from DataJob import DataJob

DataJob.pipeline_name = pipeline_name
DataJob.params = params
DataJob.log_print = True

DataJob.run()
