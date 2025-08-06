# FILE: pipelines/IncrementalPipelineTemplate.py

class IncrementalPipelineTemplate(DataPipeline):
    def __init__(self, job_id, job_name, params=None):
        super().__init__(job_id=job_id, job_name=job_name, params=params)

        # Customize per table
        self.table_name = "your_table_name"
        self.target_table = "your_catalog.your_schema." + self.table_name
        self.natural_key = ["your_key_col1", "your_key_col2"]
        self.surrogate_key = "your_surrogate_key"
        self.surrogate_len = 16
        self.excluded_columns = ["tbl_crt_dttm", "tbl_updt_dttm"]

    def run(self):
        try:
            self._log("INFO", "Starting incremental query execution")

            query = f"""
                SELECT 
                    a.your_key_col1, 
                    a.your_key_col2, 
                    b.some_field, 

                FROM your_catalog.your_schema.your_source_table a
                LEFT JOIN your_catalog.your_schema.your_lookup_table b 
                    ON a.some_id = b.some_id
            """

            df = spark.sql(query).cache()
            self._log("INFO", f"Initial row count: {df.count()}")

            self._log("INFO", "Generating surrogate key")
            df = pf.sk_generation(df, self.natural_key, self.surrogate_key, self.surrogate_len).cache()

            self._log("INFO", "Filtering new records")
            df = pf.filter_new_records(df, self.target_table, self.natural_key).cache()

            if df.isEmpty():
                self._log("INFO", "No new records to process")
                return True

            self._log("INFO", f"Merging into {self.target_table}")
            success = pf.write_merge_to_table(df, self.target_table, self.natural_key, self.excluded_columns)

            if not success:
                raise Exception("Merge to target table failed")

            self._log("INFO", f"{df.count()} rows merged into {self.target_table}")
            return True

        except Exception as e:
            self._log("ERROR", f"Unhandled exception: {e}")
            return False


# FILE: pipelines/FullPipelineTemplate.py

class FullPipelineTemplate(DataPipeline):
    def __init__(self, job_id, job_name, params=None):
        super().__init__(job_id=job_id, job_name=job_name, params=params)

        # Customize per table
        self.table_name = "your_table_name"
        self.target_table = "your_catalog.your_schema." + self.table_name
        self.natural_key = ["your_key_col1", "your_key_col2"]
        self.surrogate_key = "your_surrogate_key"
        self.surrogate_len = 16
        self.excluded_columns = ["tbl_crt_dttm", "tbl_updt_dttm"]

    def run(self):
        try:
            self._log("INFO", "Starting full query execution")

            query = f"""
                SELECT 
                    a.your_key_col1, 
                    a.your_key_col2, 
                    b.some_field, 
                FROM your_catalog.your_schema.your_source_table a
                LEFT JOIN your_catalog.your_schema.your_lookup_table b 
                    ON a.some_id = b.some_id
            """

            df = spark.sql(query).cache()

            self._log("INFO", "Generating surrogate key")
            df = pf.sk_generation(df, self.natural_key, self.surrogate_key, self.surrogate_len).cache()

            self._log("INFO", f"Overwriting into {self.target_table}")
            success = pf.write_overwrite_to_table(df, self.target_table)

            if not success:
                raise Exception("Overwrite to target table failed")

            return True

        except Exception as e:
            self._log("ERROR", f"Unhandled exception: {e}")
            return False
