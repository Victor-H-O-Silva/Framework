from delvdaas_pipeline import platfatdmt_framework_functions
import datetime as pydt
import traceback as pytrace

class TretorResptNpsHstDtlPipeline:
    def __init__(self, config):
        self.config = config

        self.type_pipeline = 'fact'
        self.table_name = 'TBD'
        self.table_final_dbrs = f'TBD.{self.table_name}'
        self.base_path = f'abfss://{self.config["fileSystemName"]}@{self.config["storageAccountName"]}.dfs.core.windows.net'
        self.table_path = f'{self.base_path}/TBD/{self.type_pipeline}/{self.table_name}/'
        self.natural_key = ['TBD']
        self.partition_cols = ['cal_yr']
        self.surrogate_key = 'tbl_srogt_cd'
        self.surrogate_len = 16
        self.domn_nm = 'TRETORRESPTNPSHSTDTLPIPELINE'
        self.exc_cols = ['TBD']

    def run(self):
        try:
            print(self.__class__.__name__ + ' - Starting Pipeline --> ' + str(pydt.datetime.now()))
            
            # 1. Source extraction (actual business logic)
            sql_query = '''
                SELECT *, YEAR(delv_date) as cal_yr
                FROM pltfatdmt_landing_db.source_my_delv
            '''

            df = platfatdmt_framework_functions.read_data_from_query(sql_query).cache()

            # 2. Data transformation as needed (example: remove columns, cast columns, etc.)
            # df = platfatdmt_framework_functions.remove_columns(df, ['unwanted_col'])

            # 3. Generate surrogate key, TBD if this is useful or not
            df = platfatdmt_framework_functions.sk_generation(df, self.natural_key, self.surrogate_key, self.surrogate_len).cache()

            # 4. Upsert/write to delta table incrementally
            result = platfatdmt_framework_functions.write_delta_incremental(
                df=df,
                table_path=self.table_path,
                natural_key=self.natural_key,
                excluded_cols=self.exc_cols
            )

            if result:
                print(self.__class__.__name__ + ' - Data upserted/written --> ' + str(pydt.datetime.now()))
                job_run_ctrl_l2 = True
            else:
                print('No new data to write. Ending the pipeline...')
                job_run_ctrl_l2 = False

            # 5. Update control table if needed, saving which table ran at what time + fail or success record
            # platfatdmt_framework_functions.updt_ctrl_tbl(self.table_final_dbrs, 'pltfatdmt', self.table_name, self.domn_nm)

            # 6. Optimize delta table (optional)
            spark.sql(f'OPTIMIZE "{self.table_path}" ZORDER BY (TBD)')

            print(self.__class__.__name__ + ' - Pipeline Finished --> ' + str(pydt.datetime.now()))

        except Exception as exception:
            message = f'{self.__class__.__name__} Failed: {exception.__class__.__name__}: {str(exception)}\nTraceback: {pytrace.format_exc()}'
            print(message)
            return False
        return True
