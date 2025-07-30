class platfatdmt_framework_functions():
    @staticmethod
    def read_data_from_tables(tbl):
        """
        Reads a table from the current Spark session by name.
        
        Args:
            tbl (str): Table name in the format 'database.tablename'
        
        Returns:
            pyspark.sql.DataFrame: DataFrame with the table contents, or dummy DataFrame if error.
        
        Example:
            df = platfatdmt_framework_functions.read_data_from_tables('pltfatdmt_dwh_db.my_table')
        """
        try:
            df = spark.read.table(tbl)
        except:
            print('Problem Reading Table {0}...'.format(tbl))
            df = spark.createDataFrame([(' ')],['_c0'])
        return df

    @staticmethod
    def read_data_from_query(query):
        """
        Executes a Spark SQL query and returns the resulting DataFrame.
        
        Args:
            query (str): SQL query string
        
        Returns:
            pyspark.sql.DataFrame: DataFrame with query result, or dummy DataFrame if error.
        
        Example:
            df = platfatdmt_framework_functions.read_data_from_query('SELECT * FROM some_table')
        """
        try:
            df = spark.sql(query)
            print('Dataframe Created Successfully...')
        except:
            print('Problem Reading Query...')
            df = spark.createDataFrame([(' ')],['_c0'])
        return df

    @staticmethod
    def remove_columns(df, columns):
        """
        Removes columns from a DataFrame.
        
        Args:
            df (pyspark.sql.DataFrame): Input DataFrame
            columns (list of str): List of column names to drop
        
        Returns:
            pyspark.sql.DataFrame: DataFrame with columns removed, or dummy DataFrame if error.
        
        Example:
            df = platfatdmt_framework_functions.remove_columns(df, ['old_col1', 'old_col2'])
        """
        try:
            for co in columns:
                df = df.drop(co)
        except:
            print('Problem Removing Column {0}...'.format(co))
            df = spark.createDataFrame([(' ')],['_c0'])
        return df

    @staticmethod
    def add_columns(df, columns):
        """
        Adds new columns to a DataFrame using expressions.
        
        Args:
            df (pyspark.sql.DataFrame): Input DataFrame
            columns (list of tuple): List of (new_col_name, expression_string)
                e.g. [('new_col', "col('existing_col') + 1")]
        
        Returns:
            pyspark.sql.DataFrame: DataFrame with new columns, or dummy DataFrame if error.
        
        Example:
            df = platfatdmt_framework_functions.add_columns(df, [('calc_col', "col('val1') + col('val2')")])
        """
        try:
            for co in columns:
                df = df.withColumn(co[0], eval(co[1]))
        except:
            print('Problem Adding Column {0}...'.format(co[0]))
            df = spark.createDataFrame([(' ')],['_c0'])
        return df

    @staticmethod
    def casting_columns(df, columns, data_type):
        """
        Casts specified columns to a given data type after trimming.
        
        Args:
            df (pyspark.sql.DataFrame): Input DataFrame
            columns (list of str): List of columns to cast
            data_type (str): Data type string ('int', 'float', 'string', etc.)
        
        Returns:
            pyspark.sql.DataFrame: DataFrame with casted columns, or dummy DataFrame if error.
        
        Example:
            df = platfatdmt_framework_functions.casting_columns(df, ['amount'], 'float')
        """
        try:
            for co in columns:
                df = df.withColumn(co, trim(col(co)).cast(data_type))
        except:
            print('Problem Casting Field...')
            df = spark.createDataFrame([(' ')],['_c0'])
        return df    

    @staticmethod  
    def string_defaults(df, columns):
        """
        Fills null/empty string values for given columns with a dash ('-').
        Trims values and decodes/encodes to clean encoding issues.
        
        Args:
            df (pyspark.sql.DataFrame): Input DataFrame
            columns (list of str): Columns to clean
        
        Returns:
            pyspark.sql.DataFrame: Cleaned DataFrame, or dummy DataFrame if error.
        
        Example:
            df = platfatdmt_framework_functions.string_defaults(df, ['name', 'desc'])
        """
        try:
            for co in columns:
                df = df.na.fill('-', co)
                df = df.withColumn(co, when(trim(co)==',','-').when(trim(co)=='.','-').when(trim(co)=='','-').when(trim(co)==' ','-').otherwise(trim(co)))
                df = df.withColumn(co, trim(co))
                df = df.withColumn(co, decode(encode(col(co),'cp1252'),'utf-8'))
            print('Setting String Defaults Successfully...')
        except:
            print('Problem Setting Defaults On String Fields...')
            df = spark.createDataFrame([(' ')],['_c0'])
        return df

    @staticmethod  
    def numerical_default(df):
        """
        Fills all numeric columns with zero for null values.
        
        Args:
            df (pyspark.sql.DataFrame): Input DataFrame
        
        Returns:
            pyspark.sql.DataFrame: DataFrame with numeric nulls replaced by 0, or dummy DataFrame if error.
        
        Example:
            df = platfatdmt_framework_functions.numerical_default(df)
        """
        try:
            return df.na.fill(0)
            print('Setting Numerical Defaults Successfully...')
        except:
            print('Problem Numerical Defaults On Numeric Fields...')
            return spark.createDataFrame([(' ')],['_c0'])

    @staticmethod
    def get_only_new_records(df, table_name, table_key):
        """
        Returns new records in 'df' not found in the target table based on a primary/natural key.
        This is a left anti join for change data capture / incremental processing.
        
        Args:
            df (pyspark.sql.DataFrame): Source DataFrame
            table_name (str): Name of target table ('db.tablename')
            table_key (list of str): List of key column names
        
        Returns:
            pyspark.sql.DataFrame: DataFrame with only new records, or dummy DataFrame if error.
        
        Example:
            df_new = platfatdmt_framework_functions.get_new_records(df, 'pltfatdmt_dwh_db.target_table', ['id'])
        """
        try:
            print('Start Identifying New Records...') 
            df_cols = df.columns
            delta_data = spark.read.table(table_name)
            new_records = df.join(delta_data, on=table_key, how='left_anti').select(*df_cols)
            print('Identifying New Records Finished...')
            return new_records
        except:
            print('A Problem Occurs Identifying New Records...') 
            return spark.createDataFrame([(' ')],['_c0'])

    @staticmethod  
    def sk_generation(df, natural_key_columns, surrogate_key, truncate_length=16):
        """
        Generates a surrogate key column by hashing the concatenation of specified natural key columns.
        Uses SHA-256, encodes to base62, truncates to specified length.
        
        Args:
            df (pyspark.sql.DataFrame): DataFrame to process
            natural_key_columns (list of str): Natural key column names
            surrogate_key (str): Output column name for surrogate key
            truncate_length (int): Length of the truncated surrogate key
        
        Returns:
            pyspark.sql.DataFrame: DataFrame with surrogate key column added
        
        Example:
            df = platfatdmt_framework_functions.sk_generation(df, ['col1','col2'], 'sk_id', 14)
        """
        import pyspark.sql.functions as F

        def sha256_hash(natural_key_str):
            import hashlib
            return hashlib.sha256(natural_key_str.encode()).hexdigest()
        
        sha256_udf = F.udf(sha256_hash, StringType())
        df = df.withColumn("natural_key_str", F.concat_ws('!!@#%*', *natural_key_columns))
        df = df.withColumn("hashed_key", sha256_udf(F.col("natural_key_str")))

        def base62_encode(hex_str):
            num = int(hex_str, 16)
            base62_chars = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
            encoded = []
            while num:
                num, rem = divmod(num, 62)
                encoded.append(base62_chars[rem])
            return ''.join(reversed(encoded))[:truncate_length]

        base62_udf = F.udf(base62_encode, StringType())
        df = df.withColumn(surrogate_key, base62_udf(F.col("hashed_key")))
        df = df.drop("hashed_key", "natural_key_str")
        final_columns = [col for col in df.columns if col != surrogate_key] + [surrogate_key]
        df = df.select(*final_columns)
        return df

    @staticmethod
    def write_delta(df, mod, table_name, table_path):
        """
        Writes a DataFrame to a Delta table with specified mode and path.
        Use 'overwrite' mode for full load, or 'append' to add to existing table.
        
        Args:
            df (pyspark.sql.DataFrame): DataFrame to write
            mod (str): Write mode ('overwrite', 'append')
            table_name (str): Delta table name ('db.tablename')
            table_path (str): DBFS or ADLS path for Delta table
        
        Returns:
            bool: True if successful, False if error
        
        Example:
            platfatdmt_framework_functions.write_delta(
                df, 'overwrite', 'pltfatdmt_dwh_db.my_table', '/mnt/pltfatdmtdb/my_table/'
            )
        """
        try:
            for co in spark.table(table_name).schema:
                df = df.withMetadata(co.name, co.metadata)
            df.write.format('delta').mode(mod).option('path', table_path).option('overwriteSchema', "true").saveAsTable(table_name)
            print('Delta Table Wrote Successfully....')
            return True
        except Exception as err:
            print('An Error Occurs When Writing Delta Table....')
            print(f"ERROR: {err}")
            return False   

    @staticmethod
    def write_delta_incremental(df, table_path, natural_key, excluded_cols):
        """
        Incremental upsert (merge) of a DataFrame into a Delta table using a natural key.
        Updates non-key columns, inserts new records.
        
        Args:
            df (pyspark.sql.DataFrame): DataFrame to upsert
            table_path (str): Delta table path
            natural_key (list of str): List of key columns
            excluded_cols (list of str): Columns to exclude from update
        
        Returns:
            bool: True if success, False if error
        
        Example:
            platfatdmt_framework_functions.write_delta_incremental(
                df, '/mnt/pltfatdmtdb/my_table/', ['id'], ['dt_create']
            )
        """
        try:  
            deltaTable = DeltaTable.forPath(spark, table_path)
            match_condition = ' AND '.join(['fct.' + key + ' = ' + 'updates.' + key for key in natural_key])
            update_condition = ' OR '.join([
                'fct.' + column + ' != ' + 'updates.' + column 
                for column in df.columns if (column not in natural_key) & (column not in excluded_cols)
            ])
            deltaTable.alias("fct").merge(df.alias("updates"), match_condition) \
                .whenMatchedUpdate(
                    set={col: 'updates.' + col for col in df.columns if (col not in natural_key) & (col not in excluded_cols)},
                    condition=update_condition
                ) \
                .whenNotMatchedInsert(
                    values={col: 'updates.' + col for col in df.columns}
                ).execute()            
            print('Delta Table Upsert Success...')
            return True
        except:
            print('A Problem Occurs When Upserting On Delta Table ...')
            return False
