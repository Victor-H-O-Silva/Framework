class platfatdmt_framework_functions:

    @staticmethod
    def sk_generation(df, natural_key_columns, surrogate_key, truncate_length=16):
        try:
            def base62_encode(hex_str):
                num = int(hex_str, 16)
                base62_chars = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
                encoded = []
                while num:
                    num, rem = divmod(num, 62)
                    encoded.append(base62_chars[rem])
                return ''.join(reversed(encoded))[:truncate_length]

            base62_udf = F.udf(base62_encode, StringType())

            df = df.withColumn("natural_key_str", concat_ws('!!@#%*', *natural_key_columns))
            df = df.withColumn("hashed_key", sha2("natural_key_str", 256))
            df = df.withColumn(surrogate_key, base62_udf(F.col("hashed_key")))
            df = df.drop("hashed_key", "natural_key_str")
            final_columns = [c for c in df.columns if c != surrogate_key] + [surrogate_key]
            return df.select(*final_columns)
        except Exception as e:
            print(f"Error in sk_generation: {e}")
            raise

    @staticmethod
    def remove_columns(df, columns):
        try:
            for co in columns:
                df = df.drop(co)
            return df
        except Exception as e:
            print(f"Error in remove_columns: {e}")
            raise

    @staticmethod
    def filter_new_records(df, table_name, table_key):
        try:
            print('Checking for new records...')
            existing = spark.table(table_name)
            return df.join(existing, on=table_key, how='left_anti')
        except Exception as e:
            print(f"Error in filter_new_records: {e}")
            raise

    @staticmethod
    def write_merge_to_table(df, table_name, natural_key, excluded_cols):
        try:
            delta_table = DeltaTable.forName(spark, table_name)
            match_condition = ' AND '.join([f"target.{k} = source.{k}" for k in natural_key])
            update_expr = {c: expr(f"source.{c}") for c in df.columns if c not in natural_key and c not in excluded_cols}
            insert_expr = {c: expr(f"source.{c}") for c in df.columns}
            update_condition = ' OR '.join([
                f"target.{c} != source.{c}" for c in df.columns if c not in natural_key and c not in excluded_cols
            ])

            delta_table.alias("target").merge(
                df.alias("source"), match_condition
            ).whenMatchedUpdate(
                set=update_expr,
                condition=update_condition
            ).whenNotMatchedInsert(
                values=insert_expr
            ).execute()

            print("Merge completed successfully.")
            return True
        except Exception as e:
            print(f"Error in write_merge_to_table: {e}")
            raise

    @staticmethod
    def write_overwrite_to_table(df, table_name):
        try:
            df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)
            print(f"Overwrite to table {table_name} completed successfully.")
            return True
        except Exception as e:
            print(f"Error in write_overwrite_to_table: {e}")
            raise
