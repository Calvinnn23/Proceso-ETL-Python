class Transformer:
    def __init__(self, data: dict):
        self.data = data

    # Metodo limpieza con pandas
    def clean_data_pandas(self) -> dict:
        cleaned_data = {}
        for sheet, df in self.data.items():
            try:
                df_clean = df.drop_duplicates()
                df_clean = df_clean.fillna("N/A")
                cleaned_data[sheet] = df_clean
            except Exception as e:
                print(f"Error limpiando datos de la hoja {sheet}: {e}")
                raise
        print("Limpieza de datos con Pandas")
        return cleaned_data

    # Metodo limpieza con spark
    def clean_data_spark(self, spark_data: dict) -> dict:
        from pyspark.sql.functions import col, when

        cleaned_data = {}
        for sheet, sdf in spark_data.items():
            try:
                sdf_clean = sdf.dropDuplicates()
                for column in sdf_clean.columns:
                    sdf_clean = sdf_clean.withColumn(
                        column, when(col(column).isNull(), "N/A").otherwise(col(column))
                    )
                cleaned_data[sheet] = sdf_clean
            except Exception as e:
                print(f"Error limpiando datos de la hoja {sheet} con Spark: {e}")
                raise
        print("Limpieza de datos con Spark")
        return cleaned_data

    # Metodo transformacion
    def apply_transformations(self, cleaned_data: dict) -> dict:
        transformed_data = {}
        for sheet, df in cleaned_data.items():
            try:
                transformed_data[sheet] = df
            except Exception as e:
                print(f"Error transformando datos de la hoja {sheet}: {e}")
                raise
        print("Transformaciones adicionales aplicadas")
        return transformed_data
