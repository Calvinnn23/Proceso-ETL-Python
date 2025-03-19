import pandas as pd
from pyspark.sql import SparkSession


class Extractor:
    def __init__(self, file_path: str):
        self.file_path = file_path

    # Metodo extraccion con pandas
    def extract_with_pandas(self) -> dict:
        try:
            # Lee todas las hojas del archivo
            data = pd.read_excel(self.file_path, sheet_name=None, engine="openpyxl")
            print("Extracción con Pandas")
            return data
        except Exception as e:
            print(f"Error al extraer datos con Pandas: {e}")
            raise

    # Metodo extraccion con spark
    def extract_with_spark(self, spark: SparkSession) -> dict:
        data_dict = {}
        sheets = pd.ExcelFile(self.file_path, engine="openpyxl").sheet_names
        for sheet in sheets:
            try:
                # Convertir cada hoja a CSV temporal
                temp_csv = f"data/{sheet}.csv"
                df = pd.read_excel(self.file_path, sheet_name=sheet, engine="openpyxl")
                df.to_csv(temp_csv, index=False)
                spark_df = spark.read.csv(temp_csv, header=True, inferSchema=True)
                data_dict[sheet] = spark_df
            except Exception as e:
                print(f"Error al extraer la hoja {sheet} con Spark: {e}")
                raise
        print("Extracción con Spark")
        return data_dict
