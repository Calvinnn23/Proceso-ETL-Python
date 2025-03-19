from pyspark.sql import SparkSession
from extract.extractor import Extractor
from transform.transformer import Transformer
from load.loader import Loader
from observability.logger import setup_logger


def main():
    logger = setup_logger("ETL_Main")
    logger.info("Inicio del proceso ETL")

    # Inicializa Spark
    spark = SparkSession.builder.appName("ETL_Films").getOrCreate()

    # Ruta al archivo Excel
    excel_path = "data/Films.xlsx"

    # --- Extracción ---
    extractor = Extractor(excel_path)
    try:
        raw_data = extractor.extract_with_pandas()
        logger.info("Extracción completada")
    except Exception as e:
        logger.error(f"Fallo en la extracción: {e}")
        return

    # --- Transformación ---
    transformer = Transformer(raw_data)
    try:
        # Limpieza de datos con Pandas
        cleaned_data = transformer.clean_data_pandas()
        transformed_data = transformer.apply_transformations(cleaned_data)
        logger.info("Transformación completada")
    except Exception as e:
        logger.error(f"Fallo en la transformación: {e}")
        return

    # --- Carga ---
    loader = Loader(destination="data/output")
    try:
        loader.load_to_csv(transformed_data)
        logger.info("Carga completada")
    except Exception as e:
        logger.error(f"Fallo en la carga: {e}")
        return

    logger.info("Proceso ETL finalizado exitosamente")
    spark.stop()


if __name__ == "__main__":
    main()
