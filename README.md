# Proceso ETL con Python

## Descripción

Este proyecto implementa un pipeline ETL utilizando Python y PySpark para procesar los datos contenidos en el archivo `Films.xlsx`. La solución es modular y escalable, siguiendo buenas prácticas de POO y principios SOLID.

## Entorno virtual

```bash
python -m venv venv
venv\Scripts\activate
```

## Librerias

```bash
pip install pyspark pandas openpyxl
```

## Ejecución Pipeline ETL

```bash
python src/main.py
```

Esto ejecutará el proceso de extracción, transformación y carga de datos, generando archivos limpios y procesados en la carpeta `data/output/`.

## Tests

Para ejecutar las pruebas unitarias:

```bash
python -m unittest discover tests
```

## Módulos y Funcionalidades

### Extracción (`extractor.py`)

- Carga los datos desde el archivo Excel.
- Soporta lectura con Pandas y Spark.

### Transformación (`transformer.py`)

- Limpieza de datos (eliminación de duplicados, manejo de valores nulos, validaciones).
- Aplicación de reglas de negocio y formato de datos.

### Carga (`loader.py`)

- Almacena los datos transformados en formato CSV.
- Posibilidad de expansión a bases de datos o Parquet.

### Observabilidad (`logger.py`)

- Registro de logs detallados para cada paso del pipeline ETL.
