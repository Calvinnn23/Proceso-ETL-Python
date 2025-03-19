import sys
import os

# Agregar el directorio 'src' al sys.path
current_dir = os.path.dirname(os.path.abspath(__file__))
src_path = os.path.join(current_dir, "..", "src")
if src_path not in sys.path:
    sys.path.append(src_path)

from extract.extractor import Extractor
from transform.transformer import Transformer
import unittest
import pandas as pd


class TestETL(unittest.TestCase):
    def setUp(self):
        # Prepara un pequeño DataFrame de ejemplo para pruebas
        self.sample_data = {
            "TestSheet": pd.DataFrame(
                {"id": [1, 2, 2, 3], "value": [10, None, None, 30]}
            )
        }

    def test_clean_data(self):
        transformer = Transformer(self.sample_data)
        cleaned = transformer.clean_data_pandas()
        df_clean = cleaned["TestSheet"]
        # Verifica que se hayan eliminado duplicados y que los nulos se hayan rellenado
        self.assertEqual(len(df_clean), 3)
        self.assertFalse(df_clean.isnull().values.any())


if __name__ == "__main__":
    unittest.main()
