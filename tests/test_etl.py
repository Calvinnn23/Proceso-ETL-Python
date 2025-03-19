import sys
import os

# Agrega directorio "src" al sys.path
current_dir = os.path.dirname(os.path.abspath(__file__))
src_path = os.path.join(current_dir, "..", "src")
if src_path not in sys.path:
    sys.path.append(src_path)

from extract.extractor import Extractor
from transform.transformer import Transformer
import unittest
import pandas as pd


class TestETL(unittest.TestCase):
    # Prueba la limpieza de datos con Pandas
    def setUp(self):
        self.sample_data = {
            "TestSheet": pd.DataFrame(
                {"id": [1, 2, 2, 3], "value": [10, None, None, 30]}
            )
        }

    def test_clean_data(self):
        transformer = Transformer(self.sample_data)
        cleaned = transformer.clean_data_pandas()
        df_clean = cleaned["TestSheet"]
        self.assertEqual(len(df_clean), 3)
        self.assertFalse(df_clean.isnull().values.any())


if __name__ == "__main__":
    unittest.main()
