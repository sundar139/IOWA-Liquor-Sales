import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src.extract import extract_iowa_liquor_sales
from src.transform import transform_iowa_liquor_data

def test_transform():
    df = extract_iowa_liquor_sales("2020-01-01", "2020-01-02")
    df_t = transform_iowa_liquor_data(df)
    assert "date" in df_t.columns
    assert df_t.isnull().sum().sum() == 0

if __name__ == "__main__":
    test_transform()
    print("Transform test passed.")
