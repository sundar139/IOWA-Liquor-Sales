import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src.extract import extract_iowa_liquor_sales
from src.transform import transform_iowa_liquor_data
from src.load import load_to_postgres, get_pg_conn

def test_load():
    df = extract_iowa_liquor_sales("2020-01-01", "2020-01-02")
    df_t = transform_iowa_liquor_data(df)
    load_to_postgres(df_t)
    conn = get_pg_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM iowa_liquor_sales;")
    count = cursor.fetchone()[0]
    assert count > 0
    cursor.close()
    conn.close()

if __name__ == "__main__":
    test_load()
    print("Load test passed.")
