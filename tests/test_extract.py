import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src.extract import extract_iowa_liquor_sales

def test_extract():
    df = extract_iowa_liquor_sales("2020-01-01", "2020-01-02")
    assert not df.empty
    assert "invoice_line_no" in df.columns

if __name__ == "__main__":
    test_extract()
    print("Extract test passed.")
