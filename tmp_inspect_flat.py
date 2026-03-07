import pandas as pd
import sys

try:
    file_path = r"d:\BhumiDoc\Flat Resident List-c wing.xlsx"
    df = pd.read_excel(file_path)
    print("COLUMNS:")
    print(df.columns.tolist())
    print("\nFIRST 5 ROWS:")
    print(df.head())
except Exception as e:
    print(f"Error: {e}")
