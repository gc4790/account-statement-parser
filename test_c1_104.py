import pandas as pd
import sys

try:
    file_path = r"d:\BhumiDoc\Maintenance Details With Interest 2023-25 - latest_05_25.xlsx"
    df = pd.read_excel(file_path, sheet_name='C1-1101', header=None)
    
    print("--- FIRST 5 ROWS ---")
    print(df.head())
    
    print("--- LAST 20 ROWS ---")
    print(df.tail(20).to_string())
except Exception as e:
    print(f"Error: {e}")
