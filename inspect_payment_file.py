import pandas as pd

file_path = r"d:\BhumiDoc\Maintenance Details With Interest 2023-25 - latest_05_25.xlsx"

try:
    xl = pd.ExcelFile(file_path)
    print("Sheets available:", xl.sheet_names[:10], "... (Showing first 10)")
    
    # Read the first sheet to inspect its structure
    first_sheet = xl.sheet_names[0]
    df = pd.read_excel(file_path, sheet_name=first_sheet, nrows=20)
    print(f"\n--- Structure of sheet: '{first_sheet}' ---")
    print(df.head(15))
    
    print("\n--- Columns ---")
    print(df.columns.tolist())
    
except Exception as e:
    print(f"Error reading file: {e}")
