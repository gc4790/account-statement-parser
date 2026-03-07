import pandas as pd

file_path = r"d:\BhumiDoc\Maintenance Details With Interest 2023-25 - latest_05_25.xlsx"
xl = pd.ExcelFile(file_path)
print("Sheets matching 1101:", [s for s in xl.sheet_names if '1101' in s])

if 'C1-1101' in xl.sheet_names:
    df = pd.read_excel(file_path, sheet_name='C1-1101', header=None)
    print("\n--- C1-1101 First 20 Rows ---")
    print(df.head(20).to_string())
    
    print("\n--- C1-1101 Rows 15-30 ---")
    print(df.iloc[15:30].to_string())
else:
    print("C1-1101 not found.")
