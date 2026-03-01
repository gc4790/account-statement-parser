import sys
try:
    import pandas as pd
    print("Pandas version:", pd.__version__)
except ImportError:
    print("Pandas not installed.")
    sys.exit(1)

try:
    # Read the file
    df = pd.read_excel(r'd:\BhumiDoc\Acct Statement.xls', sheet_name=0, header=None)
    
    # We want data from row 23 (index 22). Let's see row 22.
    print("--- Row 23 (Header candidates?) ---")
    print(df.iloc[22].values)
    
    print("\n--- Rows 24-25 (Data samples) ---")
    print(df.iloc[23:25].values)
    
    print("\n--- Identifying Column C (Index 2) ---")
    print("Row 23, Col C:", df.iloc[22, 2])
    
except Exception as e:
    print("Error reading Excel:", e)
