import pandas as pd

try:
    df = pd.read_excel(r'd:\BhumiDoc\Acct Statement.xls', sheet_name=0, header=None)
    print("--- First 20 rows of Column C (Index 2) from row 23 ---")
    for i in range(22, min(50, len(df))):
        val = df.iloc[i, 2]
        print(f"Row {i+1}: {val}")
except Exception as e:
    print("Error:", e)
