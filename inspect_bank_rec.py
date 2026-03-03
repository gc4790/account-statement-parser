import pandas as pd
import json

try:
    df = pd.read_excel(r'D:\BhumiDoc\Bank_Reconciliation.xlsx', sheet_name=0)
    
    # We want to see the column names and first 5 rows
    data = {"columns": df.columns.tolist(), "rows": []}
    
    for i in range(min(5, len(df))):
        row = df.iloc[i].fillna("").astype(str).tolist()
        data["rows"].append(row)
        
    with open(r'd:\BhumiDoc\bank_rec_sample.json', 'w') as f:
        json.dump(data, f, indent=2)
        
    print("Done writing to bank_rec_sample.json")
except Exception as e:
    print("Error:", e)
