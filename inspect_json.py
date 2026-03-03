import pandas as pd
import json

try:
    df = pd.read_excel(r'd:\BhumiDoc\Acct Statement.xls', sheet_name=0, header=None)
    
    data = []
    # from row 23 (index 22) up to 30
    for i in range(22, min(30, len(df))):
        row = df.iloc[i].fillna("").astype(str).tolist()
        data.append({f"Row {i+1}": row[:10]}) # restrict to first 10 columns for brevity
        
    with open(r'd:\BhumiDoc\sample_output.json', 'w') as f:
        json.dump(data, f, indent=2)
    print("Done writing to sample_output.json")
except Exception as e:
    print("Error:", e)
