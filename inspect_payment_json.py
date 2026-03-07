import pandas as pd
import json
import datetime

def default_json(o):
    if isinstance(o, (datetime.date, datetime.datetime)):
        return o.isoformat()
    return str(o)

file_path = r"d:\BhumiDoc\Maintenance Details With Interest 2023-25 - latest_05_25.xlsx"
xl = pd.ExcelFile(file_path)

sheets = xl.sheet_names
print(f"Total sheets: {len(sheets)}")

target_sheet = None
for s in sheets:
    if "101" in s or "201" in s or "301" in s:
        target_sheet = s
        break

if not target_sheet and len(sheets) > 1:
    target_sheet = sheets[1] 

if target_sheet:
    print(f"Inspecting sheet: {target_sheet}")
    df = pd.read_excel(file_path, sheet_name=target_sheet, nrows=20)
    
    df = df.fillna("")
    data = df.to_dict(orient="records")
    
    with open("inspect_payment.json", "w") as f:
        json.dump({"sheet": target_sheet, "columns": list(df.columns), "data": data}, f, indent=2, default=default_json)
    print("Saved to inspect_payment.json")
