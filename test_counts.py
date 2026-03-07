import pandas as pd
pay_file = r'd:\BhumiDoc\Maintenance Details With Interest 2023-25 - latest_05_25.xlsx'
xl = pd.ExcelFile(pay_file)
counts = {}
for s in xl.sheet_names:
    if 'master' in s.lower() or 'total' in s.lower(): continue
    df = pd.read_excel(pay_file, sheet_name=s, header=None)
    header_idx = None
    for i, r in df.iterrows():
        rx = r.astype(str).str.lower().tolist()
        if 'date' in rx and 'amount' in rx: 
            header_idx = i
            break
    if header_idx is not None:
        df.columns = df.iloc[header_idx].astype(str).str.lower().str.strip()
        df = df.iloc[header_idx+1:]
        if 'date' in df.columns and 'amount' in df.columns:
            df['amount'] = pd.to_numeric(df['amount'], errors='coerce')
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
            df = df[~df['date'].isna() & (df['amount']>0)]
            counts[s] = len(df)
        else:
            counts[s] = 0
    else:
        counts[s] = 0
for k, v in counts.items():
    if v > 1:
        print(f"Flat {k} has {v} records.")
    elif v == 1:
        print(f"Flat {k} has 1 record.")
    else:
        print(f"Flat {k} has 0 records.")
