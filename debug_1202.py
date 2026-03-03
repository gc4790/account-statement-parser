import pandas as pd

try:
    df_stmt = pd.read_excel(r'D:\BhumiDoc\Acct Statement.xls', sheet_name=0, header=None, skiprows=22).fillna("").astype(str)
    df_rec = pd.read_excel(r'D:\BhumiDoc\Bank_Reconciliation.xlsx', sheet_name=0, header=3).fillna("").astype(str)
    
    with open(r'd:\BhumiDoc\debug_out2.txt', 'w') as f:
        f.write("--- Bank Rec ---\n")
        mask_rec = df_rec.apply(lambda row: row.astype(str).str.contains('1202').any(), axis=1)
        res_rec = df_rec[mask_rec]
        f.write(f"Found {len(res_rec)} rows in Bank Rec containing '1202'\n")
        for i, r in res_rec.iterrows():
            f.write(str(r.to_dict())[:200] + "\n")
            
        f.write("\n--- Acct Stmt ---\n")
        mask_stmt = df_stmt.apply(lambda row: row.astype(str).str.contains('1202').any(), axis=1)
        res_stmt = df_stmt[mask_stmt]
        f.write(f"Found {len(res_stmt)} rows in Acct Stmt containing '1202'\n")
        for i, r in res_stmt.iterrows():
            f.write(str(r.to_dict())[:200] + "\n")
except Exception as e:
    print("Error:", e)
