import pandas as pd
import re

try:
    df_stmt = pd.read_excel(r'D:\BhumiDoc\Acct Statement.xls', sheet_name=0, header=None, skiprows=22)
    df_stmt = df_stmt.fillna("").astype(str)
    
    col_b_name = df_stmt.columns[1]
    col_c_name = df_stmt.columns[2]
    
    b_normalized = df_stmt[col_b_name].str.lower().str.replace(r'[\s\-]', '', regex=True)
    c_normalized = df_stmt[col_c_name].str.lower().str.replace(r'[\s\-]', '', regex=True)
    
    mask_stmt = b_normalized.str.contains('c11202') | c_normalized.str.contains('c11202')
    
    filtered_stmt = df_stmt[mask_stmt]
    
    with open(r'd:\BhumiDoc\debug_out.txt', 'w') as f:
        f.write(f"Found {len(filtered_stmt)} rows in Acct Statement.\n")
        if len(filtered_stmt) > 0:
            for idx, row in filtered_stmt.iterrows():
                f.write(f"Row {idx}:\n")
                f.write(f"  Col Date: {row[0]}\n")
                f.write(f"  Col B: {row[col_b_name]}\n")
                f.write(f"  Col C: {row[col_c_name]}\n")
            
except Exception as e:
    with open(r'd:\BhumiDoc\debug_out.txt', 'w') as f:
        f.write(f"Error: {e}")
