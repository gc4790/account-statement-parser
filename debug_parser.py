import pandas as pd

pay_file = r"d:\BhumiDoc\Maintenance Details With Interest 2023-25 - latest_05_25.xlsx"
xl = pd.ExcelFile(pay_file)
all_payments = []
valid_months = ['jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec']

for sheet in xl.sheet_names[:10]: # Check first 10 sheets
    if "master" in sheet.lower() or "total" in sheet.lower():
        continue
        
    df_sheet = pd.read_excel(xl, sheet_name=sheet, header=None)
    header_idx = None
    for i, row in df_sheet.iterrows():
        row_str = row.astype(str).str.lower().tolist()
        if 'date' in row_str and 'amount' in row_str:
            header_idx = i
            break
            
    if header_idx is not None:
        df_sheet.columns = df_sheet.iloc[header_idx].astype(str).str.lower().str.strip()
        df_sheet = df_sheet.iloc[header_idx + 1:].reset_index(drop=True)
        
        col_month = 'column 1' if 'column 1' in df_sheet.columns else None
        col_date = 'date'
        col_narr = 'narration' if 'narration' in df_sheet.columns else None
        col_amt = 'amount'
        
        if col_date in df_sheet.columns and col_amt in df_sheet.columns:
            temp_df = pd.DataFrame()
            temp_df['Flat No'] = sheet.strip().upper()
            temp_df['Payment Month'] = df_sheet[col_month].astype(str).str.lower().str.strip() if col_month else ""
            temp_df['Payment Date'] = pd.to_datetime(df_sheet[col_date], errors='coerce').dt.date
            temp_df['Narration'] = df_sheet[col_narr].astype(str) if col_narr else ""
            temp_df['Amount Paid'] = pd.to_numeric(df_sheet[col_amt], errors='coerce')
            
            temp_df = temp_df[temp_df['Payment Month'].isin(valid_months)]
            temp_df = temp_df[temp_df['Amount Paid'] > 0]
            
            if not temp_df.empty:
                all_payments.append(temp_df)

if len(all_payments) > 0:
    final_pay_df = pd.concat(all_payments, ignore_index=True)
    print(final_pay_df)
else:
    print("No payments found in first 10 sheets")
