import pandas as pd

pay_file = r"d:\BhumiDoc\Maintenance Details With Interest 2023-25 - latest_05_25.xlsx"
xl = pd.ExcelFile(pay_file)
all_payments = []

sheet = 'C1-1101'
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
    
    col_date = 'date'
    col_narr = 'narration' if 'narration' in df_sheet.columns else None
    col_amt = 'amount'
    
    if col_date in df_sheet.columns and col_amt in df_sheet.columns:
        temp_df = pd.DataFrame()
        temp_df['Flat Number'] = sheet.strip().upper()
        temp_df['Date'] = pd.to_datetime(df_sheet[col_date], errors='coerce').dt.date
        temp_df['Narration'] = df_sheet[col_narr].astype(str) if col_narr else ""
        temp_df['Amount'] = pd.to_numeric(df_sheet[col_amt], errors='coerce')
        
        # Keep only rows where amount is > 0 and date exists
        temp_df = temp_df[~temp_df['Date'].isna()]
        temp_df = temp_df[temp_df['Amount'] > 0]
        
        if not temp_df.empty:
            temp_df['Month'] = pd.to_datetime(temp_df['Date']).dt.strftime('%b-%Y').str.upper()
            all_payments.append(temp_df)

if len(all_payments) > 0:
    final_pay_df = pd.concat(all_payments, ignore_index=True)
    final_pay_df = final_pay_df[['Month', 'Flat Number', 'Date', 'Narration', 'Amount']]
    print(final_pay_df)
else:
    print("No payments extracted for C1-1101")
