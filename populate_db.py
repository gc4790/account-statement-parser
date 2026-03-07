import pandas as pd
import sqlalchemy

def populate():
    print("Connecting to MySQL...")
    engine = sqlalchemy.create_engine("mysql+pymysql://root:root@localhost:3306/society_plus")
    
    file_path = r"d:\BhumiDoc\Flat Resident List.xlsx"
    print(f"Reading {file_path}...")
    df_upload = pd.read_excel(file_path, header=None)
    
    # 1. Dynamically find the header row by looking for 'Flat No'
    header_idx = None
    for i, row in df_upload.iterrows():
        if row.astype(str).str.contains(r'Flat\s*No', case=False, na=False).any():
            header_idx = i
            break
            
    if header_idx is not None:
        df_upload.columns = df_upload.iloc[header_idx]
        df_upload = df_upload.iloc[header_idx + 1:].reset_index(drop=True)
        
    # 2. Map custom columns to the database schema
    rename_map = {}
    for col in df_upload.columns:
        col_str = str(col).strip().lower()
        if 'owner' in col_str:
            rename_map[col] = 'Owner Name'
        elif 'flat no' in col_str:
            rename_map[col] = 'Flat No'
        elif 'mobile' in col_str or 'contact' in col_str:
            rename_map[col] = 'Contact Number'
        elif 'tenant' in col_str:
            rename_map[col] = 'Tenant Name'
    
    if rename_map:
        df_upload.rename(columns=rename_map, inplace=True)

    # Expected columns
    expected_cols = ["Flat No", "Owner Name", "Rented Status", "Tenant Name", "Flat Type", "Area (sq ft)", "Contact Number", "Email ID"]
    for col in expected_cols:
        if col not in df_upload.columns:
            df_upload[col] = None
            
    # Remove trailing NaNs
    df_upload.dropna(subset=['Flat No'], inplace=True)
    
    # Reorder
    df_flat = df_upload[expected_cols]
    
    print(f"Writing {len(df_flat)} rows to MySQL...")
    df_flat.to_sql(name="flat_details", con=engine, if_exists="replace", index=False)
    print("Done!")

if __name__ == "__main__":
    populate()
