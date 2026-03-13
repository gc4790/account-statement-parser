import os, sys, sqlalchemy, pandas as pd
sys.path.append(os.path.abspath('d:/Society_Plus/account-statement-parser'))
import app

engine = app.get_engine()

print("Simulating bulk upload extraction...")
all_sheets = pd.read_excel('test_resident_upload.xlsx', header=None, sheet_name=None)
sheet_names = list(all_sheets.keys())

# 1. Main Sheet
df_upload = all_sheets[sheet_names[0]]
df_upload.columns = df_upload.iloc[0]
df_flat = df_upload.iloc[1:].reset_index(drop=True)

# 2. Tenant Sheet
tenant_sheet_name = next((s for s in sheet_names if 'tenant' in s.lower()), None)
df_t = pd.DataFrame()
if tenant_sheet_name:
    df_t_raw = all_sheets[tenant_sheet_name]
    df_t_raw.columns = df_t_raw.iloc[0]
    df_t = df_t_raw.iloc[1:].reset_index(drop=True)
    df_t.columns = [str(c).strip().title() for c in df_t.columns]

print(f"Parsed {len(df_flat)} Flats and {len(df_t)} Tenants")

print("Simulating logic to save to database...")
try:
    with engine.begin() as conn:
        conn.execute(sqlalchemy.text("""
            CREATE TABLE IF NOT EXISTS tenant_history (
                id INT AUTO_INCREMENT PRIMARY KEY,
                flat_no VARCHAR(50),
                tenant_name VARCHAR(255),
                contact VARCHAR(50),
                from_date VARCHAR(20),
                to_date VARCHAR(20),
                rent_agreement_provided VARCHAR(10) DEFAULT 'No'
            )
        """))
        
        # Insert Tenant
        for _, row in df_t.iterrows():
            conn.execute(sqlalchemy.text("""
                INSERT INTO tenant_history (flat_no, tenant_name, contact, from_date, to_date, rent_agreement_provided)
                VALUES (:f, :n, :c, :fd, :td, :ra)
            """), {
                "f": row["Flat No"],
                "n": row["Tenant Name"],
                "c": row["Contact"],
                "fd": row["From Date"],
                "td": str(row["To Date"]) if pd.notna(row["To Date"]) else None,
                "ra": "No"
            })
            
    # Verify
    with engine.connect() as conn:
        res = conn.execute(sqlalchemy.text("SELECT * FROM tenant_history WHERE flat_no='A-101'")).fetchall()
        print("Records in DB:", res)
        
except Exception as e:
    print("Error:", e)
