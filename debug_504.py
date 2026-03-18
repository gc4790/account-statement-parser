import sqlalchemy
import json
import pandas as pd
import sys

try:
    with open('db_config.json', 'r') as f:
        cfg = json.load(f)
    
    url = f"mysql+pymysql://{cfg['user']}:{cfg['password']}@{cfg['host']}:{cfg['port']}/{cfg['database']}"
    connect_args = {"ssl": {"ssl_mode": "VERIFY_IDENTITY"}} if cfg.get('use_ssl') else {}
    engine = sqlalchemy.create_engine(url, connect_args=connect_args)
    
    flat_no = 'C1-504'
    
    print(f"--- Checking data for {flat_no} ---")
    
    with open('debug_504_output.txt', 'w') as out_f:
        with engine.connect() as conn:
            # 1. Check flat_carry_forward table
            cf_rows = conn.execute(sqlalchemy.text("SELECT * FROM flat_carry_forward WHERE `Flat Number` = :f"), {"f": flat_no}).fetchall()
            out_f.write(f"\n[flat_carry_forward] table entries for {flat_no}:\n")
            for row in cf_rows:
                out_f.write(f"  - {row}\n")
            
            # 2. Check payment_history table (All records)
            ph_all = pd.read_sql(sqlalchemy.text("SELECT id, `Flat Number`, Amount, Date, Outstanding FROM payment_history WHERE `Flat Number` = :f ORDER BY `Date` ASC, id ASC"), conn, params={"f": flat_no})
            out_f.write(f"\n[payment_history] All records for {flat_no}:\n")
            if not ph_all.empty:
                out_f.write(ph_all.to_string() + "\n")
            else:
                out_f.write("  No records found in payment_history\n")

            # 3. Check what app.py would pick
            out_f.write("\n--- App Logic Simulation ---\n")
            _cf = conn.execute(sqlalchemy.text("SELECT `Outstanding` FROM flat_carry_forward WHERE `Flat Number` = :f"), {"f": flat_no}).scalar()
            if _cf is not None:
                out_f.write(f"Found in flat_carry_forward: {float(_cf)}\n")
            else:
                out_f.write("Not found in flat_carry_forward, checking payment_history...\n")
                if not ph_all.empty:
                    out_f.write(f"Fallback to payment_history (Outstanding): {float(ph_all.iloc[0]['Outstanding'])}\n")
                else:
                    out_f.write("No fallback found, default to 0.0\n")
            
            # Check flat_details
            fd = conn.execute(sqlalchemy.text("SELECT * FROM flat_details WHERE `Flat No` = :f"), {"f": flat_no}).fetchone()
            out_f.write(f"\nflat_details entry: {fd}\n")

    print("Done. Output written to debug_504_output.txt")

except Exception as e:
    print(f"Error: {e}")
