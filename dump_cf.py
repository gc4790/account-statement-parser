import sqlalchemy
import json
import pandas as pd

try:
    with open('db_config.json', 'r') as f:
        cfg = json.load(f)
    
    url = f"mysql+pymysql://{cfg['user']}:{cfg['password']}@{cfg['host']}:{cfg['port']}/{cfg['database']}"
    connect_args = {"ssl": {"ssl_mode": "VERIFY_IDENTITY"}} if cfg.get('use_ssl') else {}
    engine = sqlalchemy.create_engine(url, connect_args=connect_args)
    
    with engine.connect() as conn:
        df_cf = pd.read_sql("SELECT * FROM flat_carry_forward", conn)
        print("--- All Entries in flat_carry_forward ---")
        print(df_cf.to_string())
        
        # Check total count in both tables
        ph_count = conn.execute(sqlalchemy.text("SELECT COUNT(DISTINCT `Flat Number`) FROM payment_history")).scalar()
        cf_count = conn.execute(sqlalchemy.text("SELECT COUNT(*) FROM flat_carry_forward")).scalar()
        print(f"\nDistinct flats in payment_history: {ph_count}")
        print(f"Entries in flat_carry_forward: {cf_count}")

except Exception as e:
    print(f"Error: {e}")
