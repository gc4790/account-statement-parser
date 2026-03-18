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
        df_ph = pd.read_sql("""
            SELECT `Flat Number`, `Outstanding`, `Date` 
            FROM payment_history 
            WHERE id IN (SELECT MIN(id) FROM payment_history GROUP BY `Flat Number`)
        """, conn)
        
        merged = pd.merge(df_cf, df_ph, left_on='Flat Number', right_on='Flat Number', suffixes=('_cf', '_ph'))
        
        diff = merged[merged['Outstanding_cf'] != merged['Outstanding_ph']]
        
        print(f"Total flats with entries in both: {len(merged)}")
        print(f"Flats with discrepancies: {len(diff)}")
        
        if not diff.empty:
            print("\nDiscrepancies (Top 10):")
            print(diff[['Flat Number', 'Outstanding_cf', 'Outstanding_ph', 'Date']].head(10))

except Exception as e:
    print(f"Error: {e}")
