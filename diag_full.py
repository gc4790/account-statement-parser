import sqlalchemy
import json
import os
import pandas as pd

def check():
    path = 'd:/Society_Plus/account-statement-parser/db_config.json'
    with open(path, 'r') as f:
        cfg = json.load(f)
        
    url = f"mysql+pymysql://{cfg['user']}:{cfg['password']}@{cfg['host']}:{cfg['port']}/{cfg['database']}"
    connect_args = {"ssl": {"ssl_mode": "VERIFY_IDENTITY"}} if cfg.get('use_ssl', False) else {}
    engine = sqlalchemy.create_engine(url, connect_args=connect_args)
    
    with engine.connect() as conn:
        print("--- Tables ---")
        res = conn.execute(sqlalchemy.text("SHOW TABLES")).fetchall()
        for r in res:
            print(r[0])
            
        print("\n--- flat_details Schema ---")
        try:
            df = pd.read_sql("DESCRIBE flat_details", conn)
            print(df)
            
            print("\n--- Sample data from flat_details ---")
            df_data = pd.read_sql("SELECT * FROM flat_details LIMIT 5", conn)
            print(df_data)
        except Exception as e:
            print(f"Error describing flat_details: {e}")

        print("\n--- tenant_history Schema ---")
        try:
            df_t = pd.read_sql("DESCRIBE tenant_history", conn)
            print(df_t)
            
            print("\n--- Sample data from tenant_history ---")
            df_t_data = pd.read_sql("SELECT * FROM tenant_history LIMIT 5", conn)
            print(df_t_data)
        except Exception as e:
            print(f"Error describing tenant_history: {e}")

if __name__ == "__main__":
    check()
