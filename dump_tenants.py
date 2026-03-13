import pandas as pd
import json
from sqlalchemy import create_engine

def dump_tenant_history():
    try:
        with open('db_config.json', 'r') as f:
            cfg = json.load(f)
        
        engine = create_engine(f"mysql+pymysql://{cfg['user']}:{cfg['password']}@{cfg['host']}:{cfg['port']}/{cfg['database']}")
        
        df = pd.read_sql('SELECT * FROM tenant_history', engine)
        print(df.to_string())
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    dump_tenant_history()
