import sqlalchemy
import json
import os
import sys

try:
    with open('db_config.json', 'r') as f:
        cfg = json.load(f)
    
    url = f"mysql+pymysql://{cfg['user']}:{cfg['password']}@{cfg['host']}:{cfg['port']}/{cfg['database']}"
    
    # Connect with SSL if needed
    connect_args = {}
    if cfg.get('use_ssl'):
        connect_args = {"ssl": {"ssl_mode": "VERIFY_IDENTITY"}}
    
    engine = sqlalchemy.create_engine(url, connect_args=connect_args)
    
    with engine.connect() as conn:
        res = conn.execute(sqlalchemy.text("SELECT 1")).scalar()
        print(f"Connection successful: {res == 1}")
        
    # Check if flat_details table exists
    with engine.connect() as conn:
        res = conn.execute(sqlalchemy.text("SHOW TABLES LIKE 'flat_details'")).fetchone()
        print(f"Table 'flat_details' exists: {res is not None}")

except Exception as e:
    print(f"Error: {e}")
    sys.exit(1)
