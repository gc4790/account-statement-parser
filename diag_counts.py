import sqlalchemy
import json
import os

def check():
    path = 'd:/Society_Plus/account-statement-parser/db_config.json'
    if not os.path.exists(path):
        print("Config not found")
        return
        
    with open(path, 'r') as f:
        cfg = json.load(f)
        
    url = f"mysql+pymysql://{cfg['user']}:{cfg['password']}@{cfg['host']}:{cfg['port']}/{cfg['database']}"
    
    connect_args = {}
    if cfg.get('use_ssl', False):
        connect_args = {"ssl": {"ssl_mode": "VERIFY_IDENTITY"}}
        
    engine = sqlalchemy.create_engine(url, connect_args=connect_args)
    
    with engine.connect() as conn:
        try:
            res = conn.execute(sqlalchemy.text("SELECT COUNT(*) FROM flat_details")).scalar()
            print(f"Total flats: {res}")
            
            res_tenants = conn.execute(sqlalchemy.text("SELECT COUNT(*) FROM tenant_history")).scalar()
            print(f"Total tenant history records: {res_tenants}")
            
            # Check owner/tenant split in flat_details
            res_t = conn.execute(sqlalchemy.text("SELECT COUNT(*) FROM flat_details WHERE `Rented Status` IN ('Y', 'Yes')")).scalar()
            print(f"Tenants in flat_details: {res_t}")
            
            res_o = conn.execute(sqlalchemy.text("SELECT COUNT(*) FROM flat_details WHERE `Rented Status` NOT IN ('Y', 'Yes')")).scalar()
            print(f"Owners in flat_details: {res_o}")
            
        except Exception as e:
            print(f"Error: {e}")

if __name__ == "__main__":
    check()
