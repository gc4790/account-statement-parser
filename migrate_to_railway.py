"""
migrate_to_railway.py
Copies all tables from local MySQL (society_plus) to Railway MySQL (railway).
Run: python migrate_to_railway.py
"""
import pandas as pd
import sqlalchemy
import json, os

# ── Load local DB config from db_config.json ──────────────────────────────────
_cfg = {}
if os.path.exists("db_config.json"):
    with open("db_config.json") as f:
        _cfg = json.load(f)

LOCAL_HOST = _cfg.get("host", "localhost")
LOCAL_PORT = int(_cfg.get("port", 3306))
LOCAL_USER = _cfg.get("user", "root")
LOCAL_PASS = _cfg.get("password", "root")
LOCAL_DB   = _cfg.get("database", "society_plus")

# ── Railway DB config ─────────────────────────────────────────────────────────
RLW_HOST = "crossover.proxy.rlwy.net"
RLW_PORT = 12345             # ← replace with your Railway public port
RLW_USER = "root"
RLW_PASS = "xcYZqdYHVZqkyFHSyeiuRaKEyGRJgwiz"
RLW_DB   = "railway"

# ─────────────────────────────────────────────────────────────────────────────

def migrate():
    local_engine  = sqlalchemy.create_engine(
        f"mysql+pymysql://{LOCAL_USER}:{LOCAL_PASS}@{LOCAL_HOST}:{LOCAL_PORT}/{LOCAL_DB}"
    )
    rlw_engine = sqlalchemy.create_engine(
        f"mysql+pymysql://{RLW_USER}:{RLW_PASS}@{RLW_HOST}:{RLW_PORT}/{RLW_DB}"
    )

    with local_engine.connect() as conn:
        tables = [row[0] for row in conn.execute(sqlalchemy.text("SHOW TABLES")).fetchall()]

    print(f"Found {len(tables)} table(s) in local DB: {tables}")

    for table in tables:
        print(f"  Migrating '{table}'...", end=" ", flush=True)
        try:
            df = pd.read_sql_table(table, local_engine)
            df.to_sql(name=table, con=rlw_engine, if_exists="replace", index=False)
            print(f"✅ {len(df)} rows")
        except Exception as e:
            print(f"❌ Failed: {e}")

    print("\nMigration complete!")

if __name__ == "__main__":
    migrate()
