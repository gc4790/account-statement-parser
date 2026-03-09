import json, sqlalchemy as _sa, traceback

def get_engine():
    with open("db_config.json") as f:
        _c = json.load(f)
    _url = f"mysql+pymysql://{_c['user']}:{_c['password']}@{_c['host']}:{_c['port']}/{_c['database']}"
    if _c.get("use_ssl", False):
        return _sa.create_engine(_url, connect_args={"ssl": {"ssl_mode": "VERIFY_IDENTITY"}})
    return _sa.create_engine(_url)

engine = get_engine()
try:
    with engine.connect() as conn:
        conn.execute(_sa.text("""
            CREATE TABLE IF NOT EXISTS app_users (
                username VARCHAR(50) PRIMARY KEY,
                password_hash VARCHAR(255) NOT NULL,
                role VARCHAR(20) NOT NULL
            )
        """))
        res = conn.execute(_sa.text("SELECT COUNT(*) FROM app_users")).scalar()
        print("Initial users count:", res)
        if res == 0:
            from passlib.hash import pbkdf2_sha256
            default_hash = pbkdf2_sha256.hash("admin123")
            print("Hash:", default_hash)
            conn.execute(_sa.text("INSERT INTO app_users (username, password_hash, role) VALUES ('admin', :hash, 'admin')"), {"hash": default_hash})
            print("Insert executed, now committing...")
            conn.commit()
            print("Committed successfully.")
except Exception as e:
    traceback.print_exc()
