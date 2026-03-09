import json, sqlalchemy

with open("db_config.json") as f:
    c = json.load(f)

url = "mysql+pymysql://{}:{}@{}:{}/{}".format(
    c["user"], c["password"], c["host"], c["port"], c["database"]
)
engine = sqlalchemy.create_engine(url, connect_args={"ssl": {"ssl_mode": "VERIFY_IDENTITY"}} if c.get("use_ssl") else {})

with engine.connect() as conn:
    print("Users:", conn.execute(sqlalchemy.text("SELECT username, role, password_hash FROM app_users")).fetchall())
