import os, sys, sqlalchemy, pandas as pd
sys.path.append(os.path.abspath('d:/Society_Plus/account-statement-parser'))
import app

engine = app.get_engine()

print("Initializing DB...")
app.init_auth_db()

print("Testing save...")
with engine.begin() as _conn:
    _conn.execute(sqlalchemy.text("""
        INSERT INTO society_expenses 
        (`Date`, `Category`, `Description`, `Amount`, `Narration`)
        VALUES (:date, :cat, :desc, :amount, :narr)
    """), {
        "date": "2024-04-15",
        "cat": "Security",
        "desc": "April Security Salary",
        "amount": 15000.0,
        "narr": "NEFT TRANSFER TO AB SECURE"
    })

print("Verifying save...")
df = pd.read_sql("SELECT * FROM society_expenses", engine)
print(df)
