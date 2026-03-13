import os, sys, sqlalchemy, pandas as pd
sys.path.append(os.path.abspath('d:/Society_Plus/account-statement-parser'))
import app

engine = app.get_engine()
with engine.connect() as conn:
    df = pd.read_sql("SELECT * FROM flat_details LIMIT 5", conn)
    print("Columns in flat_details:")
    print(df.columns.tolist())
    print("\nSample Data:")
    print(df.head())
    
    # Also count total rows
    count = conn.execute(sqlalchemy.text("SELECT COUNT(*) FROM flat_details")).scalar()
    print("\nTotal rows in flat_details:", count)
