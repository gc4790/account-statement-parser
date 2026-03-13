import os, sys, sqlalchemy, pandas as pd
sys.path.append(os.path.abspath('d:/Society_Plus/account-statement-parser'))
import app

print("Creating dummy multi-sheet Excel file...")
# Create a dummy Flat DB
df_main = pd.DataFrame([{
    "Flat No": "A-101",
    "Owner Name": "John Doe",
    "Contact Number": "1234567890",
    "Rented Status": "Yes"
}])

# Create a dummy Tenant sheet
df_tenant = pd.DataFrame([{
    "Flat No": "A-101",
    "Tenant Name": "Jane Smith",
    "Contact": "0987654321",
    "From Date": "2024-01",
    "To Date": ""
}])

import io
excel_buffer = io.BytesIO()
with pd.ExcelWriter("test_resident_upload.xlsx") as writer:
    df_main.to_excel(writer, sheet_name="Flat DB", index=False)
    df_tenant.to_excel(writer, sheet_name="Tenant Tracker", index=False)

print("Created test_resident_upload.xlsx")
