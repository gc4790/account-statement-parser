import pandas as pd
import re

# We don't have the full Bank_Reconciliation.xlsx in memory easily right now, but let's mock the logic based on what we saw.

def extract_references(text):
    # Extract any alphanumeric string of length 6 or more
    # But ignore common words. Actually, just alphanumeric strings that contain digits will be safer.
    # UTRs and Txn Ids usually have digits.
    # Let's extract words that have at least one digit and length >= 6
    if not isinstance(text, str):
        return []
    words = re.findall(r'\b[A-Za-z0-9]{6,}\b', text)
    # filter to those containing at least one digit
    return [w for w in words if re.search(r'\d', w)]

test_strs = [
    "UTR NO:AXNFCN0535675256\nTxn Id:4a40dd94fc2c441483ae137bbf18cacf",
    "Advance online Payment\n Txn Id: 4a40dd94fc2c441483ae137bbf18cacf",
    "NEFT-0000409262184705"
]

for t in test_strs:
    print(f"Text: '{t}' -> Refs: {extract_references(t)}")
