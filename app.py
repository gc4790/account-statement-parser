import streamlit as st
import pandas as pd
import os

# --- Page Config & Styling ---
st.set_page_config(
    page_title="Account Statement Parser",
    page_icon="🧾",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# Premium Custom CSS
st.markdown("""
<style>
    /* Main background */
    .stApp {
        background-color: #0e1117;
    }
    
    /* Header styling */
    h1 {
        color: #fca311;
        font-family: 'Inter', sans-serif;
        font-weight: 700;
        text-shadow: 0px 4px 10px rgba(0,0,0,0.5);
    }
    
    /* Input box styling */
    .stTextInput > div > div > input {
        border-radius: 10px;
        border: 2px solid #3a3f58;
        background-color: #1a1e2b;
        color: #ffffff;
        padding: 10px 15px;
        font-size: 16px;
        transition: all 0.3s ease;
    }
    .stTextInput > div > div > input:focus {
        border-color: #fca311;
        box-shadow: 0 0 10px rgba(252, 163, 17, 0.4);
    }
    
    /* Premium Dataframe / Table container */
    [data-testid="stDataFrame"] {
        border-radius: 12px;
        overflow: hidden;
        box-shadow: 0 8px 32px rgba(0,0,0,0.3);
        border: 1px solid rgba(255,255,255,0.05);
    }
    
    /* Info text styling */
    .info-text {
        font-size: 1.1rem;
        color: #a0aec0;
        margin-bottom: 20px;
    }
    
    /* Glassmorphism metric cards */
    [data-testid="stMetric"] {
        background: rgba(26, 30, 43, 0.6);
        backdrop-filter: blur(10px);
        -webkit-backdrop-filter: blur(10px);
        border-radius: 15px;
        padding: 20px;
        border: 1px solid rgba(255, 255, 255, 0.05);
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
        transition: transform 0.2s;
    }
    [data-testid="stMetric"]:hover {
        transform: translateY(-5px);
    }
    [data-testid="stMetricValue"] {
        color: #00f0ff;
    }
</style>
""", unsafe_allow_html=True)


st.title("🧾 Account Statement Parser")
st.markdown('<p class="info-text">Search for transactions by Flat No., Owner Name, or Reference ID</p>', unsafe_allow_html=True)


@st.cache_data
def load_statement_data(file_source):
    try:
        # Load excel skipping the first 22 rows (header stuff)
        df = pd.read_excel(file_source, sheet_name=0, header=None, skiprows=22)
        
        column_mapping = {
            0: "Txn Date",
            1: "Narration (Col B)",
            2: "Ref No (Col C)",
            3: "Value Date",
            4: "Withdrawal",
            5: "Deposit",
            6: "Closing Balance"
        }
        df = df.rename(columns=column_mapping)
        df = df.fillna("")
        for col in df.columns:
            df[col] = df[col].astype(str)
        return df
    except Exception as e:
        st.error(f"Error parsing Account Statement: {e}")
        return None

@st.cache_data
def load_bank_rec_data(file_source):
    try:
        # Load Bank Reconciliation, headers are on row 4 (index 3)
        df = pd.read_excel(file_source, sheet_name=0, header=3)
        df = df.fillna("")
        for col in df.columns:
            df[col] = df[col].astype(str)
        return df
    except Exception as e:
        st.error(f"Error parsing Bank Reconciliation: {e}")
        return None

# --- Navigation ---
st.sidebar.markdown("## 🧭 Navigation")
app_mode = st.sidebar.radio("Select View:", ["🔍 Transaction Search", "🏢 Flat Management"])

st.sidebar.markdown("---")
st.sidebar.markdown("## 💾 Database Sync")
st.sidebar.markdown("Store your parsed records permanently in MySQL.")

with st.sidebar.expander("MySQL Connection Settings", expanded=False):
    db_host = st.text_input("Host", value="localhost")
    db_port = st.text_input("Port", value="3306")
    db_user = st.text_input("Username", value="root")
    db_pass = st.text_input("Password", value="root", type="password")
    db_name = st.text_input("Database Name", value="society_plus")
    
    if st.button("🔄 Sync to MySQL", use_container_width=True):
        if "df_stmt" not in st.session_state or st.session_state.df_stmt is None or len(st.session_state.df_stmt) == 0:
            st.error("⚠️ No Account Statement data loaded to sync.")
        elif "df_rec" not in st.session_state or st.session_state.df_rec is None or len(st.session_state.df_rec) == 0:
            st.error("⚠️ No Bank Reconciliation data loaded to sync.")
        else:
            try:
                import sqlalchemy
                # create database if not exists
                temp_engine = sqlalchemy.create_engine(f"mysql+pymysql://{db_user}:{db_pass}@{db_host}:{db_port}")
                with temp_engine.connect() as conn:
                    conn.execute(sqlalchemy.text(f"CREATE DATABASE IF NOT EXISTS {db_name}"))
                
                # connect to the specific database
                engine = sqlalchemy.create_engine(f"mysql+pymysql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}")
                
                with st.spinner("Syncing data to MySQL..."):
                    # Create copies for SQL to avoid mutating the UI dataframes
                    sql_df_stmt = st.session_state.df_stmt.copy()
                    sql_df_rec = st.session_state.df_rec.copy()
                    
                    import re
                    def sanitize_col(col_name):
                        name = str(col_name).strip()
                        if len(name) > 60: name = name[:60]  # MySQL limit is 64 chars
                        name = re.sub(r'[^a-zA-Z0-9]', '_', name) # Replace weird chars with underscore
                        return name.strip('_')

                    sql_df_stmt.columns = [sanitize_col(c) for c in sql_df_stmt.columns]
                    sql_df_rec.columns = [sanitize_col(c) for c in sql_df_rec.columns]
                    
                    sql_df_stmt.to_sql(name="account_statement", con=engine, if_exists="replace", index=False)
                    sql_df_rec.to_sql(name="bank_reconciliation", con=engine, if_exists="replace", index=False)
                
                st.success(f"✅ Successfully synced tables to `{db_name}`.")
            except Exception as e:
                st.error(f"❌ Database Error: {e}")

if "df_stmt" not in st.session_state:
    st.session_state.df_stmt = None
if "df_rec" not in st.session_state:
    st.session_state.df_rec = None

# Show file uploader only in Transaction Search mode OR if files aren't loaded yet
if app_mode == "🔍 Transaction Search" or st.session_state.df_stmt is None or st.session_state.df_rec is None:
    # --- File Upload ---
    if st.session_state.df_stmt is not None:
        st.success("✅ Files are loaded in memory. Upload new files below if you need to update them.")
        
    col_up1, col_up2 = st.columns(2)
    with col_up1:
        stmt_file = st.file_uploader("📥 Upload Account Statement (.xls)", type=["xls", "xlsx"], key="stmt")
    with col_up2:
        rec_file = st.file_uploader("📥 Upload Bank Reconciliation (.xlsx)", type=["xls", "xlsx"], key="rec")

    if stmt_file is not None and rec_file is not None:
        st.session_state.df_stmt = load_statement_data(stmt_file)
        st.session_state.df_rec = load_bank_rec_data(rec_file)

if st.session_state.df_stmt is None or st.session_state.df_rec is None:
    st.info("👆 Please upload **BOTH** the Account Statement AND the Bank Reconciliation files to continue.")
    st.stop()

df_stmt = st.session_state.df_stmt
df_rec = st.session_state.df_rec

import re

if app_mode == "🔍 Transaction Search":
    # --- Search & Filter Interface ---
    st.markdown("### 🔍 Search & Filter")
    col1, col2, col3 = st.columns([2, 1, 1])

    with col1:
        search_query = st.text_input("Search Text", placeholder="e.g. C1 301, MEGHA, or UTR number...")

    with col2:
        search_col_opt = st.selectbox("Search Column", ["Both Col B & C", "Only Col B (Narration)", "Only Col C (Ref No)"])

    with col3:
        date_filter = st.date_input("Filter by Date Range", value=[])
else:
    st.markdown("### 🏢 Flat Account Management")
    # Extract unique flats securely
    flats = set()
    col_b_name = "Narration (Col B)" if "Narration (Col B)" in df_stmt.columns else df_stmt.columns[1]
    for val in df_stmt[col_b_name].dropna():
        matches = re.findall(r'\b[A-Za-z]\d[\s\-]\d{3,4}\b', str(val))
        for m in matches: flats.add(m.replace(' ', '-').upper())
        
    for col in df_rec.columns:
        if 'doc' in col.lower() or 'desc' in col.lower() or 'unnamed' in col.lower():
            for val in df_rec[col].dropna():
                matches = re.findall(r'\b[A-Za-z]\d[\s\-]\d{3,4}\b', str(val))
                for m in matches: flats.add(m.replace(' ', '-').upper())
            
    sorted_flats = ["Select a Flat..."] + sorted(list(flats))
    
    col1, col3 = st.columns([3, 1])
    with col1:
        selected_flat = st.selectbox("Select Flat Number", sorted_flats)
    with col3:
        date_filter = st.date_input("Filter by Date Range", value=[], key='flat_date')
        
    search_col_opt = "Both Col B & C"
    search_query = selected_flat if selected_flat != "Select a Flat..." else ""

# --- Apply Date Filter First ---
if len(date_filter) == 2:
    start_date, end_date = date_filter
    
    # Filter Statement safely
    if "Txn Date" in df_stmt.columns:
        stmt_dates = pd.to_datetime(df_stmt["Txn Date"], dayfirst=True, errors='coerce').dt.date
        df_stmt = df_stmt[(stmt_dates >= start_date) & (stmt_dates <= end_date)]
        
    # Filter Bank Rec safely
    if 'Date' in df_rec.columns:
        rec_dates = pd.to_datetime(df_rec['Date'], errors='coerce').dt.date
        df_rec = df_rec[(rec_dates >= start_date) & (rec_dates <= end_date)]
    elif 'Unnamed: 1' in df_rec.columns:
        rec_dates = pd.to_datetime(df_rec['Unnamed: 1'], errors='coerce').dt.date
        df_rec = df_rec[(rec_dates >= start_date) & (rec_dates <= end_date)]

st.markdown("---")

# --- Filtering Logic ---
if search_query:
    import re
    # Validate that no space is used in flat number formats (e.g. "C1 " or "- " is not allowed) if it looks like a flat
    if re.search(r'^[a-zA-Z]\d\s', search_query) or '- ' in search_query or ' -' in search_query:
        st.warning("⚠️ Spaces are not allowed in the search query. Please remove any extra spaces.")
        st.stop()
        
    # Remove spaces and hyphens from the query for flexible matching against the Excel data
    query = re.sub(r'[\s\-]', '', search_query.lower())
    
    # 1. Filter Account Statement (Direct Match)
    col_b_name = "Narration (Col B)" if "Narration (Col B)" in df_stmt.columns else df_stmt.columns[1]
    col_c_name = "Ref No (Col C)" if "Ref No (Col C)" in df_stmt.columns else df_stmt.columns[2]
    
    b_normalized = df_stmt[col_b_name].str.lower().str.replace(r'[\s\-]', '', regex=True)
    c_normalized = df_stmt[col_c_name].str.lower().str.replace(r'[\s\-]', '', regex=True)
    
    if search_col_opt == "Both Col B & C":
        mask_stmt = b_normalized.str.contains(query) | c_normalized.str.contains(query)
    elif search_col_opt == "Only Col B (Narration)":
        mask_stmt = b_normalized.str.contains(query)
    else:
        mask_stmt = c_normalized.str.contains(query)
    
    # 2. Filter Bank Reconciliation First
    # If the user pasted an Account Statement UTR that ends in "DC", strip it so Bank Rec matches
    query_rec = query[:-2] if query.endswith('dc') else query
    
    # UTRs from the Account Statement often have extra leading zeroes (e.g., 00036... vs 036...) compared to Bank Rec.
    # We strip all leading zeros to find the core identifier, making matching bulletproof!
    query_rec_no_zeros = re.sub(r'^0+', '', query_rec)
    
    mask_rec = pd.Series([False] * len(df_rec))
    for col in df_rec.columns:
        col_normalized = df_rec[col].str.lower().str.replace(r'[\s\-]', '', regex=True)
        # If the query is sufficiently long (like a real UTR/Ref No), search using the stripped core 
        # to gracefully bypass differing lengths of leading zeroes between the files.
        if len(query_rec_no_zeros) >= 4:
            mask_rec = mask_rec | col_normalized.str.contains(query_rec_no_zeros)
        else:
            mask_rec = mask_rec | col_normalized.str.contains(query_rec)
    
    filtered_df_rec = df_rec[mask_rec]
    
    # 3. Cross-Reference Account Statement using Bank Reconciliation References
    
    # Start with empty mask for cross-references
    mask_stmt_cross = pd.Series([False] * len(df_stmt))
    
    if len(filtered_df_rec) > 0:
        rec_refs = set()
        # Look across all columns to extract UTRs/Txn IDs, bypassing broken Excel headers
        for col in df_rec.columns:
            for val in filtered_df_rec[col].dropna():
                # Replace non-alphanumeric with spaces to safely isolate words like UTR and TxnID
                clean_val = re.sub(r'[^a-zA-Z0-9]', ' ', str(val))
                # Extract any alphanumeric word >= 6 characters that contains at least one digit
                words = [w.lower() for w in clean_val.split() if len(w) >= 6 and re.search(r'\d', w)]
                for w in words:
                    rec_refs.add(w)
                    # Also add the zero-stripped version in case Bank Rec has MORE zeroes than Acct Statement (rare, but safe)
                    stripped = re.sub(r'^0+', '', w)
                    if len(stripped) >= 5:
                        rec_refs.add(stripped)
                            
        if rec_refs:
            # Create a regex to match ANY of these extracted references
            ref_regex = '|'.join([re.escape(r) for r in rec_refs])
            # Check Account statement for these references from Bank Rec
            # Because Account Statement adds "DC" (e.g. AXNFCN0...DC), str.contains() naturally matches the extracted core substring!
            mask_stmt_cross = b_normalized.str.contains(ref_regex) | c_normalized.str.contains(ref_regex)

    # Combine direct statement query matches AND cross-referenced matches from Bank Rec
    mask_stmt = mask_stmt | mask_stmt_cross
    filtered_df_stmt = df_stmt[mask_stmt]

else:
    filtered_df_stmt = df_stmt
    filtered_df_rec = df_rec

# --- Display Results ---

# Metrics Row
if app_mode == "🏢 Flat Management" and search_query:
    total_deposit = 0.0
    if "Deposit" in filtered_df_stmt.columns:
        total_deposit = pd.to_numeric(filtered_df_stmt["Deposit"].replace(r'[^\d.]', '', regex=True), errors='coerce').fillna(0).sum()
        
    total_debit = 0.0
    total_credit = 0.0
    for col in filtered_df_rec.columns:
        if 'debit' in str(col).lower() or 'unnamed: 5' in str(col).lower():
            total_debit += pd.to_numeric(filtered_df_rec[col].astype(str).replace(r'[^\d.]', '', regex=True), errors='coerce').fillna(0).sum()
        if 'credit' in str(col).lower() or 'unnamed: 6' in str(col).lower():
            total_credit += pd.to_numeric(filtered_df_rec[col].astype(str).replace(r'[^\d.]', '', regex=True), errors='coerce').fillna(0).sum()

    fm1, fm2, fm3, fm4 = st.columns(4)
    fm1.metric("Selected Flat", search_query)
    fm2.metric("Total Dues (Debit)", f"₹ {total_debit:,.2f}")
    fm3.metric("Total Paid (Credit)", f"₹ {total_credit:,.2f}")
    fm4.metric("Total Statement Deposits", f"₹ {total_deposit:,.2f}")
else:
    col_m1, col_m2 = st.columns(2)
    with col_m1:
        st.metric(label="Total Matches", value=(len(filtered_df_stmt) + len(filtered_df_rec)))
    with col_m2:
        if search_query:
            st.metric(label="Total Search Status", value="Filtered", delta="Active Search")
        else:
            st.metric(label="Total Search Status", value="Showing All", delta="No Filter", delta_color="off")

if app_mode == "🔍 Transaction Search":
    st.markdown("### 📋 Transaction & Reconciliation Records")
    st.markdown('<p class="info-text" style="font-size: 0.9rem;">View statements or reconciliations. Navigate the tabs below.</p>', unsafe_allow_html=True)
    
    tab1, tab2 = st.tabs(["📊 Account Statement Records", "🏦 Bank Reconciliation Status"])
    
    with tab1:
        if len(filtered_df_stmt) > 0:
            col_b_name = "Narration (Col B)" if "Narration (Col B)" in df_stmt.columns else df_stmt.columns[1]
            
            # We will build an HTML table that includes a copy button for each row using JavaScript.
            html_table = f"""
            <style>
            .table-container {{
                width: 100%;
                overflow-x: auto;
                border-radius: 10px;
                margin-bottom: 20px;
            }}
            .custom-table {{
                width: 100%;
                min-width: 800px;
                border-collapse: collapse;
                font-family: 'Inter', sans-serif;
                color: #ffffff;
                background-color: #1a1e2b;
            }}
            .custom-table th, .custom-table td {{
                padding: 12px 15px;
                text-align: left;
                border-bottom: 1px solid #3a3f58;
                word-wrap: break-word;
            }}
            .custom-table th {{
                background-color: #0e1117;
                color: #fca311;
                font-weight: 600;
                text-transform: uppercase;
                font-size: 0.85rem;
                position: sticky;
                top: 0;
                z-index: 10;
            }}
            /* Pin the action column to the right */
            .custom-table th:last-child,
            .custom-table td:last-child {{
                position: sticky;
                right: 0;
                background-color: #1a1e2b;
                box-shadow: -2px 0 5px rgba(0,0,0,0.2);
                z-index: 5;
            }}
            .custom-table th:last-child {{
                background-color: #0e1117;
                z-index: 11;
            }}
            .custom-table tr:hover td {{
                background-color: #2a2f45;
            }}
            .copy-btn {{
                background-color: #fca311;
                color: #000;
                border: none;
                padding: 6px 12px;
                border-radius: 5px;
                cursor: pointer;
                font-weight: bold;
                transition: background-color 0.2s;
                display: inline-flex;
                align-items: center;
                gap: 5px;
                white-space: nowrap;
            }}
            .copy-btn:hover {{
                background-color: #e3910c;
            }}
            </style>
            <script>
            function copyNarration(btn, text) {{
                navigator.clipboard.writeText(text).then(function() {{
                    const originalText = btn.innerHTML;
                    btn.innerHTML = '📋 Copied!';
                    btn.style.backgroundColor = '#4caf50';
                    btn.style.color = '#fff';
                    setTimeout(function() {{
                        btn.innerHTML = originalText;
                        btn.style.backgroundColor = '#fca311';
                        btn.style.color = '#000';
                    }}, 2000);
                }}, function(err) {{
                    console.error('Could not copy text: ', err);
                }});
            }}
            </script>
            <div class="table-container">
            <table class="custom-table">
                <thead>
                    <tr>
            """
            
            # Render table headers
            for col in filtered_df_stmt.columns:
                html_table += f"<th>{col}</th>"
            html_table += "<th>Action</th></tr></thead><tbody>"
            
            # Render table rows
            import json
            max_rows = min(len(filtered_df_stmt), 500)
            for idx, row in filtered_df_stmt.head(max_rows).iterrows():
                html_table += "<tr>"
                
                raw_narration = str(row.get(col_b_name, ""))
                js_safe_narration = json.dumps(raw_narration).replace('"', '&quot;')
                
                for col in filtered_df_stmt.columns:
                    val = row[col]
                    html_table += f"<td>{val}</td>"
                    
                html_table += f"""
                    <td>
                        <!-- We pass the safely encoded string to our JS function -->
                        <button class='copy-btn' onclick="copyNarration(this, {js_safe_narration})">
                            📋 Copy Narration
                        </button>
                    </td>
                </tr>
                """
                
            html_table += "</tbody></table></div>"
            if len(filtered_df_stmt) > 500:
                html_table += f"<div style='text-align: center; color: #fca311; padding: 10px;'>Showing first 500 of {len(filtered_df_stmt)} statement records.</div>"
            
            st.components.v1.html(html_table, height=500, scrolling=True)
        else:
            st.warning("No Account Statement records found matching your query.")
    
    with tab2:
        if len(filtered_df_rec) > 0:
            st.dataframe(
                filtered_df_rec,
                use_container_width=True,
                hide_index=True,
                height=500
            )
        else:
            st.warning("No Bank Reconciliation records found matching your query.")

elif app_mode == "🏢 Flat Management":
    st.markdown("---")
    if search_query:
        st.markdown(f"### 📋 Financial Ledger for Flat: {search_query}")
    else:
        st.markdown("### 📋 Financial Ledger (All Flats)")
        
    st.markdown('<p class="info-text" style="font-size: 0.95rem;">Grouped transactions sorted logically to easily verify dues against payments.</p>', unsafe_allow_html=True)
    
    # Identify Debit and Credit Columns dynamically
    debit_col = None
    credit_col = None
    for col in filtered_df_rec.columns:
        if 'debit' in str(col).lower() or 'unnamed: 5' in str(col).lower(): debit_col = col
        if 'credit' in str(col).lower() or 'unnamed: 6' in str(col).lower(): credit_col = col
        
    # Split the Bank Rec into Dues and Payments
    if debit_col and credit_col and len(filtered_df_rec) > 0:
        df_dues = filtered_df_rec[pd.to_numeric(filtered_df_rec[debit_col].astype(str).replace(r'[^\d.]', '', regex=True), errors='coerce').fillna(0) > 0]
        df_payments = filtered_df_rec[pd.to_numeric(filtered_df_rec[credit_col].astype(str).replace(r'[^\d.]', '', regex=True), errors='coerce').fillna(0) > 0]
    else:
        df_dues = pd.DataFrame()
        df_payments = pd.DataFrame()

    c1, c2 = st.columns(2)
    with c1:
        st.markdown("#### 🔴 Invoiced Dues (To Bank)")
        if len(df_dues) > 0:
            st.dataframe(df_dues, use_container_width=True, hide_index=True, height=350)
        else:
            st.info("No Dues found.")
            
    with c2:
        st.markdown("#### 🟢 Payments Received (From Bank)")
        if len(df_payments) > 0:
            st.dataframe(df_payments, use_container_width=True, hide_index=True, height=350)
        else:
            st.info("No Payments logged in Bank Rec.")
            
    st.markdown("---")
    st.markdown("#### 🏦 Direct Bank Deposits (Account Statement)")
    if len(filtered_df_stmt) > 0:
        st.dataframe(filtered_df_stmt, use_container_width=True, hide_index=True, height=350)
    else:
        st.info("No direct account statement records found for this flat.")

# --- Footer ---
st.markdown("---")
st.markdown(
    """
    <div style='text-align: center; color: #666; font-size: 0.9em; padding-bottom: 20px;'>
        Built for parsing Account Statements 🚀
    </div>
    """,
    unsafe_allow_html=True
)
