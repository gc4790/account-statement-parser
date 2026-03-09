import streamlit as st
import pandas as pd
import os
import json

SETTINGS_FILE = "society_settings.json"

def load_settings():
    default_settings = {
        "base_maintenance": 2500.0,
        "penalty_apr": 18.0,
        "grace_period_day": 10,
        "gmail_sender_email": "bhumisilveriiocwing@gmail.com",
        "brevo_login": "",
        "brevo_smtp_key": ""
    }
    if os.path.exists(SETTINGS_FILE):
        try:
            with open(SETTINGS_FILE, "r") as f:
                settings = json.load(f)
                default_settings.update(settings)
        except:
            pass
    return default_settings

def save_settings(settings):
    with open(SETTINGS_FILE, "w") as f:
        json.dump(settings, f, indent=4)

DB_CONFIG_FILE = "db_config.json"

def load_db_config():
    default = {"host": "localhost", "port": "3306", "user": "root", "password": "root", "database": "society_plus"}
    if os.path.exists(DB_CONFIG_FILE):
        try:
            with open(DB_CONFIG_FILE, "r") as f:
                default.update(json.load(f))
        except:
            pass
    return default

def save_db_config(cfg):
    with open(DB_CONFIG_FILE, "w") as f:
        json.dump(cfg, f, indent=4)


def send_payment_receipt(to_email, flat_no, owner_name, fy_label, res_df, carry_forward, brevo_login, brevo_smtp_key, from_email="bhumisilveriiocwing@gmail.com"):
    """Sends an HTML payment receipt via Brevo SMTP relay."""
    import smtplib
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText

    FROM = from_email

    # Build month-wise table rows
    rows_html = ""
    for _, r in res_df.iterrows():
        rows_html += f"""
        <tr>
            <td style='padding:8px 12px;border-bottom:1px solid #e0e0e0;'>{r['Month']}</td>
            <td style='padding:8px 12px;border-bottom:1px solid #e0e0e0;text-align:right;'>&#8377;{float(r['Current Dues']):,.2f}</td>
            <td style='padding:8px 12px;border-bottom:1px solid #e0e0e0;text-align:right;'>&#8377;{float(r['Amount Paid']):,.2f}</td>
            <td style='padding:8px 12px;border-bottom:1px solid #e0e0e0;text-align:right;'>&#8377;{float(r['New Penalty Added']):,.2f}</td>
            <td style='padding:8px 12px;border-bottom:1px solid #e0e0e0;text-align:right;'>&#8377;{float(r['Closing Principal']):,.2f}</td>
        </tr>"""

    total_paid = res_df['Amount Paid'].sum()
    final_principal = res_df.iloc[-1]['Closing Principal']
    final_penalty = res_df['New Penalty Added'].sum()

    html = f"""
    <html><body style='font-family:Arial,sans-serif;color:#222;'>
    <div style='max-width:650px;margin:auto;border:1px solid #ddd;border-radius:10px;overflow:hidden;'>
        <div style='background:#1a1e2b;padding:20px 30px;'>
            <h2 style='color:#fca311;margin:0;'>Bhumi Silver II — OC Wing</h2>
            <p style='color:#aaa;margin:4px 0 0;'>Maintenance Payment Receipt</p>
        </div>
        <div style='padding:24px 30px;'>
            <table style='width:100%;margin-bottom:16px;'>
                <tr><td style='color:#666;'>Flat No.</td><td><strong>{flat_no}</strong></td></tr>
                <tr><td style='color:#666;'>Owner</td><td><strong>{owner_name}</strong></td></tr>
                <tr><td style='color:#666;'>Financial Year</td><td><strong>{fy_label}</strong></td></tr>
                <tr><td style='color:#666;'>Opening Outstanding</td><td><strong>&#8377;{carry_forward:,.2f}</strong></td></tr>
            </table>
            <h3 style='color:#1a1e2b;border-bottom:2px solid #fca311;padding-bottom:6px;'>Month-wise Summary</h3>
            <table style='width:100%;border-collapse:collapse;font-size:0.9rem;'>
                <thead style='background:#f5f5f5;'>
                    <tr>
                        <th style='padding:8px 12px;text-align:left;'>Month</th>
                        <th style='padding:8px 12px;text-align:right;'>Dues</th>
                        <th style='padding:8px 12px;text-align:right;'>Paid</th>
                        <th style='padding:8px 12px;text-align:right;'>Penalty</th>
                        <th style='padding:8px 12px;text-align:right;'>Closing</th>
                    </tr>
                </thead>
                <tbody>{rows_html}</tbody>
            </table>
            <table style='width:100%;margin-top:20px;background:#f9f9f9;border-radius:8px;padding:16px;'>
                <tr><td style='padding:6px;color:#555;'>Total Paid</td><td style='text-align:right;'><strong>&#8377;{total_paid:,.2f}</strong></td></tr>
                <tr><td style='padding:6px;color:#c0392b;'>Total Penalty</td><td style='text-align:right;color:#c0392b;'><strong>&#8377;{final_penalty:,.2f}</strong></td></tr>
                <tr><td style='padding:6px;color:#555;'>Outstanding Balance</td><td style='text-align:right;'><strong>&#8377;{final_principal:,.2f}</strong></td></tr>
            </table>
        </div>
        <div style='background:#f5f5f5;padding:14px 30px;font-size:0.8rem;color:#888;text-align:center;'>
            This is an auto-generated receipt. For queries contact the society office.
        </div>
    </div>
    </body></html>"""

    msg = MIMEMultipart("alternative")
    msg["Subject"] = f"Maintenance Receipt — Flat {flat_no} ({fy_label})"
    msg["From"] = FROM
    msg["To"] = to_email
    msg.attach(MIMEText(html, "html"))

    # Brevo SMTP — try new hostname first, fall back to legacy sendinblue hostname
    _brevo_login_clean = brevo_login.strip()
    _brevo_key_clean = brevo_smtp_key.strip()
    _sent = False
    _last_err = None
    for _host in ["smtp-relay.brevo.com", "smtp-relay.sendinblue.com"]:
        try:
            with smtplib.SMTP(_host, 587, timeout=15) as server:
                server.ehlo()
                server.starttls()
                server.ehlo()
                server.login(_brevo_login_clean, _brevo_key_clean)
                server.sendmail(FROM, to_email, msg.as_string())
            _sent = True
            break
        except Exception as _e:
            _last_err = _e
    if not _sent:
        raise Exception(f"{_last_err}")


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


# Main title logic moved into dedicated view blocks below


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
app_mode = st.sidebar.radio("Select View:", ["🔍 Transaction Search", "🏢 Flat Management", "📤 Upload Payments", "⚙️ Settings"])

# --- Load DB config globally (used across all pages) ---
_cfg = load_db_config()
db_host = _cfg["host"]
db_port = _cfg["port"]
db_user = _cfg["user"]
db_pass = _cfg["password"]
db_name = _cfg["database"]


if "df_stmt" not in st.session_state:
    st.session_state.df_stmt = None
if "df_rec" not in st.session_state:
    st.session_state.df_rec = None

# Show file uploader only in Transaction Search mode
if app_mode == "🔍 Transaction Search":
    st.title("🧾 Account Statement Parser")
    st.markdown('<p class="info-text">Search for transactions by Flat No., Owner Name, or Reference ID</p>', unsafe_allow_html=True)
    
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

# Initialize variables to avoid NameErrors globally across all views
search_query = ""
date_filter = []
search_col_opt = "Both Col B & C"

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
elif app_mode == "🏢 Flat Management":
    st.title("🏢 Flat Account Management")
    st.markdown('<p class="info-text">View your reconciled financial ledgers and manage the resident database below.</p>', unsafe_allow_html=True)

    # --- Single shared flat selector ABOVE tabs ---
    _flat_list_shared = []
    try:
        import sqlalchemy as _sqla_shared
        _eng_shared = _sqla_shared.create_engine(f"mysql+pymysql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}")
        _df_shared = pd.read_sql("SELECT `Flat No` FROM flat_details", con=_eng_shared)
        if not _df_shared.empty:
            _flat_list_shared = sorted(_df_shared['Flat No'].dropna().unique().tolist())
    except:
        pass
    selected_flat = st.selectbox("🏠 Select Flat", ["-- Select Flat --"] + _flat_list_shared, key="shared_flat_selector")

    tab_search, tab_upload, tab_calc = st.tabs(["🔍 Search Flat Details", "📥 Bulk Upload Tenant/Owner DB", "🧮 Maintenance Calculator"])


    with tab_search:
        if selected_flat != "-- Select Flat --":
            try:
                import sqlalchemy as _sqla_srch
                _eng_srch = _sqla_srch.create_engine(f"mysql+pymysql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}")
                _df_srch = pd.read_sql(
                    f"SELECT * FROM flat_details WHERE `Flat No` = '{selected_flat}' LIMIT 1",
                    con=_eng_srch,
                )
                if not _df_srch.empty:
                    info = _df_srch.iloc[0]
                    flat_no = info.get("Flat No", "N/A")
                    owner = info.get("Owner Name", "N/A")
                    is_rented = info.get("Rented Status", "No")
                    tenant = info.get("Tenant Name", "N/A") if is_rented == "Yes" else "N/A"
                    contact = info.get("Contact Number", "N/A")
                    f_type = info.get("Flat Type", "N/A")
                    f_area = info.get("Area (sq ft)", "N/A")
                    st.info(f"### 🚪 Flat {flat_no}\n**👤 Owner:** {owner} &nbsp;&nbsp;|&nbsp;&nbsp; **📞 Contact:** {contact} &nbsp;&nbsp;|&nbsp;&nbsp; **🏠 Type:** {f_type} ({f_area} sq ft)\n\n**🔑 Rented:** {is_rented} &nbsp;&nbsp;|&nbsp;&nbsp; **🧑\u200d🤝\u200d🧑 Tenant:** {tenant}")
                else:
                    st.warning("No details found for this flat.")
            except Exception as e:
                st.error(f"Search error: {e}")
        else:
            st.info("👆 Select a flat from the dropdown above to view its details.")

# --- Apply Date Filter First ---
if len(date_filter) == 2 and df_stmt is not None and df_rec is not None:
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
if search_query and df_stmt is not None and df_rec is not None:
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

# Metrics Row
if app_mode == "🔍 Transaction Search":
    col_m1, col_m2 = st.columns(2)
    with col_m1:
        total = 0
        if filtered_df_stmt is not None and filtered_df_rec is not None:
            total = len(filtered_df_stmt) + len(filtered_df_rec)
        st.metric(label="Total Matches", value=total)
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
    


    with tab_upload:
        st.markdown("### 🏠 Flat & Tenant Database")
        st.markdown('<p class="info-text">Manage flat owners, tenant details, and area specifications natively in MySQL.</p>', unsafe_allow_html=True)
        
        try:
            import sqlalchemy
            engine = sqlalchemy.create_engine(f"mysql+pymysql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}")
            
            flat_file = st.file_uploader("📥 Bulk Upload Flat & Tenant Details (.xls / .xlsx)", type=["xls", "xlsx"], key="flat_upload")
            
            try:
                df_flat_db = pd.read_sql("SELECT * FROM flat_details", con=engine)
            except Exception:
                df_flat_db = pd.DataFrame(columns=[
                    "Flat No", "Owner Name", "Rented Status", "Tenant Name", "Flat Type", "Area (sq ft)", "Contact Number", "Email ID"
                ])
                
            if flat_file is not None:
                try:
                    # Quick validation to prevent Payment File upload here
                    xl_preview = pd.ExcelFile(flat_file)
                    if len(xl_preview.sheet_names) > 50 and any('C1-' in s for s in xl_preview.sheet_names):
                        st.error("🛑 STOP! You uploaded the **Payment / Maintenance** file here!\n\nThis upload box is ONLY for the **Tenant & Owner Database**. To upload parsed payment records, please click **'📤 Upload Payments'** in the gray Navigation Sidebar on the far left!")
                        df_flat = df_flat_db
                    else:
                        df_upload = pd.read_excel(flat_file, header=None)
                        
                        # 1. Dynamically find the header row by looking for 'Flat No'
                        header_idx = None
                        for i, row in df_upload.iterrows():
                            if row.astype(str).str.contains(r'Flat\s*No', case=False, na=False).any():
                                header_idx = i
                                break
                                
                        if header_idx is not None:
                            df_upload.columns = df_upload.iloc[header_idx]
                            df_upload = df_upload.iloc[header_idx + 1:].reset_index(drop=True)
                            
                        # 2. Map custom columns to the database schema
                        rename_map = {}
                        for col in df_upload.columns:
                            col_str = str(col).strip().lower()
                            if 'owner' in col_str:
                                rename_map[col] = 'Owner Name'
                            elif 'flat no' in col_str:
                                rename_map[col] = 'Flat No'
                            elif 'mobile' in col_str or 'contact' in col_str:
                                rename_map[col] = 'Contact Number'
                            elif 'tenant' in col_str:
                                rename_map[col] = 'Tenant Name'
                        
                        if rename_map:
                            df_upload.rename(columns=rename_map, inplace=True)

                        # Ensure schema matches by adding missing expected columns
                        for col in df_flat_db.columns:
                            if col not in df_upload.columns:
                                df_upload[col] = None
                                
                        # Remove trailing NaNs
                        df_upload.dropna(subset=['Flat No'], inplace=True)
                        
                        # Reorder/filter to strictly match the expected DB schema
                        df_flat = df_upload[[c for c in df_flat_db.columns if c in df_upload.columns]]
                        st.success("✅ Tenant DB file loaded! Review the data below and click **Save Database Changes** to safely write this into MySQL.")
                except Exception as e:
                    st.error(f"Error reading uploaded file: {e}")
                    df_flat = df_flat_db
            else:
                df_flat = df_flat_db
                
            edited_df = st.data_editor(
                df_flat, 
                num_rows="dynamic",
                use_container_width=True,
                hide_index=True,
                column_config={
                    "Rented Status": st.column_config.SelectboxColumn("Rented Status", options=["Yes", "No"], required=True),
                    "Flat Type": st.column_config.SelectboxColumn("Flat Type", options=["1 BHK", "2 BHK", "3 BHK", "4 BHK", "Penthouse"]),
                }
            )
            
            if st.button("💾 Save Database Changes", type="primary"):
                with st.spinner("Saving to MySQL..."):
                    edited_df.to_sql(name="flat_details", con=engine, if_exists="replace", index=False)
                st.success("✅ Changes successfully saved to `society_plus` database!")
        except Exception as e:
            st.error(f"❌ Failed to connect to MySQL. Ensure your credentials in the sidebar are correct. Error: {e}")

    with tab_calc:
        st.markdown("### 🧮 Maintenance Ledger & Penalty Calculator")
        st.markdown('<p class="info-text">Simulate and calculate resident maintenance dues with automatic penalty and overpayment forwarding logic.</p>', unsafe_allow_html=True)
        
        society_settings = load_settings()
        
        # Pull configuration silently from Global Settings
        base_dues = float(society_settings["base_maintenance"])
        penalty_apr = float(society_settings.get("penalty_apr", 18.0))
        grace_day = int(society_settings["grace_period_day"])
        
        monthly_interest_rate = penalty_apr / 12 / 100
        
        st.info(f"⚙️ **Active Global Policy:** {penalty_apr}% Annual Penalty applied to unpaid dues after day {grace_day} of each month.")
        # selected_flat is the shared selector defined above the tabs
        if selected_flat == "-- Select Flat --":
            st.info("👆 Select a flat from the dropdown above to view the maintenance ledger.")
            st.stop()

        # --- Financial Year selector (Indian FY: Apr YY to Mar YY+1) ---
        current_cal_year = 2026
        fy_start_options = list(range(2023, current_cal_year + 1))
        fy_labels = [f"{y}-{str(y+1)[-2:]}" for y in fy_start_options]
        all_fy_options = ["All Years"] + fy_labels
        default_fy_idx = 1  # Default to 2023-24 (index 1 since All Years is index 0)
        selected_fy_label = st.selectbox("📅 Select Financial Year", all_fy_options, index=default_fy_idx)
        all_years_mode = selected_fy_label == "All Years"
        selected_year = fy_start_options[fy_labels.index(selected_fy_label)] if not all_years_mode else fy_start_options[0]

        summary_container = st.container()

        # Build month list helper — all FYs are Apr–Mar (Indian FY)
        # FY 2023-24 starts from Apr 2023 naturally; Jan/Feb/Mar 2024 are valid tail months
        def build_fy_months(yr):
            return ([(m, yr) for m in ["Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]] +
                    [(m, yr+1) for m in ["Jan","Feb","Mar"]])

        if all_years_mode:
            fy_months = []
            for yr in fy_start_options:
                fy_months += build_fy_months(yr)
        else:
            fy_months = build_fy_months(selected_year)

        # Trim months beyond the current month (never show future months)
        _now = pd.Timestamp.now()
        _month_abbrs = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
        fy_months = [
            (m, y) for m, y in fy_months
            if pd.Timestamp(year=y, month=_month_abbrs.index(m)+1, day=1) <= _now.replace(day=1)
        ]

        months_with_year = [f"{m} {y}" for m, y in fy_months]
        num_months = len(fy_months)


        if "calc_df" not in st.session_state:
            st.session_state.calc_df = pd.DataFrame({
                "Month": months_with_year,
                "Base Dues": [base_dues] * num_months,
                "Payment Received": [0.0] * num_months,
            })
            st.session_state.calc_key = 0
            st.session_state.calc_year = selected_year
            st.session_state.calc_all_years = all_years_mode

        # Detect flat or year change and auto-fill payments from DB
        if "last_selected_flat" not in st.session_state:
            st.session_state.last_selected_flat = "-- Select Flat --"
        if "calc_year" not in st.session_state:
            st.session_state.calc_year = selected_year
        if "calc_all_years" not in st.session_state:
            st.session_state.calc_all_years = all_years_mode

        flat_changed = selected_flat != st.session_state.last_selected_flat
        year_changed  = selected_year != st.session_state.calc_year or all_years_mode != st.session_state.calc_all_years

        if flat_changed or year_changed:
            st.session_state.last_selected_flat = selected_flat
            st.session_state.calc_year = selected_year
            st.session_state.calc_all_years = all_years_mode

            # Clear stale carry-forward widget state
            old_cf_key = f"cf_input_{selected_flat}_{selected_year}"
            if old_cf_key in st.session_state:
                del st.session_state[old_cf_key]

            # Rebuild month list
            if all_years_mode:
                fy_months = []
                for yr in fy_start_options:
                    fy_months += build_fy_months(yr)
            else:
                fy_months = build_fy_months(selected_year)

            months_with_year = [f"{m} {y}" for m, y in fy_months]
            num_months = len(fy_months)
            st.session_state.calc_df = pd.DataFrame({
                "Month": months_with_year,
                "Base Dues": [base_dues] * num_months,
                "Payment Received": [0.0] * num_months,
            })

            
            if selected_flat != "-- Select Flat --":
                try:
                    import sqlalchemy
                    engine = sqlalchemy.create_engine(f"mysql+pymysql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}")
                    # Fetch all payments for the selected flat
                    df_hist = pd.read_sql(
                        f"SELECT `Month`, `Amount`, `Date` FROM payment_history "
                        f"WHERE `Flat Number` = '{selected_flat}'",
                        con=engine
                    )

                    # Always explicitly zero-out all months first so missing months show 0
                    st.session_state.calc_df["Payment Received"] = 0.0

                    if not df_hist.empty:
                        # Parse the Date column (most reliable source) to derive year + month
                        df_hist['_parsed_date'] = pd.to_datetime(df_hist['Date'], errors='coerce')

                        # Filter to FY date range only if not showing all years
                        if not all_years_mode:
                            fy_start_dt = pd.Timestamp(year=selected_year, month=4, day=1)
                            fy_end_dt   = pd.Timestamp(year=selected_year + 1, month=3, day=31)
                            df_hist = df_hist[
                                (df_hist['_parsed_date'] >= fy_start_dt) &
                                (df_hist['_parsed_date'] <= fy_end_dt)
                            ].copy()

                        # Derive month abbreviation and year from the Date column
                        df_hist['Month_Short'] = df_hist['_parsed_date'].dt.strftime('%b')
                        df_hist['Year_Num'] = df_hist['_parsed_date'].dt.year

                        # For rows where Date is missing/invalid, fall back to the Month text field
                        def parse_month_abbrev_fallback(val):
                            import re
                            s = str(val).strip()
                            try:
                                return pd.to_datetime(s).strftime('%b')
                            except Exception:
                                pass
                            letters = re.sub(r'[^a-zA-Z]', '', s)[:3].title()
                            return letters if letters else ''

                        mask_no_date = df_hist['Month_Short'].isna() | (df_hist['Month_Short'] == '')
                        if mask_no_date.any():
                            df_hist.loc[mask_no_date, 'Month_Short'] = df_hist.loc[mask_no_date, 'Month'].apply(parse_month_abbrev_fallback)

                        # Ensure Amount is numeric
                        df_hist['Amount'] = pd.to_numeric(df_hist['Amount'], errors='coerce').fillna(0)

                        # Sum amounts per (Month, Year) and map to calc_df rows
                        monthly_totals = df_hist.groupby(['Month_Short', 'Year_Num'])['Amount'].sum().reset_index()

                        for _, row in monthly_totals.iterrows():
                            label = f"{row['Month_Short']} {int(row['Year_Num'])}"
                            idx = st.session_state.calc_df[st.session_state.calc_df['Month'] == label].index
                            if not idx.empty:
                                st.session_state.calc_df.loc[idx[0], 'Payment Received'] = float(row['Amount'])


                except Exception:
                    pass
            
            st.session_state.calc_key += 1
            st.rerun()
            
        # Hard-sync Base Dues to the global configuration
        if "calc_df" in st.session_state:
            st.session_state.calc_df["Base Dues"] = base_dues
            
        if st.button("Reset Payments to Zero"):
            st.session_state.calc_df["Payment Received"] = 0.0
            st.session_state.calc_key += 1
            st.rerun()
            
        st.markdown("#### 📝 Input Payments (Editable)")
        st.info("Edit the **Payment Received** column below to simulate different scenarios. The Ledger will dynamically recalculate below.")
        
        edited_df = st.data_editor(
            st.session_state.calc_df, 
            key=f"calc_editor_{st.session_state.calc_key}",
            use_container_width=True, 
            hide_index=True,
            column_config={
                "Month": st.column_config.Column("Month", disabled=True),
                "Base Dues": st.column_config.NumberColumn("Base Dues (₹) - From Config", disabled=True),
                "Payment Received": st.column_config.NumberColumn("Payment Received (₹)", min_value=0.0, step=100.0),
            }
        )
        # Update session state with edited values so they persist if other tabs are clicked
        st.session_state.calc_df = edited_df.copy()

        # --- Silently load carry forward ---
        # Priority: flat_carry_forward table (manually managed, upload-safe)
        # Fallback: Outstanding from first record in payment_history (for flats not yet in flat_carry_forward)
        carry_forward_input = 0.0
        if selected_flat != "-- Select Flat --":
            try:
                import sqlalchemy as _sqla_cf_load
                _cf_load_eng = _sqla_cf_load.create_engine(f"mysql+pymysql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}")
                with _cf_load_eng.connect() as _conn:
                    _conn.execute(_sqla_cf_load.text("""
                        CREATE TABLE IF NOT EXISTS `flat_carry_forward` (
                            `Flat Number` VARCHAR(50) PRIMARY KEY,
                            `Outstanding` DOUBLE NOT NULL DEFAULT 0.0
                        )
                    """))
                    _conn.commit()
                _cf_row = pd.read_sql(
                    f"SELECT `Outstanding` FROM flat_carry_forward WHERE `Flat Number` = '{selected_flat}' LIMIT 1",
                    con=_cf_load_eng,
                )
                if not _cf_row.empty:
                    # flat_carry_forward has an explicit entry (may be 0 if user cleared it)
                    carry_forward_input = float(_cf_row.iloc[0]["Outstanding"])
                else:
                    # No entry yet — fall back to first record in payment_history
                    _ph_row = pd.read_sql(
                        f"SELECT `Outstanding` FROM payment_history "
                        f"WHERE `Flat Number` = '{selected_flat}' ORDER BY `Date` ASC LIMIT 1",
                        con=_cf_load_eng,
                    )
                    if not _ph_row.empty:
                        carry_forward_input = float(_ph_row.iloc[0]["Outstanding"])
            except Exception:
                carry_forward_input = 0.0


        # Seed calculation engine
        results = []
        forward_principal = carry_forward_input
        accumulated_penalty = 0.0

        for idx, row in edited_df.iterrows():
            month = row["Month"]
            dues = float(row["Base Dues"])
            payment = float(row["Payment Received"])
            
            # Step 1: Outstanding Principal + Current Month Dues
            total_principal_required = forward_principal + dues
            
            # Total amount owed including historical penalties (for display purposes)
            total_required_display = total_principal_required + accumulated_penalty
            
            # Step 2: Apply Payment
            # Rule 4: Pay off historical penalties first, then principal
            remaining_payment = payment
            
            if remaining_payment > 0 and accumulated_penalty > 0:
                if remaining_payment >= accumulated_penalty:
                    remaining_payment -= accumulated_penalty
                    accumulated_penalty = 0.0
                else:
                    accumulated_penalty -= remaining_payment
                    remaining_payment = 0.0
            
            # Apply remaining payment to principal
            principal_after_payment = total_principal_required - remaining_payment
            
            penalty_applied = 0.0
            # Penalty only applies from October 2023 onwards (society policy)
            PENALTY_START = pd.Timestamp(2023, 10, 1)
            try:
                month_dt = pd.to_datetime(month, format="%b %Y")
            except Exception:
                month_dt = None

            # Rule 1 & Rule 3: Penalty if principal payment not received / underpaid
            if principal_after_payment > 0 and month_dt is not None and month_dt >= PENALTY_START:
                # Calculate penalty based on the unadjusted principal owed (simple interest)
                penalty_applied = principal_after_payment * monthly_interest_rate
                # Simple Interest: Add to accumulated penalty, NOT to principal
                accumulated_penalty += penalty_applied
                closing_principal = principal_after_payment
            else:
                # Rule 2: If overpaid, adjust in next month -> principal_after_payment is <= 0
                closing_principal = principal_after_payment
            
            # The closing balance shown is the net sum of advanced principal + any unpaid penalties
            # e.g., if advance is -1000 and penalty is 250, closing balance represents net -750 or just show them separately.
            # To be clear, we will show Closing Principal and Total Closing Balance
            
            closing_balance = closing_principal + accumulated_penalty

            # The closing principal is strictly dues minus base payments.
            # Penalties exist in a completely separate bucket (accumulated_penalty).

            results.append({
                "Month": month,
                "Opening Principal": forward_principal,
                "Current Dues": dues,
                "Total Principal Due": total_principal_required,
                "Unpaid Penalties": accumulated_penalty - penalty_applied, # Historical penalties before this month's addition
                "Amount Paid": payment,
                "New Penalty Added": penalty_applied,
                "Closing Principal": closing_principal,
                "Total Closing Obligation": closing_principal + accumulated_penalty
            })
            
            # Carry over to next month
            forward_principal = closing_principal
            
        res_df = pd.DataFrame(results)
        
        # Formatting for display
        def format_currency(val):
            if val == 0:
                return "₹0"
            elif val > 0:
                return f"₹{val:,.2f} (Due)"
            else:
                return f"₹{abs(val):,.2f} (Advance)"
                
        display_df = res_df.copy()
        
        # Format general currency columns 
        for col in ["Current Dues", "Amount Paid", "New Penalty Added", "Unpaid Penalties"]:
            display_df[col] = display_df[col].apply(lambda x: f"₹{x:,.2f}")
            
        # Format Principal/Obligation columns with (Due)/(Advance)
        for col in ["Opening Principal", "Total Principal Due", "Closing Principal", "Total Closing Obligation"]:
            display_df[col] = display_df[col].apply(format_currency)
            
        st.markdown(f"#### 📊 Final Computed Ledger (Simple Interest) for {selected_flat if selected_flat != '-- Select Flat --' else 'Pending Selection'}")
        
        # Show carry-forward banner if available
        # Show carry-forward banner using the value already loaded from flat_carry_forward
        if carry_forward_input > 0:
            st.markdown(
                f"""<div style="background:linear-gradient(90deg,#fca311,#e3910c);padding:12px 20px;border-radius:10px;
                font-size:1.1rem;font-weight:700;color:#000;margin-bottom:12px;">
                📌 Carry Forward (Opening Outstanding): ₹{carry_forward_input:,.2f}</div>""",
                unsafe_allow_html=True,
            )

        st.dataframe(display_df, use_container_width=True, hide_index=True)

        # --- Excel Download ---
        import io
        def to_excel(df):
            buf = io.BytesIO()
            with pd.ExcelWriter(buf, engine="openpyxl") as writer:
                df.to_excel(writer, index=False, sheet_name="Ledger")
            return buf.getvalue()

        flat_label = selected_flat if selected_flat != "-- Select Flat --" else "All_Flats"
        fy_label_clean = selected_fy_label.replace("/", "-").replace(" ", "_")
        st.download_button(
            label="⬇️ Download Ledger as Excel",
            data=to_excel(res_df),
            file_name=f"Ledger_{flat_label}_{fy_label_clean}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

        # --- Send Payment Receipt Email ---
        st.markdown("---")
        if st.button("📧 Send Payment Receipt to Flat Owner", type="primary", use_container_width=True):
            _sett = load_settings()
            _brevo_login = _sett.get("brevo_login", "").strip()
            _brevo_key = _sett.get("brevo_smtp_key", "").strip()
            if not _brevo_login or not _brevo_key:
                st.error("❌ Brevo credentials not configured. Go to ⚙️ Settings and enter your Brevo Login Email and SMTP Key.")
            else:
                try:
                    import sqlalchemy as _sqla_email
                    _email_eng = _sqla_email.create_engine(f"mysql+pymysql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}")
                    _flat_row = pd.read_sql(
                        f"SELECT `Owner Name`, `Email ID` FROM flat_details WHERE `Flat No` = '{selected_flat}' LIMIT 1",
                        con=_email_eng,
                    )
                    if _flat_row.empty or not str(_flat_row.iloc[0].get("Email ID", "")).strip():
                        st.toast(f"⚠️ Add an email for flat {selected_flat} first!", icon="📧")
                        st.warning(f"No email ID found for **{selected_flat}**. Please go to the 📥 Bulk Upload tab and add an email for this flat.")
                    else:
                        _to_email = str(_flat_row.iloc[0]["Email ID"]).strip()
                        _owner = str(_flat_row.iloc[0].get("Owner Name", "Resident"))
                        with st.spinner(f"📤 Sending receipt to {_to_email}..."):
                            try:
                                send_payment_receipt(
                                    to_email=_to_email,
                                    flat_no=selected_flat,
                                    owner_name=_owner,
                                    fy_label=selected_fy_label,
                                    res_df=res_df,
                                    carry_forward=carry_forward_input,
                                    brevo_login=_brevo_login,
                                    brevo_smtp_key=_brevo_key,
                                    from_email=_sett.get("gmail_sender_email", "bhumisilveriiocwing@gmail.com"),
                                )
                                st.success(f"✅ Receipt sent successfully to **{_to_email}** ({_owner})")
                            except Exception as _mail_err:
                                st.error(f"❌ Failed to send email: {_mail_err}")
                except Exception as _db_err:
                    st.error(f"❌ Could not fetch flat email from DB: {_db_err}")


        if selected_flat != "-- Select Flat --":
            with summary_container:
                st.markdown("---")
                st.markdown(f"### 🎯 Year-End Summary for {selected_flat}")

                
                # Extract the final month's metrics directly from the calculation engine's final iteration
                final_principal = closing_principal
                final_penalties = accumulated_penalty
                final_total_obligation = final_principal + final_penalties
                
                # Use already-loaded carry forward (from flat_carry_forward table)
                carry_forward = carry_forward_input


                # Display final metrics including Carry Forward
                m1, m2, m3, m4 = st.columns(4)
                with m1:
                    st.metric(
                        "Total Principal Outstanding",
                        f"₹{final_principal:,.2f}"
                        if final_principal > 0
                        else f"₹0.00 (Advance: ₹{abs(final_principal):,.2f})",
                    )
                with m2:
                    st.metric("Total Unpaid Penalty", f"₹{final_penalties:,.2f}")
                with m3:
                    st.metric(
                        "Carry Forward Amount",
                        f"₹{carry_forward:,.2f}",
                        delta="Included",
                        delta_color="inverse",
                    )
                with m4:
                    total_obligation = final_total_obligation + carry_forward
                    st.metric(
                        "Final Total Obligation",
                        f"₹{total_obligation:,.2f}",
                        delta="Action Required" if total_obligation > 0 else "All Clear",
                        delta_color="inverse",
                    )
                st.markdown("---")


# --- Upload Payments Menu ---
elif app_mode == "📤 Upload Payments":
    st.title("📤 Manage Payment Details")
    st.markdown('<p class="info-text">Upload payment files and search for specific flat details.</p>', unsafe_allow_html=True)
    
    st.markdown("### 📥 Upload Payment File")
    st.info("Upload an Excel file containing payment details.")
    
    pay_file = st.file_uploader("Upload Resident Payments (.xls / .xlsx)", type=["xls", "xlsx"], key="payment_upload")
    
    if pay_file is not None:
        try:
            with st.spinner("Processing file..."):
                xl = pd.ExcelFile(pay_file)
                sheet_names = [s for s in xl.sheet_names if s.strip().lower() not in ["master", "total"]]
                
            st.success(f"✅ File loaded successfully! Found {len(sheet_names)} payment sheets.")
            
            st.markdown("### 🔍 Search Payments")
            search_flat = st.text_input("Enter Flat Number (e.g., C1-1101, A-104)", placeholder="Search for a flat...")
            
            if search_flat:
                search_query = search_flat.strip().upper().replace(" ", "")
                
                # Simple exact match or contains match on sheet names
                matching_sheets = [s for s in sheet_names if search_query in s.strip().upper().replace(" ", "")]
                
                if matching_sheets:
                    st.write(f"Found {len(matching_sheets)} matching flat(s):")
                    
                    # --- Parse all matching sheets first ---
                    parsed_sheets = {}  # sheet_name -> final_df
                    for sheet in matching_sheets:
                        try:
                            df_sheet = pd.read_excel(xl, sheet_name=sheet, header=None)
                            
                            header_idx = None
                            start_col_idx = None
                            
                            def is_start_col(c):
                                cell_str = str(c).strip().lower()
                                return cell_str.startswith('year') or cell_str == 'month' or cell_str.startswith('column')
                            
                            for i, row in df_sheet.iterrows():
                                row_str = row.astype(str).str.lower().tolist()
                                year_month_present = any(is_start_col(cell) for cell in row_str)
                                amount_present = any(str(cell).strip().lower() == 'amount' for cell in row_str)
                                if year_month_present and amount_present:
                                    header_idx = i
                                    for j, cell in enumerate(row_str):
                                        if is_start_col(cell):
                                            start_col_idx = j
                                            break
                                    break
                                    
                            if header_idx is not None and start_col_idx is not None:
                                clean_df = df_sheet.iloc[header_idx + 1:, start_col_idx : start_col_idx + 9].copy()
                                clean_df.columns = df_sheet.iloc[header_idx, start_col_idx : start_col_idx + 9].astype(str).str.strip()
                                
                                clean_headers = ["Month", "Last Month Interest", "Outstanding", "Monthly Dues", "Date", "Narration", "Amount", "Balance", "Interest"]
                                if len(clean_df.columns) == 9:
                                    clean_df.columns = clean_headers
                                else:
                                    from collections import Counter
                                    counts = Counter()
                                    new_cols = []
                                    for col in clean_df.columns:
                                        new_cols.append(f"{col}_{counts[col]}" if counts[col] > 0 else col)
                                        counts[col] += 1
                                    clean_df.columns = new_cols
                                
                                clean_df = clean_df.dropna(how='all')
                                if "Date" in clean_df.columns:
                                    clean_df["Date"] = pd.to_datetime(clean_df["Date"], errors='coerce').dt.strftime('%Y-%m-%d').fillna("")
                                if "Amount" in clean_df.columns:
                                    clean_df["Amount"] = pd.to_numeric(clean_df["Amount"], errors='coerce').fillna(0)
                                
                                # Fill blank Month from Date (e.g. "2023-04-30" → "Apr")
                                if "Month" in clean_df.columns and "Date" in clean_df.columns:
                                    def derive_month(row):
                                        val = str(row.get("Month", "")).strip().lower()
                                        if val in ["", "nan", "none"]:
                                            date_val = str(row.get("Date", "")).strip()
                                            try:
                                                return pd.to_datetime(date_val).strftime('%b')
                                            except:
                                                return ""
                                        return row["Month"]
                                    clean_df["Month"] = clean_df.apply(derive_month, axis=1)

                                    
                                # Capture Outstanding column for carry-forward
                                if "Outstanding" in clean_df.columns:
                                    clean_df["Outstanding"] = pd.to_numeric(clean_df["Outstanding"], errors='coerce').fillna(0)
                                else:
                                    clean_df["Outstanding"] = 0.0

                                # *** Capture carry-forward from the VERY FIRST row BEFORE Amount filter ***
                                # The first row in the Excel typically has the opening outstanding balance
                                # (it may have Amount=0 and would be lost after filtering)
                                first_outstanding_val = float(clean_df.iloc[0]["Outstanding"]) if len(clean_df) > 0 else 0.0

                                keep_cols = [c for c in ["Month", "Date", "Narration", "Amount", "Outstanding"] if c in clean_df.columns]
                                final_df = clean_df[keep_cols].copy()
                                final_df.insert(0, "Flat Number", sheet)
                                if "Amount" in final_df.columns:
                                    final_df = final_df[final_df["Amount"] > 0].reset_index(drop=True)

                                # Store the pre-filter first outstanding keyed by sheet name
                                parsed_sheets[sheet] = final_df
                                parsed_sheets[f"__cf_{sheet}"] = first_outstanding_val
                        except Exception as sheet_err:
                            st.error(f"Could not parse flat {sheet}: {sheet_err}")
                    
                    # --- Single Approve All checkbox above all tables ---
                    approve_all = st.checkbox("Approve All Payments", key="approve_all_global")
                    
                    # --- Render each sheet with per-row Approved checkboxes ---
                    all_edited = {}
                    for sheet, final_df in parsed_sheets.items():
                        if sheet.startswith("__cf_"):
                            continue  # skip carry-forward float entries
                        st.markdown(f"#### 📄 Data for Flat: **{sheet}**")
                        final_df.insert(0, "Approved", approve_all)  # pre-fill based on master checkbox
                        edited_df = st.data_editor(
                            final_df,
                            use_container_width=True,
                            hide_index=True,
                            column_config={
                                "Approved": st.column_config.CheckboxColumn(
                                    "Approved",
                                    help="Tick to approve this payment. Use the header checkbox to approve all."
                                ),
                            },
                            key=f"pay_editor_{sheet}"
                        )
                        approved_count = int(edited_df["Approved"].sum())
                        st.markdown(
                            f"<span style='color:#00f0ff;font-size:0.9rem;'>🔢 {approved_count} of {len(edited_df)} payment(s) selected</span>",
                            unsafe_allow_html=True
                        )
                        
                        # --- Show duplicate transactions (already in DB) ---
                        try:
                            import sqlalchemy as _sa
                            _engine = _sa.create_engine(f"mysql+pymysql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}")
                            with _engine.connect() as _conn:
                                # Check which rows from this sheet already exist in DB
                                dup_rows = []
                                for _, row in edited_df.iterrows():
                                    exists = _conn.execute(_sa.text("""
                                        SELECT 1 FROM payment_history
                                        WHERE `Flat Number`=:flat AND `Month`=:month AND `Date`=:date AND `Amount`=:amount
                                        LIMIT 1
                                    """), {
                                        "flat": row.get("Flat Number", ""),
                                        "month": row.get("Month", ""),
                                        "date": str(row.get("Date", "")),
                                        "amount": float(row.get("Amount", 0))
                                    }).fetchone()
                                    if exists:
                                        dup_rows.append(row.drop(labels=["Approved"]))
                                        
                                if dup_rows:
                                    dup_df = pd.DataFrame(dup_rows).reset_index(drop=True)
                                    st.warning(f"⚠️ {len(dup_rows)} transaction(s) already exist in DB (will be skipped on save):")
                                    st.dataframe(dup_df, use_container_width=True, hide_index=True)
                        except Exception:
                            pass  # silently skip if DB not reachable
                        
                        all_edited[sheet] = edited_df

                    
                    # --- Save button: sends only approved rows ---
                    st.markdown("---")
                    if st.button("💾 Save Approved Payments to Database", type="primary"):
                        try:
                            import sqlalchemy
                            engine = sqlalchemy.create_engine(f"mysql+pymysql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}")
                            rows_to_save = []
                            for sheet, edited_df in all_edited.items():
                                approved_rows = edited_df[edited_df["Approved"] == True].drop(columns=["Approved"])
                                rows_to_save.append(approved_rows)
                            combined = pd.concat(rows_to_save, ignore_index=True) if rows_to_save else pd.DataFrame()
                            if len(combined) > 0:
                                with st.spinner("Saving to MySQL..."):
                                    
                                    # Extract UTR/reference from Narration
                                    # Priority: pure numeric tokens (10+ digits) like IMPS UTR
                                    # Fallback: last alphanumeric token that contains digits (NEFT UTR like AXMB231969016421)
                                    def extract_utr(narration):
                                        import re
                                        if not narration or str(narration).strip() in ["", "nan"]:
                                            return None
                                        s = str(narration).upper()
                                        # 1st priority: pure numeric sequence of 10+ digits (IMPS UTR)
                                        numeric_tokens = re.findall(r'\b\d{10,}\b', s)
                                        if numeric_tokens:
                                            return numeric_tokens[0]
                                        # 2nd priority: alphanumeric token containing digits (NEFT-style UTR)
                                        mixed_tokens = [t for t in re.findall(r'[A-Z0-9]{8,}', s) if any(c.isdigit() for c in t)]
                                        if mixed_tokens:
                                            return mixed_tokens[-1]
                                        return None
                                    
                                    combined["narration_ref"] = combined.get("Narration", pd.Series([""] * len(combined))).apply(extract_utr)
                                    
                                    with engine.connect() as conn:
                                        # Composite unique key: (Flat Number, Month, Date, Amount)
                                        # narration_ref stored for reference but NOT used as unique key
                                        # Create payment_history with Outstanding column
                                        conn.execute(sqlalchemy.text("""
                                            CREATE TABLE IF NOT EXISTS payment_history (
                                                id INT AUTO_INCREMENT PRIMARY KEY,
                                                `Flat Number` VARCHAR(50),
                                                `Month` VARCHAR(20),
                                                `Date` VARCHAR(20),
                                                `Narration` TEXT,
                                                `Amount` DECIMAL(12,2),
                                                `Outstanding` DECIMAL(12,2) DEFAULT 0,
                                                `narration_ref` VARCHAR(100),
                                                UNIQUE KEY uq_payment (`Flat Number`, `Month`, `Date`, `Amount`)
                                            )
                                        """))
                                        # Add Outstanding column if it doesn't exist yet (migration)
                                        try:
                                            conn.execute(sqlalchemy.text("""
                                                ALTER TABLE payment_history
                                                ADD COLUMN IF NOT EXISTS `Outstanding` DECIMAL(12,2) DEFAULT 0
                                            """))
                                        except Exception:
                                            pass  # column already exists
                                        # Create carry-forward table
                                        conn.execute(sqlalchemy.text("""
                                            CREATE TABLE IF NOT EXISTS flat_carry_forward (
                                                id INT AUTO_INCREMENT PRIMARY KEY,
                                                `Flat Number` VARCHAR(50) UNIQUE,
                                                `Outstanding` DECIMAL(12,2) DEFAULT 0,
                                                `updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                                            )
                                        """))
                                        conn.commit()
                                        
                                        # Insert row by row using INSERT IGNORE to skip duplicates
                                        inserted = 0
                                        skipped_rows = []
                                        for _, row in combined.iterrows():
                                            try:
                                                result = conn.execute(sqlalchemy.text("""
                                                    INSERT IGNORE INTO payment_history 
                                                    (`Flat Number`, `Month`, `Date`, `Narration`, `Amount`, `Outstanding`, `narration_ref`)
                                                    VALUES (:flat, :month, :date, :narration, :amount, :outstanding, :ref)
                                                """), {
                                                    "flat": row.get("Flat Number", ""),
                                                    "month": row.get("Month", ""),
                                                    "date": str(row.get("Date", "")),
                                                    "narration": row.get("Narration", ""),
                                                    "amount": float(row.get("Amount", 0)),
                                                    "outstanding": float(row.get("Outstanding", 0)),
                                                    "ref": row.get("narration_ref", None)
                                                })
                                                if result.rowcount > 0:
                                                    inserted += 1
                                                else:
                                                    skipped_rows.append(row.drop(labels=["narration_ref"], errors="ignore"))
                                            except Exception:
                                                skipped_rows.append(row.drop(labels=["narration_ref"], errors="ignore"))
                                        conn.commit()

                                        # --- Store first Outstanding per flat into flat_carry_forward ---
                                        # Use the pre-filter first row Outstanding captured during parsing
                                        # (NOT groupby on filtered data — that picks wrong rows like September)
                                        cf_saved = 0
                                        for sheet_name in all_edited.keys():
                                            cf_key = f"__cf_{sheet_name}"
                                            if cf_key in parsed_sheets:
                                                first_outstanding = parsed_sheets[cf_key]
                                                try:
                                                    conn.execute(sqlalchemy.text("""
                                                        INSERT INTO flat_carry_forward (`Flat Number`, `Outstanding`)
                                                        VALUES (:flat, :outstanding)
                                                        ON DUPLICATE KEY UPDATE `Outstanding` = VALUES(`Outstanding`)
                                                    """), {
                                                        "flat": sheet_name,
                                                        "outstanding": float(first_outstanding),
                                                    })
                                                    cf_saved += 1
                                                except Exception:
                                                    pass
                                        conn.commit()
                                 
                                skipped = len(skipped_rows)
                                st.success(f"✅ {inserted} payment(s) saved. {skipped} duplicate(s) skipped.")
                                st.info(f"📌 Carry forward amount stored/updated for {cf_saved} flat(s).")
                                if skipped_rows:
                                    st.warning(f"⚠️ The following {skipped} transaction(s) were skipped (already in DB):")
                                    skipped_df = pd.DataFrame(skipped_rows).reset_index(drop=True)
                                    st.dataframe(skipped_df, use_container_width=True, hide_index=True)

                            else:
                                st.warning("⚠️ No payments selected. Tick at least one row before saving.")
                        except Exception as db_err:
                            st.error(f"❌ Failed to save to database: {db_err}")
                    
        except Exception as e:
            st.error(f"Error reading uploaded file: {e}")


# --- Settings Menu ---
elif app_mode == "⚙️ Settings":
    st.title("⚙️ Global Society Settings")
    st.markdown('<p class="info-text">Configure global parameters like default maintenance fees and penalty grace periods.</p>', unsafe_allow_html=True)
    
    current_settings = load_settings()
    
    st.markdown("### 🧮 Maintenance Defaults")
    with st.form("settings_form"):
        col1, col2 = st.columns(2)
        with col1:
            new_base = st.number_input("Base Monthly Maintenance (₹)", min_value=0.0, value=float(current_settings["base_maintenance"]), step=100.0)
            new_grace = st.number_input("Penalty Applied After Day of Month", min_value=1, max_value=31, value=int(current_settings["grace_period_day"]), help="e.g., If set to 10, penalties apply after the 10th of the month.")
        with col2:
            new_penalty_apr = st.number_input("Late Payment Annual Interest Rate (%)", min_value=0.0, value=float(current_settings.get("penalty_apr", 18.0)), step=1.0, help="Annual Percentage Rate. Will divide by 12 for monthly calculation.")

        st.markdown("---")
        st.markdown("#### 📧 Email Receipt Settings (Brevo SMTP)")
        st.caption("Sign up free at [brevo.com](https://www.brevo.com) → SMTP & API → Copy SMTP Key")
        ecol1, ecol2 = st.columns(2)
        with ecol1:
            new_sender_email = st.text_input("📤 Sender Email Address", value=current_settings.get("gmail_sender_email", "bhumisilveriiocwing@gmail.com"), help="The email address receipts are sent FROM (must be verified in Brevo).")
            new_brevo_login = st.text_input("👤 Brevo Login Email", value=current_settings.get("brevo_login", ""), help="Your Brevo account email (used as SMTP username).")
        with ecol2:
            new_brevo_key = st.text_input("🔑 Brevo SMTP Key", value=current_settings.get("brevo_smtp_key", ""), type="password", help="Found in Brevo dashboard → SMTP & API → SMTP Keys.")

        submitted = st.form_submit_button("💾 Save Settings", type="primary")
        if submitted:
            new_settings = {
                "base_maintenance": new_base,
                "penalty_apr": new_penalty_apr,
                "grace_period_day": new_grace,
                "gmail_sender_email": new_sender_email,
                "brevo_login": new_brevo_login,
                "brevo_smtp_key": new_brevo_key,
            }
            save_settings(new_settings)
            st.success("✅ Society settings updated successfully!")




    # --- Database Connection Settings ---
    st.markdown("---")
    st.markdown("### 🗄️ Database Connection")
    st.markdown('<p class="info-text">Configure and test your MySQL database connection.</p>', unsafe_allow_html=True)

    with st.form("db_settings_form"):
        col1, col2 = st.columns(2)
        with col1:
            new_db_host = st.text_input("Host", value=db_host)
            new_db_user = st.text_input("Username", value=db_user)
            new_db_name = st.text_input("Database Name", value=db_name)
        with col2:
            new_db_port = st.text_input("Port", value=str(db_port))
            new_db_pass = st.text_input("Password", value=db_pass, type="password")

        col_save, col_test = st.columns(2)
        with col_save:
            save_db_btn = st.form_submit_button("💾 Save DB Config", type="primary", use_container_width=True)
        with col_test:
            test_db_btn = st.form_submit_button("🔌 Test Connection", use_container_width=True)

    if save_db_btn:
        save_db_config({"host": new_db_host, "port": new_db_port, "user": new_db_user, "password": new_db_pass, "database": new_db_name})
        st.success("✅ DB config saved! Restart the app to apply new connection settings.")

    if test_db_btn:
        if not new_db_port.strip().isdigit():
            st.error("⚠️ Please enter a valid Port number.")
        else:
            try:
                import sqlalchemy
                test_engine = sqlalchemy.create_engine(f"mysql+pymysql://{new_db_user}:{new_db_pass}@{new_db_host}:{new_db_port}/{new_db_name}")
                with test_engine.connect() as conn:
                    host_result = conn.execute(sqlalchemy.text("SELECT @@hostname, @@version")).fetchone()
                    tables_result = conn.execute(sqlalchemy.text("SHOW TABLES")).fetchall()
                    tables = [row[0] for row in tables_result]
                st.success(f"✅ Connected to **{new_db_host}:{new_db_port}/{new_db_name}**")
                st.caption(f"🖥️ Server: `{host_result[0]}` | MySQL v{host_result[1]}")
                st.caption(f"📋 Tables: `{'`, `'.join(tables)}`" if tables else "📋 No tables found yet.")
            except Exception as e:
                st.error(f"❌ Connection failed: {e}")

    # --- Sync to MySQL ---
    st.markdown("### 🔄 Sync Statement Data to MySQL")
    if st.button("🔄 Sync to MySQL", use_container_width=True):
        if "df_stmt" not in st.session_state or st.session_state.df_stmt is None or len(st.session_state.df_stmt) == 0:
            st.error("⚠️ No Account Statement data loaded to sync.")
        elif "df_rec" not in st.session_state or st.session_state.df_rec is None or len(st.session_state.df_rec) == 0:
            st.error("⚠️ No Bank Reconciliation data loaded to sync.")
        else:
            try:
                import sqlalchemy, re
                engine = sqlalchemy.create_engine(f"mysql+pymysql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}")
                def sanitize_col(col_name):
                    name = str(col_name).strip()
                    if len(name) > 60: name = name[:60]
                    name = re.sub(r'[^a-zA-Z0-9]', '_', name)
                    return name.strip('_')
                with st.spinner("Syncing data to MySQL..."):
                    sql_df_stmt = st.session_state.df_stmt.copy()
                    sql_df_rec = st.session_state.df_rec.copy()
                    sql_df_stmt.columns = [sanitize_col(c) for c in sql_df_stmt.columns]
                    sql_df_rec.columns = [sanitize_col(c) for c in sql_df_rec.columns]
                    sql_df_stmt.to_sql(name="account_statement", con=engine, if_exists="replace", index=False)
                    sql_df_rec.to_sql(name="bank_reconciliation", con=engine, if_exists="replace", index=False)
                st.success(f"✅ Successfully synced tables to `{db_name}`.")
            except Exception as e:
                st.error(f"❌ Database Error: {e}")


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
