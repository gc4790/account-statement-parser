import streamlit as st
import pandas as pd
import os
import json
import sqlalchemy
import re
from sqlalchemy import text as _text

SETTINGS_FILE = "society_settings.json"

def load_settings():
    default_settings = {
        "base_maintenance": 2500.0,
        "tenant_maintenance": 3000.0,
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
    default = {"host": "localhost", "port": "3306", "user": "root", "password": "root", "database": "society_plus", "use_ssl": False}
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


def get_engine():
    """Create a SQLAlchemy engine from db_config.json. Enables SSL for cloud DBs like TiDB."""
    _c = load_db_config()
    _url = f"mysql+pymysql://{_c['user']}:{_c['password']}@{_c['host']}:{_c['port']}/{_c['database']}"
    if _c.get("use_ssl", False):
        return sqlalchemy.create_engine(_url, connect_args={"ssl": {"ssl_mode": "VERIFY_IDENTITY"}})
    return sqlalchemy.create_engine(_url)


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
                <tr><td style='padding:6px;color:#555;'>Total Principal Due</td><td style='text-align:right;'><strong>&#8377;{final_principal:,.2f}</strong></td></tr>
                <tr><td style='padding:6px;color:#c0392b;'>Total Penalty Due</td><td style='text-align:right;color:#c0392b;'><strong>&#8377;{final_penalty:,.2f}</strong></td></tr>
                <tr style='border-top:2px solid #ddd;'><td style='padding:8px 6px;color:#000;'><strong>Final Total Obligation</strong></td><td style='text-align:right;color:#000;'><strong>&#8377;{(final_principal + final_penalty):,.2f}</strong></td></tr>
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



# --- Multi-Payment Breakdown Popup ---
@st.dialog("💳 Payment Breakdown", width="large")
def _show_payment_breakdown(month_label, txns):
    import pandas as _pd_dlg
    total = sum(float(t.get("Amount", 0)) for t in txns)
    st.markdown(f"### {month_label}")
    st.markdown(f"`{len(txns)}` payments &nbsp;·&nbsp; **Total: ₹{total:,.2f}**")
    st.dataframe(
        _pd_dlg.DataFrame([
            {"#": i, "Date": t.get("Date", ""), "Amount (₹)": float(t.get("Amount", 0)), "Narration": t.get("Narration", "—") or "—"}
            for i, t in enumerate(txns, 1)
        ]),
        use_container_width=True,
        hide_index=True,
        column_config={
            "#": st.column_config.NumberColumn("#", width="small"),
            "Amount (₹)": st.column_config.NumberColumn("Amount (₹)", format="₹%.2f"),
        }
    )


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

# --- User Authentication & Session State ---
if "logged_in" not in st.session_state:
    st.session_state.logged_in = False
    st.session_state.username = None
    st.session_state.role = None

def init_auth_db():
    try:
        engine = get_engine()
        with engine.connect() as conn:
            conn.execute(sqlalchemy.text("""
                CREATE TABLE IF NOT EXISTS app_users (
                    username VARCHAR(50) PRIMARY KEY,
                    password_hash VARCHAR(255) NOT NULL,
                    role VARCHAR(20) NOT NULL
                )
            """))
            res = conn.execute(sqlalchemy.text("SELECT COUNT(*) FROM app_users")).scalar()
            if res == 0:
                from passlib.hash import pbkdf2_sha256
                default_hash = pbkdf2_sha256.hash("admin123")
                conn.execute(sqlalchemy.text("INSERT INTO app_users (username, password_hash, role) VALUES ('admin', :hash, 'admin')"), {"hash": default_hash})
                conn.commit()
    except Exception as e:
        # Silently fail if DB isn't configured yet (allows Settings to be visible on first install if we bypass)
        pass

init_auth_db()

# --- Login UI ---
if not st.session_state.logged_in:
    st.markdown("<br><h2 style='text-align: center; color: #fca311;'>🔒 Login to Society Plus</h2>", unsafe_allow_html=True)
    c1, c2, c3 = st.columns([1, 2, 1])
    with c2:
        with st.form("login_form"):
            username = st.text_input("Username")
            password = st.text_input("Password", type="password")
            submitted = st.form_submit_button("Login", use_container_width=True)
            
            if submitted:
                try:
                    engine = get_engine()
                    with engine.connect() as conn:
                        user_row = conn.execute(sqlalchemy.text("SELECT password_hash, role FROM app_users WHERE username = :u"), {"u": username}).fetchone()
                        if user_row:
                            from passlib.hash import pbkdf2_sha256
                            is_valid = False
                            try:
                                is_valid = pbkdf2_sha256.verify(password, user_row[0])
                            except ValueError:
                                try:
                                    from passlib.hash import bcrypt
                                    is_valid = bcrypt.verify(password, user_row[0])
                                except:
                                    pass

                            if is_valid:
                                st.session_state.logged_in = True
                                st.session_state.username = username
                                st.session_state.role = user_row[1]
                                st.rerun()
                            else:
                                st.error("❌ Incorrect password")
                        else:
                            st.error("❌ Username not found")
                except Exception as e:
                    st.error(f"❌ Database connection error. Check your DB config. Details: {e}")
    st.stop()


# --- Navigation ---
st.sidebar.markdown(f"👤 **Logged in as:** `{st.session_state.username}`")
st.sidebar.caption(f"🛡️ Role: **{str(st.session_state.role).upper()}**")
if st.sidebar.button("🚪 Logout", use_container_width=True):
    st.session_state.logged_in = False
    st.session_state.username = None
    st.session_state.role = None
    st.rerun()
st.sidebar.divider()

st.sidebar.markdown("## 🧭 Navigation")

# Hide Settings from Viewer role
nav_options = ["🔍 Transaction Search", "🏢 Flat Management"]
if st.session_state.role in ["admin", "manager"]:
    nav_options.append("📤 Bulk Upload")
if st.session_state.role == "admin":
    nav_options.append("✅ Pending Approvals")
    nav_options.append("👥 User Management")
    nav_options.append("⚙️ Settings")

nav_options.append("🔑 Change Password")

app_mode = st.sidebar.radio("Select View:", nav_options)

# --- Load DB config globally (used across all pages) ---
_cfg = load_db_config()
db_host = _cfg["host"]
db_port = _cfg["port"]
db_user = _cfg["user"]
db_pass = _cfg["password"]
db_name = _cfg["database"]
db_ssl = _cfg.get("use_ssl", False)


if "df_stmt" not in st.session_state:
    st.session_state.df_stmt = None
if "df_rec" not in st.session_state:
    st.session_state.df_rec = None
if "calc_key" not in st.session_state:
    st.session_state.calc_key = 0

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
        _eng_shared = get_engine()
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
                _eng_srch = get_engine()
                _df_srch = pd.read_sql(
                    f"SELECT * FROM flat_details WHERE `Flat No` = '{selected_flat}' LIMIT 1",
                    con=_eng_srch,
                )
                if not _df_srch.empty:
                    info = _df_srch.iloc[0]
                    flat_no = info.get("Flat No", "N/A")
                    owner = info.get("Owner Name", "N/A")
                    
                    # Normalize Rented Status to Y/N
                    _rs_raw = str(info.get("Rented Status", "No")).strip().upper()
                    _default_rs = "Y" if _rs_raw in ["Y", "YES"] else "N"
                    
                    tenant = info.get("Tenant Name", "N/A")
                    contact = info.get("Contact Number", "N/A")
                    f_type = info.get("Flat Type", "N/A")
                    f_area = info.get("Area (sq ft)", "N/A")
                    
                    st.markdown(f"### 🚪 Flat {flat_no}")
                    
                    with st.form(f"edit_flat_{flat_no}"):
                        c1, c2, c3 = st.columns(3)
                        with c1:
                            st.write(f"**👤 Owner:** {owner}")
                            st.write(f"**📞 Contact:** {contact}")
                        with c2:
                            st.write(f"**🏠 Type:** {f_type}")
                            st.write(f"**📐 Area:** {f_area} sq ft")
                        with c3:
                            new_rs = st.selectbox("🔑 Rented Status", ["N", "Y"], index=0 if _default_rs == "N" else 1, help="Y = Rented, N = Owner Occupied")
                            st.write(f"**🧑‍🤝‍🧑 Tenant:** {tenant if _default_rs == 'Y' else 'N/A'}")
                        
                        if st.form_submit_button("💾 Save Flat Details", type="primary"):
                            try:
                                with get_engine().connect() as _conn_edit:
                                    _conn_edit.execute(sqlalchemy.text(
                                        "UPDATE flat_details SET `Rented Status` = :rs WHERE `Flat No` = :f"
                                    ), {"rs": new_rs, "f": flat_no})
                                    _conn_edit.commit()
                                st.success(f"✅ Rented Status for {flat_no} updated to '{new_rs}'!")
                                st.rerun()
                            except Exception as _e_upd:
                                st.error(f"❌ Failed to update DB: {_e_upd}")
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
    
    # State variable for the selected dataframe to pass to the preview section
    selected_tx_df = None
    
    tab1, tab2 = st.tabs(["📊 Account Statement Records", "🏦 Bank Reconciliation Status"])
    
    with tab1:
        if len(filtered_df_stmt) > 0:
            st.info("💡 **Select one or more rows** below to preview and submit them for Admin approval.")
            max_rows = min(len(filtered_df_stmt), 500)
            
            selection_stmt = st.dataframe(
                filtered_df_stmt.head(max_rows),
                use_container_width=True,
                selection_mode="multi-row",
                on_select="rerun",
                hide_index=True,
                height=400
            )
            
            if len(selection_stmt.selection.rows) > 0:
                selected_indices = selection_stmt.selection.rows
                selected_tx_df = filtered_df_stmt.head(max_rows).iloc[selected_indices].copy()
                
            if len(filtered_df_stmt) > 500:
                st.caption(f"Showing first 500 of {len(filtered_df_stmt)} statement records.")
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

    st.markdown("---")
    
    if selected_tx_df is not None and not selected_tx_df.empty:
        st.markdown("### 📥 Preview & Submit Selected Payments")
        st.success(f"✅ Picked {len(selected_tx_df)} transaction(s) for submission. Please assign a Flat Number and Ledger details for each.")
        
        # 1. Prepare dynamic flat list for dropdown validation
        _flat_list = []
        try:
            _eng_f = get_engine()
            _df_f = pd.read_sql("SELECT `Flat No` FROM flat_details", con=_eng_f)
            _flat_list = sorted([str(f).strip() for f in _df_f['Flat No'].dropna().unique() if str(f).strip()])
        except Exception:
            pass
            
        # UI for Manual Multi-Flat Override
        st.markdown("#### 🛠️ Manual Split Settings")
        col_m1, col_m2 = st.columns([1, 2])
        with col_m1:
            enable_manual_split = st.checkbox("Apply Multi-Flat Split (Manual)", key="manual_split_check")
        with col_m2:
            override_flats = []
            if enable_manual_split:
                override_flats = st.multiselect("Select Flats for Split", options=_flat_list, key="manual_split_flats")
                if override_flats:
                    st.caption(f"💡 All selected transactions will be split {len(override_flats)}-way.")

        # 2. Re-format the selected dataset into a cleanly structured Pending schema
        preview_data = []
        months = ["Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec", "Jan", "Feb", "Mar"]
        
        _dropdown_list = [""] + _flat_list # List for the selectbox in the editor
        
        for _, row in selected_tx_df.iterrows():
            # --- Extract standard fields exactly as we did before ---
            p_date = ""
            p_month = "Apr"
            p_year = 2024
            date_col = next((c for c in row.index if 'date' in str(c).lower()), None)
            if date_col:
                try:
                    _dt = pd.to_datetime(row[date_col], dayfirst=True)
                    p_date = _dt.strftime("%Y-%m-%d")
                    p_month = _dt.strftime("%b")
                    p_year = _dt.year
                except:
                    p_date = str(row[date_col])
                    
            p_amount = 0.0
            amount_cols = [c for c in row.index if any(x in str(c).lower() for x in ['amount', 'credit', 'deposit'])]
            if amount_cols:
                for ac in amount_cols:
                    try:
                        val = float(str(row[ac]).replace(',', '').strip())
                        if val > 0:
                            p_amount = float(val)
                            break
                    except:
                        continue
                        
            p_narration = ""
            narration_cols = [c for c in row.index if any(x in str(c).lower() for x in ['narration', 'particular', 'description'])]
            if not narration_cols and len(row.index) > 1:
                narration_cols = [row.index[1]]
            if narration_cols:
                p_narration = str(row[narration_cols[0]])
                
            # --- AI: Auto-Extract Flat Number(s) from Narration ---
            detected_flats = []
            
            # 1. Global Override: If the user searched for specific Flat patterns, use them!
            if search_query:
                # Find all flat patterns in the search query
                search_matches = re.findall(r'C\d?-\d{3,4}', search_query.upper())
                for extracted in search_matches:
                    if extracted not in _flat_list:
                        _flat_list.append(extracted)
                    if extracted not in detected_flats:
                        detected_flats.append(extracted)

            # 2. Row Fallback: Scan the row's narration text if no query matches
            if not detected_flats and p_narration:
                narration_upper = p_narration.upper()
                narration_matches = re.findall(r'C\d?-\d{3,4}', narration_upper)
                for extracted in narration_matches:
                    # Try to map to existing or force new
                    if extracted not in _flat_list:
                        _flat_list.append(extracted)
                    if extracted not in detected_flats:
                        detected_flats.append(extracted)
                
                # If still nothing, fallback to pure substring match for known flats
                if not detected_flats and len(_flat_list) > 1:
                    for f_no in _flat_list:
                        if f_no and f_no.upper() in narration_upper:
                            detected_flats.append(f_no)
                            break
            
            # --- Build Preview Data (Handle Split Payments) ---
            final_detected_flats = []
            if enable_manual_split and override_flats:
                final_detected_flats = override_flats
            else:
                final_detected_flats = detected_flats

            if len(final_detected_flats) > 1:
                # N-way Split between detected/manual flats
                split_amount = p_amount / len(final_detected_flats)
                for f_no in final_detected_flats:
                    preview_data.append({
                        "Assign Flat No": f_no, 
                        "Ledger Month": p_month, 
                        "Ledger Year": p_year,
                        "Amount (₹)": split_amount,
                        "Date": p_date,
                        "Narration Ref": p_narration
                    })
            else:
                # Single flat or no flat detected
                final_flat = final_detected_flats[0] if final_detected_flats else ""
                preview_data.append({
                    "Assign Flat No": final_flat, 
                    "Ledger Month": p_month, 
                    "Ledger Year": p_year,
                    "Amount (₹)": p_amount,
                    "Date": p_date,
                    "Narration Ref": p_narration
                })
            
        preview_df = pd.DataFrame(preview_data)
        
        # 3. Create the Editable Dataframe
        edited_preview = st.data_editor(
            preview_df,
            column_config={
                "Assign Flat No": st.column_config.SelectboxColumn(
                    "Flat Number",
                    help="Assign the flat. Auto-detected from narration if available.",
                    width="medium",
                    options=_dropdown_list,
                    required=True
                ),
                "Ledger Month": st.column_config.SelectboxColumn(
                    "Month",
                    options=months,
                    required=True
                ),
                "Ledger Year": st.column_config.NumberColumn(
                    "Year",
                    min_value=2020,
                    max_value=2030,
                    step=1,
                    required=True
                ),
                "Amount (₹)": st.column_config.NumberColumn(disabled=True),
                "Date": st.column_config.TextColumn(disabled=True),
                "Narration Ref": st.column_config.TextColumn(disabled=True, width="large"),
            },
            hide_index=True,
            use_container_width=True,
            num_rows="fixed"
        )
        
        if st.button("📤 Bulk Submit to Pending Approvals", type="primary"):
            errors = []
            success_count = 0
            
            engine = get_engine()
            
            with engine.begin() as _conn: # Using explicit transaction
                # Ensure table exists
                _conn.execute(sqlalchemy.text("""
                    CREATE TABLE IF NOT EXISTS pending_payments (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        `Flat Number` VARCHAR(50),
                        `Month` VARCHAR(20),
                        `Date` VARCHAR(20),
                        `Narration` TEXT,
                        `Amount` DECIMAL(12,2),
                        `narration_ref` VARCHAR(100),
                        `submitted_by` VARCHAR(50),
                        `submitted_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """))
                
                # Insert each edited row
                for idx, row in edited_preview.iterrows():
                    m_flat = str(row["Assign Flat No"]).strip()
                    if not m_flat:
                        errors.append(f"Row {idx+1}: Missing Flat Number.")
                        continue
                        
                    m_month_label = f"{row['Ledger Month']} {row['Ledger Year']}"
                    
                    try:
                        _conn.execute(sqlalchemy.text("""
                            INSERT INTO pending_payments 
                            (`Flat Number`, `Month`, `Date`, `Narration`, `Amount`, `narration_ref`, `submitted_by`)
                            VALUES (:flat, :month, :date, :narration, :amount, :ref, :by)
                        """), {
                            "flat": m_flat,
                            "month": m_month_label,
                            "date": row["Date"],
                            "narration": row["Narration Ref"],
                            "amount": float(row["Amount (₹)"]),
                            "ref": str(row["Narration Ref"])[:100] if row["Narration Ref"] else None,
                            "by": st.session_state.username
                        })
                        success_count += 1
                    except Exception as e:
                        errors.append(f"Row {idx+1}: DB error - {e}")
                        
            if success_count > 0:
                st.success(f"🎉 Successfully submitted {success_count} payment(s) to the Admin Pending Approvals queue!")
            if errors:
                for err in errors:
                    st.error(err)
    else:
        st.info("💡 Make a selection in the Accounts Statement Records table above to view the preview and submit payments for approval.")

    if st.session_state.role == "admin":
        st.markdown("---")
        with st.expander("➕ Add Payment to Flat Ledger", expanded=False):
            st.markdown("Found a missing transaction? Manually add it to a Flat's ledger here.")
            with st.form("manual_payment_form", clear_on_submit=True):
                col_pf1, col_pf2, col_pf3 = st.columns(3)
                with col_pf1:
                    _flat_list_admin = []
                    try:
                        engine = get_engine()
                        _df_admin = pd.read_sql("SELECT `Flat No` FROM flat_details", con=engine)
                        _flat_list_admin = sorted([str(f) for f in _df_admin['Flat No'].dropna().unique() if str(f).strip()])
                    except:
                        pass
                    m_flat = st.selectbox("Select Flat", ["-- Select Flat --"] + _flat_list_admin)
                    m_date = st.date_input("Payment Date", value=pd.Timestamp.now().date())
                
                # Intelligent defaults for manual payment month/year
                _default_month_idx = 0
                _default_year = pd.Timestamp.now().year
                _months_list = ["Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec", "Jan", "Feb", "Mar"]
                if m_date:
                    _abbr = m_date.strftime("%b")
                    if _abbr in _months_list:
                        _default_month_idx = _months_list.index(_abbr)
                    _default_year = m_date.year

                with col_pf2:
                    m_amount = st.number_input("Amount (₹)", min_value=1.0, step=100.0)
                    m_month = st.selectbox("For Ledger Month", _months_list, index=_default_month_idx)
                with col_pf3:
                    m_year = st.number_input("For Ledger Year", min_value=2023, max_value=2030, value=_default_year, step=1)
                    m_narration = st.text_input("Narration / UTR Reference")
                    
                submitted = st.form_submit_button("💾 Save Payment to DB", type="primary", use_container_width=True)
                if submitted:
                    if m_flat == "-- Select Flat --":
                        st.error("❌ Please select a valid Flat No.")
                    elif not m_date:
                        st.error("❌ Please select a payment date.")
                    else:
                        m_date_str = m_date.strftime("%Y-%m-%d %H:%M:%S")
                        m_month_label = f"{m_month} {m_year}"
                        try:
                            engine = get_engine()
                            with engine.connect() as _conn:
                                _conn.execute(sqlalchemy.text("""
                                    INSERT INTO payment_history 
                                    (`Flat Number`, `Month`, `Date`, `Narration`, `Amount`, `Outstanding`, `narration_ref`)
                                    VALUES (:flat, :month, :date, :narration, :amount, 0, :ref)
                                """), {
                                    "flat": m_flat,
                                    "month": m_month_label,
                                    "date": m_date_str,
                                    "narration": m_narration,
                                    "amount": m_amount,
                                    "ref": m_narration[:200] if m_narration else None
                                })
                                _conn.commit()
                            st.success(f"✅ ₹{m_amount:,.2f} added to {m_flat} for {m_month_label}!")
                            st.session_state.calc_key += 1 # force refresh ledger if it's open
                        except Exception as e:
                            st.error(f"❌ DB Error: {e}")

# --- Centralized Bulk Upload Menu ---
elif app_mode == "📤 Bulk Upload":
    st.title("📤 Centralized Bulk Upload")
    st.markdown('<p class="info-text">Manage maintenance payments and resident database uploads in one place.</p>', unsafe_allow_html=True)
    
    # Initialize engine for the entire Bulk Upload section
    engine = get_engine()
    
    if st.session_state.role not in ["admin", "manager"]:
        st.error("🔒 You do not have permission to access Bulk Uploads.")
        st.stop()
        
    tab_payments, tab_residents = st.tabs(["💰 Maintenance Payments", "🏠 Flat & Tenant DB"])

    with tab_payments:
        st.markdown("### 📥 Bulk Maintenance Payments")
        st.info("Upload an Excel file containing parsed maintenance records from your offline tracker.")
        
        pay_file = st.file_uploader("Upload Resident Payments (.xls / .xlsx)", type=["xls", "xlsx"], key="payment_upload")
        
        if pay_file is not None:
            try:
                with st.spinner("Processing file..."):
                    xl = pd.ExcelFile(pay_file)
                    # Filter out helper sheets
                    sheet_names = [s for s in xl.sheet_names if s.strip().lower() not in ["master", "total", "summary"]]
                    
                st.success(f"✅ File loaded successfully! Found {len(sheet_names)} payment sheets.")
                
                st.markdown("---")
                st.markdown("### 🔍 Search & Validate Payments")
                search_flat = st.text_input("Enter Flat Number to extract (e.g., C1-1101)", placeholder="Quick search...")
                
                if search_flat:
                    search_query = search_flat.strip().upper().replace(" ", "")
                    matching_sheets = [s for s in sheet_names if search_query in s.strip().upper().replace(" ", "")]
                    
                    if matching_sheets:
                        st.write(f"Found {len(matching_sheets)} matching flat(s):")
                        parsed_sheets = {}
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
                                    if any(is_start_col(cell) for cell in row_str) and any(str(cell).strip().lower() == 'amount' for cell in row_str):
                                        header_idx = i
                                        for j, cell in enumerate(row_str):
                                            if is_start_col(cell):
                                                start_col_idx = j
                                                break
                                        break
                                        
                                if header_idx is not None and start_col_idx is not None:
                                    clean_df = df_sheet.iloc[header_idx + 1:, start_col_idx : start_col_idx + 9].copy()
                                    clean_df.columns = ["Month", "Last Month Interest", "Outstanding", "Monthly Dues", "Date", "Narration", "Amount", "Balance", "Interest"]
                                    
                                    clean_df = clean_df.dropna(how='all')
                                    if "Date" in clean_df.columns:
                                        clean_df["Date"] = pd.to_datetime(clean_df["Date"], errors='coerce').dt.strftime('%Y-%m-%d').fillna("")
                                    if "Amount" in clean_df.columns:
                                        clean_df["Amount"] = pd.to_numeric(clean_df["Amount"], errors='coerce').fillna(0)
                                    
                                    first_outstanding_val = float(clean_df.iloc[0]["Outstanding"]) if len(clean_df) > 0 else 0.0
                                    
                                    final_df = clean_df[["Month", "Date", "Narration", "Amount", "Outstanding"]].copy()
                                    final_df["Amount"] = pd.to_numeric(final_df["Amount"], errors='coerce').fillna(0)
                                    final_df["Outstanding"] = pd.to_numeric(final_df["Outstanding"], errors='coerce').fillna(0)
                                    final_df.insert(0, "Flat Number", sheet)
                                    final_df = final_df[final_df["Amount"] > 0].reset_index(drop=True)
                                    
                                    parsed_sheets[sheet] = final_df
                                    parsed_sheets[f"__cf_{sheet}"] = first_outstanding_val
                            except Exception as e:
                                st.error(f"Error parsing flat {sheet}: {e}")

                        approve_all = st.checkbox("Select All Payments", key="approve_all_bulk")
                        all_edited = {}
                        for sheet, final_df in parsed_sheets.items():
                            if sheet.startswith("__cf_"): continue
                            final_df.insert(0, "Approved", approve_all)
                            edited_df = st.data_editor(final_df, use_container_width=True, hide_index=True, key=f"bulk_pay_{sheet}")
                            all_edited[sheet] = edited_df

                        if st.button("💾 Save to Database", type="primary"):
                            rows_to_save = [df[df["Approved"]].drop(columns=["Approved"]) for df in all_edited.values()]
                            combined = pd.concat(rows_to_save, ignore_index=True) if rows_to_save else pd.DataFrame()
                            if not combined.empty:
                                try:
                                    with engine.connect() as conn:
                                        inserted = 0
                                        for _, row in combined.iterrows():
                                            res = conn.execute(sqlalchemy.text("""
                                                INSERT IGNORE INTO payment_history 
                                                (`Flat Number`, `Month`, `Date`, `Narration`, `Amount`, `Outstanding`)
                                                VALUES (:flat, :month, :date, :narration, :amount, :outstanding)
                                            """), {
                                                "flat": row["Flat Number"], "month": row["Month"], "date": str(row["Date"]),
                                                "narration": row["Narration"], "amount": float(row["Amount"]), "outstanding": float(row["Outstanding"])
                                            })
                                            if res.rowcount > 0: inserted += 1
                                        conn.commit()
                                    st.success(f"✅ {inserted} payments saved!")
                                except Exception as e:
                                    st.error(f"DB Error: {e}")
            except Exception as e:
                st.error(f"File Error: {e}")

    with tab_residents:
        st.markdown("### 🏠 Tenant & Owner Database")
        if st.session_state.role == "admin":
            flat_file = st.file_uploader("📥 Bulk Upload Resident Details (.xls / .xlsx)", type=["xls", "xlsx"], key="bulk_flat_upload")
        else:
            st.info("🔒 Only Admins can upload Tenant data.")
            flat_file = None

        try:
            df_flat_db = pd.read_sql("SELECT * FROM flat_details", con=engine)
            if flat_file:
                df_upload = pd.read_excel(flat_file, header=None)
                # ... reuse the dynamic header mapping logic ...
                header_idx = None
                for i, row in df_upload.iterrows():
                    if row.astype(str).str.contains(r'Flat\s*No', case=False, na=False).any():
                        header_idx = i; break
                if header_idx is not None:
                    df_upload.columns = df_upload.iloc[header_idx]
                    df_upload = df_upload.iloc[header_idx + 1:].reset_index(drop=True)
                
                # Basic schema matching (kept concise)
                for col in df_flat_db.columns:
                    if col not in df_upload.columns: df_upload[col] = None
                df_flat = df_upload[df_flat_db.columns].dropna(subset=['Flat No'])
            else:
                df_flat = df_flat_db

            edited_df = st.data_editor(df_flat, num_rows="dynamic" if st.session_state.role=="admin" else "fixed", use_container_width=True, hide_index=True)
            if st.session_state.role == "admin" and st.button("💾 Save Resident DB", type="primary"):
                edited_df.to_sql("flat_details", con=engine, if_exists="replace", index=False)
                st.success("✅ Database updated!")
        except Exception as e:
            st.error(f"Database sync error: {e}")

# --- Flat Management Menu ---
elif app_mode == "🏢 Flat Management":
    st.markdown("---")
    # Content remains, but the tab_upload logic is removed below as it's been centralized

    with tab_calc:
        st.markdown("### 🧮 Maintenance Ledger & Penalty Calculator")
        st.markdown('<p class="info-text">Simulate and calculate resident maintenance dues with automatic penalty and overpayment forwarding logic.</p>', unsafe_allow_html=True)
        
        # Pull configuration silently from Global Settings
        society_settings = load_settings()
        owner_dues = float(society_settings.get("base_maintenance", 2500.0))
        tenant_dues = float(society_settings.get("tenant_maintenance", 3000.0))
        penalty_apr = float(society_settings.get("penalty_apr", 18.0))
        grace_day = int(society_settings.get("grace_period_day", 10))
        
        # Determine current fee based on Rented Status
        base_dues = owner_dues
        is_rented_bool = False
        try:
            with get_engine().connect() as _conn_chk:
                _rs_check = _conn_chk.execute(sqlalchemy.text(
                    "SELECT `Rented Status` FROM flat_details WHERE `Flat No` = :f"
                ), {"f": selected_flat}).scalar()
                if str(_rs_check).strip().upper() in ["Y", "YES"]:
                    base_dues = tenant_dues
                    is_rented_bool = True
        except:
            pass
            
        monthly_interest_rate = penalty_apr / 12 / 100
            
        st.info(f"⚙️ **Active Policy for {selected_flat}:** Using {'Tenant' if is_rented_bool else 'Owner'} fee (₹{base_dues:,.0f}/mo). Penalty {penalty_apr}% after day {grace_day}.")
        # selected_flat is the shared selector defined above the tabs
        if selected_flat == "-- Select Flat --":
            st.info("👆 Select a flat from the dropdown above to view the maintenance ledger.")
            st.stop()

        # --- Financial Year selector (Indian FY: Apr YY to Mar YY+1) ---
        current_cal_year = 2026
        fy_start_options = list(range(2023, current_cal_year + 1))
        fy_labels = [f"{y}-{str(y+1)[-2:]}" for y in fy_start_options]
        all_fy_options = ["All Years"] + fy_labels
        
        # Calculate current FY index
        _now_dt = pd.Timestamp.now()
        _cur_fy_start = _now_dt.year if _now_dt.month >= 4 else _now_dt.year - 1
        try:
            default_fy_idx = fy_start_options.index(_cur_fy_start) + 1 # +1 because of "All Years"
        except:
            default_fy_idx = 1
            
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
                "💳 Txns": [""] * num_months,
            })
            st.session_state.calc_key = 0
            st.session_state.calc_year = selected_year
            st.session_state.calc_all_years = all_years_mode
            st.session_state.payment_txns_by_month = {}

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

            # Trim months beyond the current month
            _now = pd.Timestamp.now()
            _month_abbrs = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
            fy_months = [
                (m, y) for m, y in fy_months
                if pd.Timestamp(year=y, month=_month_abbrs.index(m)+1, day=1) <= _now.replace(day=1)
            ]

            months_with_year = [f"{m} {y}" for m, y in fy_months]
            num_months = len(fy_months)
            st.session_state.calc_df = pd.DataFrame({
                "Month": months_with_year,
                "Base Dues": [base_dues] * num_months,
                "Payment Received": [0.0] * num_months,
                "💳 Txns": [""] * num_months,
            })
            st.session_state.payment_txns_by_month = {}

            
            if selected_flat != "-- Select Flat --":
                try:
                    engine = get_engine()
                    # Fetch all payments for the selected flat
                    df_hist = pd.read_sql(
                        f"SELECT `Month`, `Amount`, `Date`, `Narration` FROM payment_history "
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
                                (df_hist['_parsed_date'].isna()) |  # Keep rows with no date for fallback
                                ((df_hist['_parsed_date'] >= fy_start_dt) &
                                 (df_hist['_parsed_date'] <= fy_end_dt))
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

                        # Normalize Month abbreviated to title case
                        df_hist['Month_Final'] = df_hist['Month_Short'].apply(lambda x: str(x).title() if pd.notna(x) and x != '' else '')
                        # If still missing, use the Month column with fallback parser
                        mask_missing = df_hist['Month_Final'] == ''
                        if mask_missing.any():
                            df_hist.loc[mask_missing, 'Month_Final'] = df_hist.loc[mask_missing, 'Month'].apply(parse_month_abbrev_fallback)

                        # Extract Year from the Month string if possible for records where Date is invalid
                        def extract_year_fallback(row):
                            if pd.notna(row['Year_Num']): return int(row['Year_Num'])
                            import re
                            # Look for 4 digits in the Month column
                            match = re.search(r'\d{4}', str(row['Month']))
                            if match: return int(match.group())
                            # Fallback to current year if absolutely unknown
                            return pd.Timestamp.now().year

                        df_hist['Year_Final'] = df_hist.apply(extract_year_fallback, axis=1)

                        # Sum amounts per (Month, Year) and map to calc_df rows
                        monthly_totals = df_hist.groupby(['Month_Final', 'Year_Final'])['Amount'].sum().reset_index()

                        for _, row in monthly_totals.iterrows():
                            label = f"{row['Month_Final']} {int(row['Year_Final'])}"
                            idx = st.session_state.calc_df[st.session_state.calc_df['Month'] == label].index
                            if not idx.empty:
                                st.session_state.calc_df.loc[idx[0], 'Payment Received'] = float(row['Amount'])

                        # Build per-month transaction details for multi-payment detection
                        txns_by_month = {}
                        for _, r in df_hist.iterrows():
                            lbl = f"{r['Month_Final']} {int(r['Year_Final'])}"
                            txns_by_month.setdefault(lbl, []).append({
                                "Amount": float(r.get("Amount", 0)),
                                "Date": str(r.get("Date", "") or ""),
                                "Narration": str(r.get("Narration", "") or "—"),
                            })
                        st.session_state.payment_txns_by_month = txns_by_month

                        # Update 💳 Txns badge in calc_df
                        for lbl, txns in txns_by_month.items():
                            idx = st.session_state.calc_df[st.session_state.calc_df["Month"] == lbl].index
                            if not idx.empty:
                                cnt = len(txns)
                                st.session_state.calc_df.loc[idx[0], "💳 Txns"] = f"🔢 {cnt}" if cnt > 1 else ""

                except Exception as e:
                    st.error(f"⚠️ Error loading ledger for {selected_flat}: {e}")
                    import traceback
                    with st.expander("Debug details"):
                        st.code(traceback.format_exc())
            
            st.session_state.calc_key += 1
            st.rerun()
            
        # Hard-sync Base Dues to the global configuration
        if "calc_df" in st.session_state:
            st.session_state.calc_df["Base Dues"] = base_dues
            
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
                "💳 Txns": st.column_config.TextColumn("💳 Txns", disabled=True, width="small"),
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
                _cf_load_eng = get_engine()
                with _cf_load_eng.connect() as _conn:
                    import sqlalchemy
                    _conn.execute(sqlalchemy.text("""
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
        
        # Inject the "💳 Txns" column from calc_df into our display_df for the final table display
        if "💳 Txns" in st.session_state.calc_df.columns:
            display_df.insert(0, "💳 Txns", st.session_state.calc_df["💳 Txns"].values)
        
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

        st.info("💡 **Tip:** Click any row with a 🔢 badge below to view its full transaction breakdown.")
        
        # Make the dataframe interactive so we can capture clicks
        # We need a unique key that changes if data changes, but sticks around for selection
        table_key = f"ledger_table_{st.session_state.calc_key}_{selected_flat}"
        
        event = st.dataframe(
            display_df, 
            use_container_width=True, 
            hide_index=True,
            selection_mode="single-row",
            on_select="rerun",
            key=table_key
        )
        
        # Check if user clicked a row
        if len(event.selection.rows) > 0:
            selected_row_idx = event.selection.rows[0]
            selected_month = display_df.iloc[selected_row_idx]["Month"]
            # See if there are multiple txns for this month
            _txns_by_month = st.session_state.get("payment_txns_by_month", {})
            if selected_month in _txns_by_month and len(_txns_by_month[selected_month]) > 1:
                _show_payment_breakdown(selected_month, _txns_by_month[selected_month])

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
        if st.session_state.role == "admin":
            if st.button("📧 Send Payment Receipt to Flat Owner", type="primary", use_container_width=True):
                _sett = load_settings()
                _brevo_login = _sett.get("brevo_login", "").strip()
                _brevo_key = _sett.get("brevo_smtp_key", "").strip()
                if not _brevo_login or not _brevo_key:
                    st.error("❌ Brevo credentials not configured. Go to ⚙️ Settings and enter your Brevo Login Email and SMTP Key.")
                else:
                    try:
                        _email_eng = get_engine()
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
                    # closing_principal already includes carry_forward_input
                    total_obligation = final_principal + final_penalties
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
                            _engine = get_engine()
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
                            engine = get_engine()
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
            new_base = st.number_input("Base Monthly Maintenance (Owner) (₹)", min_value=0.0, value=float(current_settings["base_maintenance"]), step=100.0)
            new_grace = st.number_input("Penalty Applied After Day of Month", min_value=1, max_value=31, value=int(current_settings["grace_period_day"]), help="e.g., If set to 10, penalties apply after the 10th of the month.")
        with col2:
            new_tenant_maint = st.number_input("Tenant Monthly Maintenance (₹)", min_value=0.0, value=float(current_settings.get("tenant_maintenance", 3000.0)), step=100.0)
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
                "tenant_maintenance": new_tenant_maint,
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
        new_use_ssl = st.checkbox("🔒 Enable SSL (required for TiDB / PlanetScale)", value=db_ssl)

        col_save, col_test = st.columns(2)
        with col_save:
            save_db_btn = st.form_submit_button("💾 Save DB Config", type="primary", use_container_width=True)
        with col_test:
            test_db_btn = st.form_submit_button("🔌 Test Connection", use_container_width=True)

    if save_db_btn:
        save_db_config({"host": new_db_host, "port": new_db_port, "user": new_db_user, "password": new_db_pass, "database": new_db_name, "use_ssl": new_use_ssl})
        st.success("✅ DB config saved! Restart the app to apply new connection settings.")

    if test_db_btn:
        if not new_db_port.strip().isdigit():
            st.error("⚠️ Please enter a valid Port number.")
        else:
            try:
                import sqlalchemy
                _test_url = f"mysql+pymysql://{new_db_user}:{new_db_pass}@{new_db_host}:{new_db_port}/{new_db_name}"
                _test_args = {"ssl": {"ssl_mode": "VERIFY_IDENTITY"}} if new_use_ssl else {}
                test_engine = sqlalchemy.create_engine(_test_url, connect_args=_test_args)
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
                engine = get_engine()
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

# --- Pending Approvals Menu ---
elif app_mode == "✅ Pending Approvals":
    st.title("✅ Pending Payment Approvals")
    st.markdown('<p class="info-text">Review and approve payments submitted by managers from the Transaction Search page.</p>', unsafe_allow_html=True)
    
    if st.session_state.role != "admin":
        st.error("🔒 You do not have permission to view this page. Only Admins can approve payments.")
        st.stop()
        
    try:
        engine = get_engine()
        with engine.connect() as conn:
            # Check if table exists
            has_table = conn.execute(sqlalchemy.text(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = 'pending_payments'"
            )).scalar() > 0
            
            if has_table:
                pending_df = pd.read_sql("SELECT * FROM pending_payments ORDER BY submitted_at DESC", con=conn)
            else:
                pending_df = pd.DataFrame()
            
        if pending_df.empty:
            st.success("🎉 No pending payments! The queue is currently empty.")
        else:
            st.markdown(f"**{len(pending_df)} payment(s) awaiting approval.**")
            
            # --- Filters ---
            with st.expander("🔍 Filter Pending Approvals", expanded=False):
                f_col1, f_col2, f_col3 = st.columns(3)
                
                # Fetch distinct flats currently in pending queue
                available_flats = ["All"] + sorted(pending_df["Flat Number"].dropna().unique().tolist())
                
                with f_col1:
                    filter_flat = st.selectbox("By Flat Number", available_flats)
                with f_col2:
                    # Convert dates safely to allow filtering
                    pending_df['Date_dt'] = pd.to_datetime(pending_df["Date"], errors='coerce')
                    min_date = pending_df['Date_dt'].min()
                    max_date = pending_df['Date_dt'].max()
                    
                    if pd.notna(min_date) and pd.notna(max_date):
                        date_range = st.date_input("Date Range", value=(min_date.date(), max_date.date()), min_value=min_date.date(), max_value=max_date.date())
                    else:
                        date_range = None
                        st.info("No valid dates found in pending queue.")
                with f_col3:
                    st.write("")
                    st.write("")
                    if st.button("Clear Filters", use_container_width=True):
                        st.rerun()

            # Apply filters
            filtered_pending_df = pending_df.copy()
            if filter_flat != "All":
                filtered_pending_df = filtered_pending_df[filtered_pending_df["Flat Number"] == filter_flat]
                
            if date_range and len(date_range) == 2:
                start_date, end_date = date_range
                # Make sure the parsed date falls within the selected range
                filtered_pending_df = filtered_pending_df[
                    (filtered_pending_df['Date_dt'].dt.date >= start_date) & 
                    (filtered_pending_df['Date_dt'].dt.date <= end_date)
                ]
            
            if filtered_pending_df.empty:
                st.warning("No pending payments match your filters.")
            else:
                st.info("💡 **Select a row below** to approve or reject the payment.")
                display_cols = [c for c in pending_df.columns if c not in ["id", "Date_dt"]]
                
                selection_event = st.dataframe(
                    filtered_pending_df[display_cols],
                    use_container_width=True,
                    selection_mode="multi-row",
                    on_select="rerun",
                    hide_index=True
                )
                
                selected_indices = selection_event.selection.rows
                
                if len(selected_indices) > 0:
                    selected_rows = filtered_pending_df.iloc[selected_indices]
                    total_amt = selected_rows["Amount"].sum()
                    
                    st.markdown("---")
                    st.markdown(f"### 📋 Reviewing {len(selected_rows)} selected payment(s)")
                    
                    c_metric1, c_metric2 = st.columns(2)
                    with c_metric1:
                        st.metric("Total Rows", len(selected_rows))
                    with c_metric2:
                        st.metric("Total Amount", f"₹{total_amt:,.2f}")

                    if len(selected_rows) == 1:
                        selected_row = selected_rows.iloc[0]
                        c1, c2, c3 = st.columns(3)
                        with c1:
                            st.write(f"**Flat:** {selected_row['Flat Number']}")
                            st.write(f"**Month:** {selected_row['Month']}")
                        with c2:
                            st.write(f"**Date:** {selected_row['Date']}")
                            st.write(f"**By:** {selected_row['submitted_by']}")
                        with c3:
                            st.write("**Narration:**")
                            st.write(selected_row["Narration"] if selected_row["Narration"] else "—")
                    
                    col_app, col_rej = st.columns(2)
                    with col_app:
                        btn_label = "✅ Approve Selected" if len(selected_rows) > 1 else "✅ Approve & Move to Ledger"
                        if st.button(btn_label, type="primary", use_container_width=True):
                            progress_bar = st.progress(0)
                            status_text = st.empty()
                            try:
                                with engine.connect() as conn:
                                    success_count = 0
                                    dupe_count = 0
                                    
                                    for i, (_, row) in enumerate(selected_rows.iterrows()):
                                        # Insert with IGNORE
                                        result = conn.execute(sqlalchemy.text("""
                                            INSERT IGNORE INTO payment_history 
                                            (`Flat Number`, `Month`, `Date`, `Narration`, `Amount`, `Outstanding`, `narration_ref`)
                                            VALUES (:flat, :month, :date, :narration, :amount, 0, :ref)
                                        """), {
                                            "flat": row["Flat Number"],
                                            "month": row["Month"],
                                            "date": row["Date"],
                                            "narration": row["Narration"],
                                            "amount": float(row["Amount"]),
                                            "ref": row["narration_ref"]
                                        })
                                        
                                        if result.rowcount > 0:
                                            conn.execute(sqlalchemy.text("DELETE FROM pending_payments WHERE id = :id"), {"id": int(row["id"])})
                                            success_count += 1
                                        else:
                                            dupe_count += 1
                                            
                                        # Update progress
                                        progress_bar.progress((i + 1) / len(selected_rows))
                                    
                                    conn.commit()
                                    if success_count > 0:
                                        st.success(f"✅ Successfully approved {success_count} payment(s).")
                                    if dupe_count > 0:
                                        st.warning(f"⚠️ {dupe_count} duplicate(s) were skipped as they already exist in the ledger.")
                                    
                                    st.session_state.calc_key += 1
                                    st.rerun()
                            except Exception as e:
                                st.error(f"❌ Failed to process approvals: {e}")
                    
                    with col_rej:
                        btn_rej_label = "❌ Reject Selected" if len(selected_rows) > 1 else "❌ Reject & Discard"
                        if st.button(btn_rej_label, use_container_width=True):
                            try:
                                with engine.connect() as conn:
                                    for _, row in selected_rows.iterrows():
                                        conn.execute(sqlalchemy.text("DELETE FROM pending_payments WHERE id = :id"), {"id": int(row["id"])})
                                    conn.commit()
                                st.warning(f"🗑️ {len(selected_rows)} payment submission(s) discarded.")
                                st.rerun()
                            except Exception as e:
                                st.error(f"❌ Failed to reject: {e}")

    except Exception as e:
        st.error(f"❌ DB Error fetching pending payments: {e}")

# --- User Management Menu ---
elif app_mode == "👥 User Management":
    st.title("👥 User Management")
    st.markdown('<p class="info-text">Add, edit, or remove access for society staff and administrators.</p>', unsafe_allow_html=True)
    
    if st.session_state.role != "admin":
        st.error("🔒 You do not have permission to view this page. Only Admins can manage users.")
        st.stop()
        
    tab_list, tab_add, tab_reset = st.tabs(["📋 Existing Users", "➕ Add New User", "🔐 Reset Passwords"])
    
    with tab_list:
        try:
            engine = get_engine()
            with engine.connect() as conn:
                users_df = pd.read_sql("SELECT username, role FROM app_users", con=conn)
                
            st.markdown("### Enrolled Users")
            st.dataframe(
                users_df,
                use_container_width=True,
                hide_index=True
            )
            
            st.markdown("---")
            st.markdown("#### 🗑️ Remove User")
            del_user = st.selectbox("Select User to Remove", ["-- Select User --"] + users_df["username"].tolist())
            if st.button("🚨 Delete Selected User", type="primary"):
                if del_user == "-- Select User --":
                    st.error("Please select a user to delete.")
                elif del_user == st.session_state.username:
                    st.error("🛑 You cannot delete your own active session account!")
                elif del_user == "admin":
                    st.error("🛑 The default 'admin' account cannot be deleted.")
                else:
                    try:
                        with engine.connect() as conn:
                            conn.execute(sqlalchemy.text("DELETE FROM app_users WHERE username = :u"), {"u": del_user})
                            conn.commit()
                        st.success(f"✅ User '{del_user}' was successfully removed.")
                        st.rerun()
                    except Exception as e:
                        st.error(f"❌ Could not delete user: {e}")
        except Exception as e:
            st.error(f"❌ DB Error fetching users: {e}")
            
    with tab_add:
        st.markdown("### Create Fresh Credentials")
        with st.form("add_user_form", clear_on_submit=True):
            c1, c2 = st.columns(2)
            with c1:
                new_username = st.text_input("New Username (no spaces)")
                new_role = st.selectbox("Grant Role", ["viewer", "manager", "admin"], help="Admins can manage settings and users. Managers can upload data. Viewers can only search/print receipts.")
            with c2:
                new_password = st.text_input("Temporary Password", type="password")
                new_password_confirm = st.text_input("Confirm Password", type="password")
                
            submitted = st.form_submit_button("💾 Provision User Account", type="primary", use_container_width=True)
            if submitted:
                new_username = new_username.strip().lower()
                if not new_username:
                    st.error("❌ Username cannot be empty.")
                elif " " in new_username:
                    st.error("❌ Username cannot contain spaces.")
                elif not new_password or new_password != new_password_confirm:
                    st.error("❌ Passwords do not match or are empty.")
                else:
                    try:
                        from passlib.hash import pbkdf2_sha256
                        engine = get_engine()
                        with engine.connect() as conn:
                            # Check if exists
                            exists = conn.execute(sqlalchemy.text("SELECT 1 FROM app_users WHERE username = :u"), {"u": new_username}).fetchone()
                            if exists:
                                st.error(f"❌ Username '{new_username}' already exists. Please choose another.")
                            else:
                                hash_pwd = pbkdf2_sha256.hash(new_password)
                                conn.execute(sqlalchemy.text(
                                    "INSERT INTO app_users (username, password_hash, role) VALUES (:u, :p, :r)"
                                ), {"u": new_username, "p": hash_pwd, "r": new_role})
                                conn.commit()
                                st.success(f"✅ User '{new_username}' created successfully as a '{new_role}'! They can now log in.")
                    except Exception as e:
                        st.error(f"❌ DB Error: {e}")

    with tab_reset:
        st.markdown("### Admin Password Override")
        st.info("As an Admin, you can forcefully overwrite any user's password if they forget it.")
        try:
            engine = get_engine()
            import sqlalchemy as _sa
            with engine.connect() as conn:
                all_usernames = [r[0] for r in conn.execute(_sa.text("SELECT username FROM app_users")).fetchall()]
        except Exception:
            all_usernames = []
            
        with st.form("admin_reset_password_form", clear_on_submit=True):
            reset_target = st.selectbox("Select User", ["-- Select User --"] + all_usernames)
            admin_new_pass = st.text_input("New Password", type="password")
            admin_conf_pass = st.text_input("Confirm New Password", type="password")
            submitted = st.form_submit_button("🚨 Force Reset Password", type="primary")
            
            if submitted:
                if reset_target == "-- Select User --":
                    st.error("❌ Please select a user.")
                elif not admin_new_pass or admin_new_pass != admin_conf_pass:
                    st.error("❌ Passwords do not match or are empty.")
                else:
                    try:
                        from passlib.hash import pbkdf2_sha256
                        engine = get_engine()
                        import sqlalchemy as _sa
                        with engine.connect() as conn:
                            hash_pwd = pbkdf2_sha256.hash(admin_new_pass)
                            conn.execute(_sa.text("UPDATE app_users SET password_hash = :p WHERE username = :u"), {"p": hash_pwd, "u": reset_target})
                            conn.commit()
                            st.success(f"✅ Password for '{reset_target}' has been successfully reset!")
                    except Exception as e:
                        st.error(f"❌ DB Error: {e}")


# --- Change Password Menu (For all users) ---
elif app_mode == "🔑 Change Password":
    st.title("🔑 Change Your Password")
    st.markdown('<p class="info-text">Update your login password to keep your account secure.</p>', unsafe_allow_html=True)
    
    with st.form("change_password_form", clear_on_submit=True):
        col1, col2 = st.columns(2)
        with col1:
            old_pass = st.text_input("Current Password", type="password")
        with col2:
            st.empty() # Spacer
            
        c1, c2 = st.columns(2)
        with c1:
            new_pass = st.text_input("New Password", type="password")
        with c2:
            new_pass_confirm = st.text_input("Confirm New Password", type="password")
            
        submitted = st.form_submit_button("💾 Update Password", type="primary")
        
        if submitted:
            if not old_pass:
                st.error("❌ Please enter your current password.")
            elif not new_pass or new_pass != new_pass_confirm:
                st.error("❌ New passwords do not match or are empty.")
            else:
                try:
                    from passlib.hash import pbkdf2_sha256
                    engine = get_engine()
                    import sqlalchemy as _sa
                    with engine.connect() as conn:
                        user_row = conn.execute(_sa.text("SELECT password_hash FROM app_users WHERE username = :u"), {"u": st.session_state.username}).fetchone()
                        
                        if user_row:
                            is_valid = False
                            try:
                                is_valid = pbkdf2_sha256.verify(old_pass, user_row[0])
                            except ValueError:
                                # Fallback if they had a legacy bcrypt hash
                                try:
                                    from passlib.hash import bcrypt
                                    is_valid = bcrypt.verify(old_pass, user_row[0])
                                except:
                                    pass
                                    
                            if is_valid:
                                hash_pwd = pbkdf2_sha256.hash(new_pass)
                                conn.execute(_sa.text("UPDATE app_users SET password_hash = :p WHERE username = :u"), {"p": hash_pwd, "u": st.session_state.username})
                                conn.commit()
                                st.success("✅ Your password has been updated successfully!")
                            else:
                                st.error("❌ Current password is incorrect.")
                        else:
                            st.error("❌ User account not found.")
                except Exception as e:
                    st.error(f"❌ DB Error: {e}")

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
