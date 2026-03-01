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
def load_data(file_source):
    try:
        # Load excel skipping the first 22 rows (header stuff)
        df = pd.read_excel(file_source, sheet_name=0, header=None, skiprows=22)
        
        # Clean up column names since it's hard to read header=None
        # The typical structure seen was: Date, Narration, RefNo, ValueDate, Withdrawal, Deposit, Balance
        # We will dynamically rename the first 7 columns for easier indexing if they exist
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
        
        # Convert all to string to make searching easier, fill NaNs
        df = df.fillna("")
        for col in df.columns:
            df[col] = df[col].astype(str)
            
        return df
    except Exception as e:
        st.error(f"Error parsing file: {e}")
        return None

# --- File Upload ---
uploaded_file = st.file_uploader("📥 Upload your Account Statement Excel file (.xls, .xlsx)", type=["xls", "xlsx"])

if uploaded_file is not None:
    df = load_data(uploaded_file)
    if df is None:
        st.stop()
else:
    st.info("👆 Please select an Excel Account Statement to begin analysis.")
    st.stop()

# --- Search Interface ---
col1, col2 = st.columns([3, 1])

with col1:
    search_query = st.text_input("🔍 Search Query", placeholder="e.g. C1 301, MEGHA, or 0000000569967118...")

with col2:
    st.markdown("<br>", unsafe_allow_html=True) # spacing
    search_col_opt = st.selectbox("Search in", ["Both Col B & C", "Only Col B (Narration)", "Only Col C (Ref No)"])

st.markdown("---")

# --- Filtering Logic ---
if search_query:
    import re
    # Validate that no space is used in flat number formats (e.g. "C1 " or "- " is not allowed)
    if re.search(r'[a-zA-Z]\d\s', search_query) or '- ' in search_query or ' -' in search_query:
        st.warning("⚠️ Spaces are not allowed in the flat number search. Please use the exact format CX-XXXX (e.g., C1-301).")
        st.stop()
        
    # Remove spaces and hyphens from the query for flexible matching against the Excel data
    query = re.sub(r'[\s\-]', '', search_query.lower())
    
    col_b_name = "Narration (Col B)" if "Narration (Col B)" in df.columns else df.columns[1]
    col_c_name = "Ref No (Col C)" if "Ref No (Col C)" in df.columns else df.columns[2]
    
    # Create normalized versions of the columns for searching (ignoring spaces and hyphens)
    b_normalized = df[col_b_name].str.lower().str.replace(r'[\s\-]', '', regex=True)
    c_normalized = df[col_c_name].str.lower().str.replace(r'[\s\-]', '', regex=True)
    
    if search_col_opt == "Both Col B & C":
        mask = b_normalized.str.contains(query) | c_normalized.str.contains(query)
    elif search_col_opt == "Only Col B (Narration)":
        mask = b_normalized.str.contains(query)
    else:
        mask = c_normalized.str.contains(query)
        
    filtered_df = df[mask]
else:
    filtered_df = df

# --- Display Results ---

# Metrics Row
col_m1, col_m2 = st.columns(2)
with col_m1:
    st.metric(label="Matches Found", value=len(filtered_df))
with col_m2:
    if search_query:
        st.metric(label="Total Search Status", value="Filtered", delta="Active Search")
    else:
        st.metric(label="Total Search Status", value="Showing All", delta="No Filter", delta_color="off")

st.markdown("### 📋 Transaction Records")
st.markdown('<p class="info-text" style="font-size: 0.9rem;">Click any cell and press <kbd>Ctrl</kbd>+<kbd>C</kbd> (or <kbd>Cmd</kbd>+<kbd>C</kbd>) to copy its contents.</p>', unsafe_allow_html=True)

# Use st.dataframe for an interactive table
if len(filtered_df) > 0:
    col_b_name = "Narration (Col B)" if "Narration (Col B)" in df.columns else df.columns[1]
    
    # We will build an HTML table that includes a copy button for each row using JavaScript.
    # This allows the requested premium per-row copy button functionality.
    
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
    for col in filtered_df.columns:
        html_table += f"<th>{col}</th>"
    html_table += "<th>Action</th></tr></thead><tbody>"
    
    # Render table rows
    import json
    
    # Check if there are too many records for smooth custom HTML rendering
    # Render at most the first 500 rows to prevent breaking the browser
    max_rows = min(len(filtered_df), 500)
    for idx, row in filtered_df.head(max_rows).iterrows():
        html_table += "<tr>"
        
        # Safely escape for javascript using json.dumps
        raw_narration = str(row.get(col_b_name, ""))
        js_safe_narration = json.dumps(raw_narration).replace('"', '&quot;')
        
        for col in filtered_df.columns:
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
    if len(filtered_df) > 500:
        html_table += f"<div style='text-align: center; color: #fca311; padding: 10px;'>Showing first 500 of {len(filtered_df)} records. Please use the search bar to refine your query.</div>"
    
    st.components.v1.html(html_table, height=600, scrolling=True)

else:
    st.warning("No records found matching your query. Try searching for a different term.")

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
