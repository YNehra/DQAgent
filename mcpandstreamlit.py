import sqlite3 as sql
import pandas as pd
import re
import os
import json
import requests
from datetime import datetime, timezone
import streamlit as st

# Ensure the temporary directory exists
TEMP_DIR = os.path.join(os.getcwd(), "tmp")
os.makedirs(TEMP_DIR, exist_ok=True)

# Load secrets from Streamlit deployment
azure_openai_api_key = st.secrets["azure_openai_api_key"]
azure_openai_endpoint = st.secrets["azure_openai_endpoint"]
azure_openai_deployment = st.secrets["azure_openai_deployment"]
max_tokens = st.secrets.get("max_tokens", 2000)  # Optional, with fallback

# Function to compute dynamic metrics for a DataFrame
def compute_dynamic_metrics(df, table_name):
    now = datetime.now(timezone.utc).isoformat()
    total_rows = len(df)
    metrics = []

    for col in df.columns:
        col_data = df[col]
        entry = {
            "table": table_name,
            "column": col,
            "snapshot_time": now,
            "type": "metric"
        }
        entry["completeness_pct"] = col_data.notna().sum() / total_rows * 100
        entry["uniqueness_pct"] = col_data.nunique() / total_rows * 100

        if col_data.dtype == object:
            entry["empty_string_pct"] = (col_data == "").sum() / total_rows * 100
            entry["whitespace_issues_pct"] = col_data.dropna().apply(lambda x: isinstance(x, str) and x != x.strip()).mean() * 100
            entry["capitalized_pct"] = col_data.dropna().apply(lambda x: isinstance(x, str) and x.istitle()).mean() * 100
            if "email" in col.lower():
                entry["regex_email_valid_pct"] = col_data.str.contains(r"[^@]+@[^@]+\.[^@]+", na=False).mean() * 100
            if "phone" in col.lower():
                entry["regex_phone_valid_pct"] = col_data.str.contains(r"^\+?[0-9]{10,15}$", na=False).mean() * 100

        if pd.api.types.is_numeric_dtype(col_data):
            entry["zero_values_pct"] = (col_data == 0).sum() / total_rows * 100
            entry["negative_values_pct"] = (col_data < 0).sum() / total_rows * 100

        metrics.append(entry)

    return metrics

# Function to analyze a single file
def analyze_single_file(file_path):
    df = pd.read_csv(file_path)
    table_name = os.path.basename(file_path)
    metrics = compute_dynamic_metrics(df, table_name)

    prompt = f"""
You are a world-class data quality analyst and domain expert. Your task is to analyze the provided table and identify all possible data quality issues.

For each issue, provide:
- **Issue:** [The title or short description of the issue]
- **Details:** [A detailed explanation of the issue]
- **Expected correct state:** [What the correct state should be]
- **Violated constraint:** [Any violated constraints or standards]
- **Location:** [Where the issue is located]

Additionally:
- Highlight any patterns or anomalies in the data.
- Suggest improvements or transformations that could enhance data quality.
- Identify potential risks or inconsistencies that could impact downstream processes.

Here is the table:

{df.to_markdown(index=True)}

Use '---' to separate each issue.
"""
    headers = {"api-key": azure_openai_api_key, "Content-Type": "application/json"}
    url = f"{azure_openai_endpoint.rstrip('/')}/openai/deployments/{azure_openai_deployment}/chat/completions?api-version=2024-12-01-preview"
    data = {
        "messages": [
            {"role": "system", "content": "You are an expert in the field of data quality analysis."},
            {"role": "user", "content": prompt}
        ],
        "max_tokens": max_tokens,
        "temperature": 0.7
    }

    try:
        response = requests.post(url, headers=headers, json=data)
        response_json = response.json()
        if "choices" in response_json and response_json["choices"]:
            llm_reply = response_json["choices"][0]["message"]["content"]
            return metrics, llm_reply
        else:
            st.error("❌ Unexpected LLM response structure.")
            st.code(json.dumps(response_json, indent=2))
            return metrics, ""
    except Exception as e:
        st.error(f"❌ API call failed: {e}")
        return metrics, ""
    
# Function to analyze multiple files (cross-file analysis)
def analyze_cross_files(file_paths):
    prompt = "You are a world-class data quality analyst. Analyze the relationships between the following datasets:\n\n"
    for path in file_paths:
        df = pd.read_csv(path)
        prompt += f"Dataset: {os.path.basename(path)}\n{df.to_markdown(index=True)}\n\n"

    prompt += """
For cross-file analysis, identify:
- Relationships between datasets (e.g., shared fields, dependencies, or mismatches).
- Domains and subdomains inferred from column names, sample values, and context.
- Cross-file data quality issues (e.g., mismatched references, duplicate entries across files, or missing links).

For each issue, provide:
- **Issue:** [The title or short description of the issue]
- **Details:** [A detailed explanation of the issue]
- **Expected correct state:** [What the correct state should be]
- **Violated constraint:** [Any violated constraints or standards]
- **Location:** [Where the issue is located]

Additionally:
- Highlight any patterns or anomalies across datasets.
- Suggest improvements or transformations that could enhance cross-file data quality.
- Identify potential risks or inconsistencies that could impact downstream processes.

Use '---' to separate each issue.
"""
    headers = {"api-key": azure_openai_api_key, "Content-Type": "application/json"}
    url = f"{azure_openai_endpoint.rstrip('/')}/openai/deployments/{azure_openai_deployment}/chat/completions?api-version=2024-12-01-preview"
    data = {
        "messages": [
            {"role": "system", "content": "You are an expert in the field of data quality analysis."},
            {"role": "user", "content": prompt}
        ],
        "max_tokens": max_tokens,
        "temperature": 0.7
    }

    try:
        response = requests.post(url, headers=headers, json=data)
        response_json = response.json()
        if "choices" in response_json and response_json["choices"]:
            llm_reply = response_json["choices"][0]["message"]["content"]
            return llm_reply
        else:
            st.error("❌ Unexpected LLM response structure.")
            st.code(json.dumps(response_json, indent=2))
            return ""
    except Exception as e:
        st.error(f"❌ API call failed: {e}")
        return ""
    
# Function to apply remediation for selected issues
def apply_remediation(issue, strategy, custom_fix=""):
    if strategy == "Auto-fix":
        prompt = f"""
You are a world-class data quality analyst and domain expert. Based on the following issue, provide a recommended fix.

Issue:
- **Title:** {issue['title']}
- **Details:** {issue['details']}
- **Expected correct state:** {issue['expected']}
- **Violated constraint:** {issue['constraint']}
- **Location:** {issue['location']}

Provide:
- **Recommended Fix:** [A detailed fix for the issue]
"""
        headers = {"api-key": azure_openai_api_key, "Content-Type": "application/json"}
        url = f"{azure_openai_endpoint.rstrip('/')}/openai/deployments/{azure_openai_deployment}/chat/completions?api-version=2024-12-01-preview"
        data = {
            "messages": [
                {"role": "system", "content": "You are an expert in the field of data quality analysis."},
                {"role": "user", "content": prompt}
            ],
            "max_tokens": max_tokens,
            "temperature": 0.7
        }

        try:
            response = requests.post(url, headers=headers, json=data)
            response_json = response.json()
            if "choices" in response_json and response_json["choices"]:
                llm_reply = response_json["choices"][0]["message"]["content"]
                return f"# Auto-fixed: {issue['title']}\n# Recommended Fix:\n{llm_reply}\n"
            else:
                return f"# Auto-fix failed for issue: {issue['title']}\n# Details: {issue['details']}\n"
        except Exception as e:
            return f"# Auto-fix error for issue: {issue['title']}\n# Error: {e}\n"

    elif strategy == "Add comment":
        return f"# TODO: Review issue: {issue['details']}\n"

    elif strategy == "Custom":
        return f"# Custom Fix: {custom_fix}\n"

    else:
        return "# No action taken.\n"
    
# Streamlit UI
st.set_page_config(page_title="🧹 Data Quality Copilot", layout="wide")
st.title("🧠 Data Quality Chatbot")

# Initialize session state
if "llm_output" not in st.session_state:
    st.session_state.llm_output = ""
if "issues" not in st.session_state:
    st.session_state.issues = []

# Mode selection
mode = st.radio("How would you like to provide data?", ["📤 Upload CSV files", "🛢️ Connect to Databricks"])

if mode == "📤 Upload CSV files":
    uploaded_files = st.file_uploader("Choose one or more CSV files", type="csv", accept_multiple_files=True)

    if uploaded_files:
        file_paths = []
        for file in uploaded_files:
            file_path = os.path.join(TEMP_DIR, file.name)  # Save files in the temporary directory
            df = pd.read_csv(file)
            df.to_csv(file_path, index=False)
            file_paths.append(file_path)

        # Per-file analysis button for each file
        for file_path in file_paths:
            if st.button(f"🔍 Analyze {os.path.basename(file_path)}"):
                metrics, llm_reply = analyze_single_file(file_path)
                st.markdown(f"### Analysis for file: {os.path.basename(file_path)}")
                st.text(llm_reply)
                st.download_button(
                    label=f"📥 Download Analysis for {os.path.basename(file_path)}",
                    data=llm_reply,
                    file_name=f"{os.path.basename(file_path)}_analysis.txt",
                    mime="text/plain"
                )

        # Cross-file analysis button
        if st.button("🔍 Analyze Cross Files"):
            llm_reply = analyze_cross_files(file_paths)
            st.markdown("### Cross-file Analysis Results")
            st.text(llm_reply)
            st.download_button(
                label="📥 Download Cross-file Analysis",
                data=llm_reply,
                file_name="cross_file_analysis.txt",
                mime="text/plain"
            )

elif mode == "🛢️ Connect to Databricks":
    st.subheader("Enter Databricks Connection Info")
    server_hostname = st.text_input("Server Hostname")
    http_path = st.text_input("HTTP Path")
    access_token = st.text_input("Access Token", type="password")

    if st.button("🔗 Connect & Analyze"):
        all_metrics = analyze_databricks_tables(server_hostname, http_path, access_token)
        st.session_state.issues = extract_issues_from_txt(os.path.join(TEMP_DIR, "analysis_output.txt"))
        st.session_state.llm_output = "✅ Databricks analysis complete. Issues extracted."

# Display Issues in Sidebar
if st.session_state.issues:
    st.sidebar.header("📋 Issues Found")
    idx = st.sidebar.selectbox("Select an issue to explore:",
                               range(len(st.session_state.issues)),
                               format_func=lambda i: f"{i+1}. {st.session_state.issues[i]['title']}")

    issue = st.session_state.issues[idx]

    st.subheader(f"🔍 {issue['title']}")
    st.markdown(f"**File:** `{issue.get('file', 'N/A')}`")
    st.markdown(f"**Details:** {issue.get('details', 'N/A')}`")
    st.markdown(f"**Expected State:** {issue.get('expected', 'N/A')}`")
    st.markdown(f"**Violated Constraint:** {issue.get('constraint', 'N/A')}`")
    st.markdown(f"**Location:** {issue.get('location', 'N/A')}`")
    st.markdown("---")

    strategy = st.radio("How should I fix this?", ["Auto-fix", "Add comment", "Custom"])
    custom_fix = ""
    if strategy == "Custom":
        custom_fix = st.text_area("✍️ Describe your custom fix approach:")

    if st.button("✅ Apply Fix"):
        remediation_result = apply_remediation(issue, strategy, custom_fix)
        st.success("Fix applied successfully!")
        st.code(remediation_result)

    st.markdown("🎯 Pick another issue from the sidebar to continue your review.")

# Fallback when no data is submitted
elif not st.session_state.llm_output and not st.session_state.issues:
    st.info("👋 Upload data or connect to Databricks above to begin your quality audit.")

