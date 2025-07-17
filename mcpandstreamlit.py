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
You are a world-class data quality analyst and domain expert. Your task is to analyze the provided table and identify all possible data quality issues, focusing on detailed and domain-specific errors and their source documents.

For each issue, provide:
- Issue: [The title or short description of the issue]
- Details: [A detailed explanation of the issue]
- Expected correct state: [What the correct state should be]
- Violated constraint: [Any violated constraints or standards]
- Location: [Where the issue is located]
- Guideline Violated: [Real world guideline or policy being violated, if applicable]

Include subtle, rare, or advanced domain-specific errors along with the particular real world policy or guideline which is being violated. If unsure, explain your reasoning and what you would check in the real world.

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
You are a world-class data quality analyst and domain expert. Your task is to analyze the provided datasets and identify all possible cross-file data quality issues, domains, subdomains, and fields.

For cross-file analysis, identify:
- Relationships between datasets (e.g., shared fields, dependencies, or mismatches).
- Domains and subdomains inferred from column names, sample values, and context.
- Cross-file data quality issues (e.g., mismatched references, duplicate entries across files, or missing links).

For each issue, provide:
- Issue: [The title or short description of the issue]
- Details: [A detailed explanation of the issue]
- Expected correct state: [What the correct state should be]
- Violated constraint: [Any violated constraints or standards]
- Location: [Where the issue is located]
- Guideline Violated: [Real world guideline or policy being violated, if applicable]
Include subtle, rare, or advanced domain-specific errors, even if they require deep expertise or simulated research.
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

# Function to analyze tables in Databricks
def analyze_databricks_tables(server_hostname, http_path, access_token, database="default"):
    try:
        connection = sql.connect(
            server_hostname=server_hostname,
            http_path=http_path,
            access_token=access_token,
            database=database
        )
        cursor = connection.cursor()

        # Fetch all tables in the default schema
        cursor.execute("SHOW TABLES IN default")
        tables = cursor.fetchall()
        table_names = [row[1] for row in tables]

        all_metrics = []
        for table_name in table_names:
            cursor.execute(f"SELECT * FROM {table_name}")
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            df = pd.DataFrame(rows, columns=columns)

            # Compute metrics for each table
            metrics = compute_dynamic_metrics(df, table_name)
            all_metrics.extend(metrics)

        return all_metrics
    except Exception as e:
        st.error(f"❌ Databricks connection failed: {e}")
        return []

# Function to extract issues from text file
def extract_issues_from_txt(file_path):
    try:
        with open(file_path, "r") as file:
            content = file.read()

        # Parse issues from the text content
        issues = []
        for section in content.split("---"):
            issue = {}
            lines = section.strip().split("\n")
            for line in lines:
                if line.startswith("- **Issue:**"):
                    issue["title"] = line.replace("- **Issue:**", "").strip()
                elif line.startswith("- **Details:**"):
                    issue["details"] = line.replace("- **Details:**", "").strip()
                elif line.startswith("- **Expected correct state:**"):
                    issue["expected"] = line.replace("- **Expected correct state:**", "").strip()
                elif line.startswith("- **Violated constraint:**"):
                    issue["constraint"] = line.replace("- **Violated constraint:**", "").strip()
                elif line.startswith("- **Location:**"):
                    issue["location"] = line.replace("- **Location:**", "").strip()
            if issue:
                issues.append(issue)

        return issues
    except Exception as e:
        st.error(f"❌ Failed to extract issues from text: {e}")
        return []

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
                # Apply the fix to the dataset (if applicable)
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
        # Analyze tables in Databricks
        all_metrics = analyze_databricks_tables(server_hostname, http_path, access_token, database="default")

        # Save metrics to a temporary file
        metrics_file_path = os.path.join(TEMP_DIR, "analysis_output.txt")
        with open(metrics_file_path, "w") as file:
            for metric in all_metrics:
                file.write(json.dumps(metric) + "\n")

        # Extract issues from the saved metrics file
        st.session_state.issues = extract_issues_from_txt(metrics_file_path)
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

# Toggle visibility of previously generated outputs
if st.checkbox("Show/Hide Previous Outputs"):
    if st.session_state.llm_output:
        st.markdown("### Previous Outputs")
        st.code(st.session_state.llm_output)

# Fallback when no data is submitted
elif not st.session_state.llm_output and not st.session_state.issues:
    st.info("👋 Upload data or connect to Databricks above to begin your quality audit.")
