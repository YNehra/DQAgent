# ----------------------------------------------------------------------------------------------------------------- Part 3
import sqlite3 as sql
import pandas as pd
import re
import os
import json
import requests
from datetime import datetime, timezone
import streamlit as st

def get_database_tables(server_hostname, http_path, access_token):
    try:
        connection = sql.connect(
            server_hostname=server_hostname,
            http_path=http_path,
            access_token=access_token
        )
        cursor = connection.cursor()
        cursor.execute("SHOW TABLES IN default")
        tables = cursor.fetchall()
        table_names = [row[1] for row in tables]
        return connection, table_names
    except Exception as e:
        st.error(f"❌ Databricks connection failed: {e}")
        return None, []

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

def analyze_csv_files(file_paths):
    all_metrics = []
    headers = {"api-key": azure_openai_api_key, "Content-Type": "application/json"}
    url = f"{azure_openai_endpoint.rstrip('/')}/openai/deployments/{azure_openai_deployment}/chat/completions?api-version=2024-12-01-preview"
    data = {
        "messages": [
            {"role": "system", "content": "You are an expert in the field of data quality analysis."},
            {"role": "user", "content": ""}
        ],
        "max_tokens": max_tokens,
        "temperature": 0.7
    }

    # --- Cross-file analysis ---
    st.markdown("### Cross-file Analysis")
    cross_file_output = ""
    for path in file_paths:
        df = pd.read_csv(path)
        st.write(f"📄 **{os.path.basename(path)}** preview:")
        st.dataframe(df.head())  # Display preview of the file

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
- Issue: [The title or short description of the issue]
- Details: [A detailed explanation of the issue]
- Expected correct state: [What the correct state should be]
- Violated constraint: [Any violated constraints or standards]
- Location: [Where the issue is located]
- Guideline Violated: [Real world guideline or policy being violated, if applicable]
Include subtle, rare, or advanced domain-specific errors, even if they require deep expertise or simulated research.

Use '---' to separate each issue.
"""
    data["messages"][1]["content"] = prompt

    try:
        response = requests.post(url, headers=headers, json=data)
        response_json = response.json()
        if "choices" in response_json and response_json["choices"]:
            llm_reply = response_json["choices"][0]["message"]["content"]
            if not llm_reply.strip():  # Check for empty output
                st.warning("⚠️ Cross-file analysis returned an empty output.")
            else:
                cross_file_output = llm_reply
                st.markdown("#### Cross-file Analysis Results")
                st.text(llm_reply)  # Display the output directly
        else:
            st.error("❌ LLM cross-file response structure unexpected.")
            st.code(json.dumps(response_json, indent=2))
    except Exception as e:
        st.error(f"❌ API call failed: {e}")

    # --- Per-file analysis ---
    st.markdown("### Per-file Analysis")
    per_file_output = ""
    for path in file_paths:
        df = pd.read_csv(path)
        table_name = os.path.basename(path)
        all_metrics.extend(compute_dynamic_metrics(df, table_name))

        prompt = f"""
You are a world-class data quality analyst and domain expert. Your task is to analyze the provided table and identify all possible data quality issues.

For each issue, provide:
- Issue: [The title or short description of the issue]
- Details: [A detailed explanation of the issue]
- Expected correct state: [What the correct state should be]
- Violated constraint: [Any violated constraints or standards]
- Location: [Where the issue is located]
- Guideline Violated: [Real world guideline or policy being violated, if applicable]

Additionally:
- Highlight any patterns or anomalies in the data.
- Suggest improvements or transformations that could enhance data quality.
- Identify potential risks or inconsistencies that could impact downstream processes.

Here is the table:

{df.to_markdown(index=True)}

Use '---' to separate each issue.
"""
        data["messages"][1]["content"] = prompt

        try:
            response = requests.post(url, headers=headers, json=data)
            response_json = response.json()
            if "choices" in response_json and response_json["choices"]:
                llm_reply = response_json["choices"][0]["message"]["content"]
                if not llm_reply.strip():  # Check for empty output
                    st.warning(f"⚠️ Analysis for file {table_name} returned an empty output.")
                else:
                    per_file_output += f"Analysis for file: {table_name}\n{llm_reply}\n\n"
                    st.markdown(f"#### Analysis for file: {table_name}")
                    st.text(llm_reply)  # Display the output directly
            else:
                st.warning(f"⚠️ Unexpected LLM output for file: {table_name}")
                st.code(json.dumps(response_json, indent=2))
        except Exception as e:
            st.warning(f"⚠️ API error while analyzing file {table_name}: {e}")

    # Provide download buttons for the outputs
    st.download_button(
        label="📥 Download Cross-file Analysis",
        data=cross_file_output,
        file_name="cross_file_analysis.txt",
        mime="text/plain"
    )
    st.download_button(
        label="📥 Download Per-file Analysis",
        data=per_file_output,
        file_name="per_file_analysis.txt",
        mime="text/plain"
    )

    return all_metrics

# -----------------------------------------------------------------------------------------------------------------
# Load secrets from Streamlit deployment
azure_openai_api_key = st.secrets["azure_openai_api_key"]
azure_openai_endpoint = st.secrets["azure_openai_endpoint"]
azure_openai_deployment = st.secrets["azure_openai_deployment"]
max_tokens = st.secrets.get("max_tokens", 2000)  # Optional, with fallback

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
            file_path = f"/tmp/{file.name}"
            df = pd.read_csv(file)
            df.to_csv(file_path, index=False)
            file_paths.append(file_path)

        if st.button("🔍 Analyze Uploaded Files"):
            analyze_csv_files(file_paths)

elif mode == "🛢️ Connect to Databricks":
    st.subheader("Enter Databricks Connection Info")
    server_hostname = st.text_input("Server Hostname")
    http_path = st.text_input("HTTP Path")
    access_token = st.text_input("Access Token", type="password")

    if st.button("🔗 Connect & Analyze"):
        st.warning("Databricks analysis is not implemented in this example.")
