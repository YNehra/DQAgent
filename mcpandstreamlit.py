# ----------------------------------------------------------------------------------------------------------------- Part 3
import sqlite3 as sql
import pandas as pd
import re
import os
import json
import requests
from datetime import datetime, timezone
import streamlit as st
import pandas as pd
import os
import json
import requests
import re
from datetime import datetime, timezone

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

def extract_issues_from_txt(txt_file):
    issues = []
    try:
        with open(txt_file, "r", encoding="utf-8") as f:
            content = f.read()
            sections = re.split(r"(?=Analysis for file:|Analysis for table:)", content)
            for section in sections:
                if "Analysis for file:" in section or "Analysis for table:" in section:
                    filename_match = re.search(r"Analysis for (file|table): (.*)", section)
                    filename = filename_match.group(2).strip() if filename_match else "Unknown"
                    issue_blocks = section.split("---")
                    for block in issue_blocks:
                        block = block.strip()
                        if block:
                            issue = {"file": filename}
                            for line in block.splitlines():
                                if line.startswith("- **Issue:**"):
                                    issue["title"] = line.split("**Issue:**")[1].strip()
                                elif line.startswith("- Details:"):
                                    issue["details"] = line.split("Details:")[1].strip()
                                elif line.startswith("- Expected correct state:"):
                                    issue["expected"] = line.split("Expected correct state:")[1].strip()
                                elif line.startswith("- Violated constraint:"):
                                    issue["constraint"] = line.split("Violated constraint:")[1].strip()
                                elif line.startswith("- Location:"):
                                    issue["location"] = line.split("Location:")[1].strip()
                            if "title" in issue:
                                issues.append(issue)
    except Exception as e:
        st.error(f"Issue extraction error: {e}")

    return issues


# ----------------------------------------------------------------------------------------------------------------- Part 4

def analyze_csv_files(file_paths):
    all_metrics = []
    headers = {"api-key": azure_openai_api_key, "Content-Type": "application/json"}
    url = f"{azure_openai_endpoint.rstrip('/')}/openai/deployments/{azure_openai_deployment}/chat/completions?api-version=2025-01-01-preview"
    data = {
        "messages": [
            {"role": "system", "content": "You are an expert in the field of data quality analysis."},
            {"role": "user", "content": ""}
        ],
        "max_tokens": max_tokens,
        "temperature": 0.7
    }

    # --- Cross-file analysis prompt ---
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

Use '---' to separate each issue.
"""
    data["messages"][1]["content"] = prompt

    try:
        response = requests.post(url, headers=headers, json=data)
        response_json = response.json()
        if "choices" in response_json and response_json["choices"]:
            llm_reply = response_json["choices"][0]["message"]["content"]
        else:
            st.error("❌ LLM cross-file response structure unexpected.")
            st.code(json.dumps(response_json, indent=2))
            return []
    except Exception as e:
        st.error(f"❌ API call failed: {e}")
        return []

    with open(output_txt, "w", encoding="utf-8") as f:
        f.write("Cross-file Analysis:\n")
        f.write(llm_reply)
        f.write("\n" + "=" * 80 + "\n")

    for path in file_paths:
        df = pd.read_csv(path)
        all_metrics.extend(compute_dynamic_metrics(df, os.path.basename(path)))

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

Here is the table (showing up to {len(df)} rows):

{df.to_markdown(index=True)}

Use '---' to separate each issue.
"""
        data["messages"][1]["content"] = prompt

        try:
            response = requests.post(url, headers=headers, json=data)
            response_json = response.json()
            if "choices" in response_json and response_json["choices"]:
                llm_reply = response_json["choices"][0]["message"]["content"]
            else:
                st.warning(f"⚠️ Unexpected LLM output for file: {os.path.basename(path)}")
                st.code(json.dumps(response_json, indent=2))
                continue
        except Exception as e:
            st.warning(f"⚠️ API error while analyzing file {path}: {e}")
            continue

        with open(output_txt, "a", encoding="utf-8") as f:
            f.write(f"Analysis for file: {path}\n")
            f.write(llm_reply)
            f.write("\n" + "=" * 80 + "\n")

    return all_metrics


def analyze_databricks_tables(server_hostname, http_path, access_token):
    all_metrics = []
    connection, table_names = get_database_tables(server_hostname, http_path, access_token)
    if not connection:
        return []

    cursor = connection.cursor()
    headers = {"api-key": azure_openai_api_key, "Content-Type": "application/json"}
    url = f"{azure_openai_endpoint}/openai/deployments/{azure_openai_deployment}/chat/completions?api-version=2025-04-01-preview"
    data = {
        "messages": [
            {"role": "system", "content": "You are an expert in the field of data quality analysis."},
            {"role": "user", "content": ""}
        ],
        "max_tokens": max_tokens,
        "temperature": 0.7
    }

    prompt = "You are a world-class data quality analyst. Analyze the relationships between the following datasets:\n\n"
    for table in table_names:
        cursor.execute(f"SELECT * FROM {table} LIMIT 100")
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(rows, columns=columns)
        prompt += f"Dataset: {table}\n{df.to_markdown(index=True)}\n\n"

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

Use '---' to separate each issue.
"""
    data["messages"][1]["content"] = prompt

    try:
        response = requests.post(url, headers=headers, json=data)
        response_json = response.json()
        if "choices" in response_json and response_json["choices"]:
            llm_reply = response_json["choices"][0]["message"]["content"]
        else:
            st.error("❌ LLM response structure unexpected during cross-table analysis.")
            st.code(json.dumps(response_json, indent=2))
            return []
    except Exception as e:
        st.error(f"❌ Failed during cross-table analysis: {e}")
        return []

    with open(output_txt, "w", encoding="utf-8") as f:
        f.write("Cross-table Analysis:\n")
        f.write(llm_reply)
        f.write("\n" + "=" * 80 + "\n")

    for table in table_names:
        cursor.execute(f"SELECT * FROM {table} LIMIT 100")
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(rows, columns=columns)
        all_metrics.extend(compute_dynamic_metrics(df, table))

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

Here is the table (up to {len(df)} rows):

{df.to_markdown(index=True)}

Use '---' to separate each issue.
"""
        data["messages"][1]["content"] = prompt

        try:
            response = requests.post(url, headers=headers, json=data)
            response_json = response.json()
            if "choices" in response_json and response_json["choices"]:
                llm_reply = response_json["choices"][0]["message"]["content"]
            else:
                st.warning(f"⚠️ Unexpected LLM output for table: {table}")
                st.code(json.dumps(response_json, indent=2))
                continue
        except Exception as e:
            st.warning(f"⚠️ Error analyzing table {table}: {e}")
            continue

        with open(output_txt, "a", encoding="utf-8") as f:
            f.write(f"Analysis for table: {table}\n")
            f.write(llm_reply)
            f.write("\n" + "=" * 80 + "\n")

    return all_metrics

# -----------------------------------------------------------------------------------------------------------------

# 🔐 Set your Azure OpenAI API credentials and output file path
azure_openai_api_key = "A7bPfHr4l6gk6YO9BZd2wD8RNxL_P1wgVsF857P44zl23NrVzPJX0JQQJ99BFACYeBjFXJ3w3AAABACOGJNa6"
azure_openai_endpoint = "https://definitivpoc.openai.azure.com"
azure_openai_deployment = "gpt-4.1-mini"
output_txt = "analysis_output.txt"
max_tokens = 2000  # You can customize as needed

st.set_page_config(page_title="🧹 Data Quality Copilot", layout="wide")
st.title("🧠 Data Quality Chatbot")

# Initialize session state
if "llm_output" not in st.session_state:
    st.session_state.llm_output = ""
if "issues" not in st.session_state:
    st.session_state.issues = []

# -- Mode selection
mode = st.radio("How would you like to provide data?", ["📤 Upload CSV files", "🛢️ Connect to Databricks"])

if mode == "📤 Upload CSV files":
    uploaded_files = st.file_uploader("Choose one or more CSV files", type="csv", accept_multiple_files=True)

    if uploaded_files:
        file_paths = []
        for file in uploaded_files:
            df = pd.read_csv(file)
            st.write(f"📄 **{file.name}** preview:")
            st.dataframe(df.head())
            file_path = f"/tmp/{file.name}"
            df.to_csv(file_path, index=False)
            file_paths.append(file_path)

        if st.button("🔍 Analyze Uploaded Files"):
            all_metrics = analyze_csv_files(file_paths)
            st.session_state.issues = extract_issues_from_txt(output_txt)
            st.session_state.llm_output = "✅ Analysis complete. Issues extracted."

elif mode == "🛢️ Connect to Databricks":
    st.subheader("Enter Databricks Connection Info")
    server_hostname = st.text_input("Server Hostname")
    http_path = st.text_input("HTTP Path")
    access_token = st.text_input("Access Token", type="password")

    if st.button("🔗 Connect & Analyze"):
        # Use these user inputs later in your connection logic
        all_metrics = analyze_databricks_tables(server_hostname, http_path, access_token)
        st.session_state.issues = extract_issues_from_txt(output_txt)
        st.session_state.llm_output = "✅ Databricks analysis complete. Issues extracted."

# --------------------------------------------------------------------------------------- Part 2
# --- Display LLM Output (if present) ---
if st.session_state.llm_output:
    st.markdown("### 🧾 LLM Output")
    st.info(st.session_state.llm_output)

# --- If issues exist, show sidebar navigator and details view ---
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
        st.success("Fix simulated! (Non-destructive)")
        if strategy == "Auto-fix":
            st.code(f"# Auto-fixed: {issue['title']}")
        elif strategy == "Add comment":
            st.code(f"# TODO: Review issue: {issue['details']}")
        elif strategy == "Custom":
            st.code(f"# Custom Fix: {custom_fix}")

    st.markdown("🎯 Pick another issue from the sidebar to continue your review.")

# --- Fallback when nothing has been submitted yet ---
elif not st.session_state.llm_output and not st.session_state.issues:
    st.info("👋 Upload data or connect to Databricks above to begin your quality audit.")
# --------------------------------------------------------------------------------------- Part 3
