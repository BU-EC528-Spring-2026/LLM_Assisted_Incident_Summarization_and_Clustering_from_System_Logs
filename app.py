import json
import pandas as pd
import streamlit as st
from pathlib import Path

st.set_page_config(page_title="Incident Dashboard", layout="wide")

DATA_DIR = Path("data")

@st.cache_data
def load_incidents(filename: str):
    with open(DATA_DIR / filename, "r") as f:
        incidents = json.load(f)
    return incidents

st.title("System Log Incident Dashboard")

available_files = sorted([p.name for p in DATA_DIR.glob("incidents_*.json")])
if not available_files:
    st.error("No incident JSON files found in data/")
    st.stop()

selected_file = st.sidebar.selectbox("Incident file", available_files)
incidents = load_incidents(selected_file)

df = pd.DataFrame([
    {
        "incident_id": item["incident_id"],
        "block_id": item["block_id"],
        "start_time": item["start_time"],
        "end_time": item["end_time"],
        "duration_seconds": item["duration_seconds"],
        "num_logs": item["num_logs"],
        "severity": item["severity"],
        "components": ", ".join(item["components"]),
    }
    for item in incidents
])

severity_filter = st.sidebar.multiselect(
    "Severity",
    options=sorted(df["severity"].dropna().unique().tolist()),
    default=sorted(df["severity"].dropna().unique().tolist())
)

min_logs = int(df["num_logs"].min())
max_logs = int(df["num_logs"].max())
log_range = st.sidebar.slider("Number of logs", min_logs, max_logs, (min_logs, max_logs))

filtered = df[
    df["severity"].isin(severity_filter) &
    df["num_logs"].between(log_range[0], log_range[1])
]

col1, col2, col3 = st.columns(3)
col1.metric("Incidents", len(filtered))
col2.metric("Avg logs/incident", f"{filtered['num_logs'].mean():.1f}" if len(filtered) else "0")
col3.metric("Max duration (s)", f"{filtered['duration_seconds'].max():.1f}" if len(filtered) else "0")

st.subheader("Severity distribution")
st.bar_chart(filtered["severity"].value_counts())

st.subheader("Incidents")
st.dataframe(filtered, use_container_width=True)

st.subheader("Incident details")
incident_ids = filtered["incident_id"].tolist()
if incident_ids:
    selected_incident_id = st.selectbox("Choose incident", incident_ids)
    selected_incident = next(x for x in incidents if x["incident_id"] == selected_incident_id)

    st.json({
        "incident_id": selected_incident["incident_id"],
        "block_id": selected_incident["block_id"],
        "severity": selected_incident["severity"],
        "components": selected_incident["components"],
        "num_logs": selected_incident["num_logs"],
        "start_time": selected_incident["start_time"],
        "end_time": selected_incident["end_time"],
    })

    st.write("Logs")
    st.dataframe(pd.DataFrame(selected_incident["logs"]), use_container_width=True)
else:
    st.info("No incidents match the current filters.")