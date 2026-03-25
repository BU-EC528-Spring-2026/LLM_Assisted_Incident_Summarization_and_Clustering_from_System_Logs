import json
import pandas as pd
import numpy as np
import streamlit as st
from pathlib import Path
from sklearn.cluster import KMeans
from sentence_transformers import SentenceTransformer

st.set_page_config(page_title="Incident Clusters Dashboard", layout="wide")

DATA_DIR = Path("data")

@st.cache_data
def load_incidents(filename: str):
    with open(DATA_DIR / filename, "r") as f:
        incidents = json.load(f)
    return incidents

@st.cache_data
def load_or_generate_clusters(incidents, n_clusters=5):
    """Load clusters from file or generate synthetic clusters"""
    cluster_file = DATA_DIR / "clusters.json"
    
    if cluster_file.exists():
        with open(cluster_file, "r") as f:
            return json.load(f)
    
    st.info("Generating synthetic clusters from incident logs...")
    
    texts = []
    for inc in incidents:
        logs = inc.get('logs', [])
        log_messages = [log.get('message', '') for log in logs]
        text = ' | '.join(log_messages) if log_messages else f"{inc.get('severity', 'UNKNOWN')} error"
        texts.append(text)
    
    model = SentenceTransformer('all-MiniLM-L6-v2')
    embeddings = model.encode(texts)
    
    kmeans = KMeans(n_clusters=n_clusters, random_state=42)
    cluster_labels = kmeans.fit_predict(embeddings)
    
    clusters = {}
    for idx, label in enumerate(cluster_labels):
        if label not in clusters:
            clusters[label] = []
        clusters[label].append(incidents[idx]["incident_id"])
    
    return clusters

st.title("System Log Incident Clusters Dashboard")

available_files = sorted([p.name for p in DATA_DIR.glob("incidents_*.json")])
if not available_files:
    st.error("No incident JSON files found in data/")
    st.stop()

selected_file = st.sidebar.selectbox("Incident file", available_files)
incidents = load_incidents(selected_file)

n_clusters = st.sidebar.slider("Number of clusters", 2, 20, 5)
clusters = load_or_generate_clusters(incidents, n_clusters)

cluster_data = []
for cluster_id, incident_ids in clusters.items():
    cluster_incidents = [inc for inc in incidents if inc["incident_id"] in incident_ids]
    
    cluster_data.append({
        "cluster_id": cluster_id,
        "num_incidents": len(incident_ids),
        "avg_logs": np.mean([inc["num_logs"] for inc in cluster_incidents]),
        "common_severity": pd.Series([inc["severity"] for inc in cluster_incidents]).mode()[0],
        "components": len(set(comp for inc in cluster_incidents for comp in inc["components"])),
    })

cluster_df = pd.DataFrame(cluster_data).sort_values("num_incidents", ascending=False)

col1, col2, col3 = st.columns(3)
col1.metric("Total Clusters", len(clusters))
col2.metric("Total Incidents", len(incidents))
col3.metric("Avg incidents/cluster", f"{len(incidents) / len(clusters):.1f}")

st.subheader("Cluster Summary")
st.dataframe(cluster_df, use_container_width=True)

st.subheader("Cluster Size Distribution")
st.bar_chart(cluster_df.set_index("cluster_id")["num_incidents"])

st.subheader("Cluster Details")
selected_cluster_id = st.selectbox(
    "Select cluster",
    options=cluster_df["cluster_id"].tolist(),
    format_func=lambda x: f"Cluster {x} ({cluster_df[cluster_df['cluster_id']==x]['num_incidents'].values[0]} incidents)"
)

selected_incident_ids = clusters[selected_cluster_id]
selected_incidents = [inc for inc in incidents if inc["incident_id"] in selected_incident_ids]

# Show incidents in cluster
incident_summary = pd.DataFrame([
    {
        "incident_id": inc["incident_id"],
        "severity": inc["severity"],
        "num_logs": inc["num_logs"],
        "duration_seconds": inc["duration_seconds"],
        "components": ", ".join(inc["components"]),
    }
    for inc in selected_incidents
])

st.dataframe(incident_summary, use_container_width=True)

st.subheader("Incident Details")
if selected_incident_ids:
    selected_incident_id = st.selectbox("Choose incident from cluster", selected_incident_ids)
    selected_incident = next(x for x in selected_incidents if x["incident_id"] == selected_incident_id)
    
    st.json({
        "incident_id": selected_incident["incident_id"],
        "severity": selected_incident["severity"],
        "components": selected_incident["components"],
        "num_logs": selected_incident["num_logs"],
        "start_time": selected_incident["start_time"],
        "end_time": selected_incident["end_time"],
    })
    
    st.write("Logs")
    st.dataframe(pd.DataFrame(selected_incident["logs"]), use_container_width=True)