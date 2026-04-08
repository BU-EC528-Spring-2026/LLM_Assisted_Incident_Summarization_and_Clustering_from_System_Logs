import numpy as np
import pandas as pd
import plotly.express as px
import streamlit as st
from pathlib import Path

st.set_page_config(page_title="HDFS Cluster Dashboard", layout="wide")

def _reduce_to_2d(vectors: np.ndarray) -> np.ndarray:
    """Reduces high-dimensional embeddings to 2D using SVD/PCA."""
    if vectors.shape[1] == 1:
        return np.column_stack([vectors[:, 0], np.zeros(vectors.shape[0])])
    
    # Center the vectors
    centered = vectors - vectors.mean(axis=0, keepdims=True)
    _, _, vh = np.linalg.svd(centered, full_matrices=False)
    
    # Project onto first two principal components
    return centered @ vh[:2].T

def main():
    st.title("HDFS Cluster Dashboard")
    st.markdown("Visualize block clusters, embeddings, and LLM summaries.")

    # 1. SIDEBAR CONFIG
    st.sidebar.header("Data Loading")
    data_dir = st.sidebar.text_input("Pipeline Output Directory", value="./pipeline_output")
    data_path = Path(data_dir)
    
    clusters_file = data_path / "clusters.csv"
    embeddings_file = data_path / "embeddings.csv"
    summaries_file = data_path / "summaries.csv"
    cluster_summaries_file = data_path / "cluster_summaries.csv" # Add this line
    
    if not clusters_file.exists():
        st.warning(f"Could not find `{clusters_file}`. Please run `cluster_pipeline.py` first, or check the folder path in the sidebar.")
        return
        
    st.sidebar.header("Performance / Sampling")
    max_per_cluster = st.sidebar.number_input(
        "Max samples per cluster", 
        min_value=5, max_value=5000, value=100, step=10,
        help="Limit the number of blocks displayed per cluster so the UI doesn't freeze."
    )

    # 2. LOAD DATA
    df_clusters = pd.read_csv(clusters_file)
    
    has_embeddings = embeddings_file.exists()
    df_embeddings = pd.read_csv(embeddings_file) if has_embeddings else pd.DataFrame()
        
    has_summaries = summaries_file.exists()
    df_summaries = pd.read_csv(summaries_file) if has_summaries else pd.DataFrame()
    
    # Add this block to load the cluster summaries
    has_cluster_summaries = cluster_summaries_file.exists()
    df_cluster_summaries = pd.read_csv(cluster_summaries_file) if has_cluster_summaries else pd.DataFrame()

    # 3. CALCULATE TRUE KPIs (Before Subsampling)
    n_total_blocks = len(df_clusters)
    cluster_ids = df_clusters["cluster_id"].unique()
    n_clusters = len([c for c in cluster_ids if c != -1])
    n_noise = len(df_clusters[df_clusters["cluster_id"] == -1])

    # 4. MERGE DATA
    df = df_clusters.copy()
    
    if has_summaries:
        df = df.merge(df_summaries, on="block_id", how="left")
        df["summary"] = df["summary"].fillna("No summary generated")
    else:
        df["summary"] = "No summary data available"

    # 5. SUBSAMPLE PER CLUSTER
    df = (
        df.groupby("cluster_id", group_keys=False)
        .apply(lambda x: x.sample(n=min(len(x), max_per_cluster), random_state=42))
        .reset_index(drop=True)
    )
    
    n_sampled_blocks = len(df)

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Blocks Evaluated", n_total_blocks)
    col2.metric("Distinct Clusters", n_clusters)
    col3.metric("Total Noise Blocks", f"{n_noise} ({(n_noise/n_total_blocks)*100:.1f}%)" if n_total_blocks else 0)
    col4.metric("Subsampled Points Shown", n_sampled_blocks)

    # 6. RENDER UI TABS
    tab1, tab2, tab3 = st.tabs(["2D Visualization", "Cluster Explorer", "Raw Data"])
    
    with tab1:
        st.subheader(f"Embedding Projection (Sampled: {n_sampled_blocks} points)")
        if has_embeddings:
            dim_cols = [c for c in df_embeddings.columns if str(c).startswith("dim_")]
            
            if dim_cols:
                df_viz = df.merge(df_embeddings[["block_id"] + dim_cols], on="block_id", how="inner")
                vectors = df_viz[dim_cols].to_numpy()
                
                coords_2d = _reduce_to_2d(vectors)
                df_viz["x"] = coords_2d[:, 0]
                df_viz["y"] = coords_2d[:, 1]
                
                df_viz["Cluster Name"] = df_viz["cluster_id"].apply(lambda x: "Noise (-1)" if x == -1 else f"Cluster {x}")
                
                fig = px.scatter(
                    df_viz,
                    x="x", y="y",
                    color="Cluster Name",
                    custom_data=["block_id", "summary", "cluster_id"], # Crucial for click events
                    hover_data={
                        "block_id": True, 
                        "Cluster Name": False, 
                        "cluster_id": False, 
                        "summary": False, # Hide on pure hover to keep it clean; relies on click
                        "x": False, "y": False
                    },
                    height=600
                )
                
                fig.update_traces(marker=dict(size=8, opacity=0.8))
                
                # Render chart and capture selection events
                event = st.plotly_chart(fig, use_container_width=True, on_select="rerun")
                
                if event and event.get("selection") and event["selection"].get("points"):
                    st.divider()
                    st.subheader("Selected Block(s)")
                    
                    selected_points = event["selection"]["points"]
                    
                    # Limit rendering if user lasso-selects thousands of points
                    if len(selected_points) > 50:
                        st.warning(f"You selected {len(selected_points)} points. Showing the first 50 below.")
                        selected_points = selected_points[:50]
                        
                    for pt in selected_points:
                        b_id = pt["customdata"][0]
                        b_sum = pt["customdata"][1]
                        c_id = pt["customdata"][2]
                        
                        # Show as an open expander for easy reading
                        c_desc = "Noise" if c_id == -1 else f"Cluster {c_id}"
                        with st.expander(f"**Block ID**: {b_id} | {c_desc}", expanded=True):
                            st.write(b_sum)

            else:
                st.info("No 'dim_X' columns found in embeddings.csv.")
        else:
            st.info("No embeddings.csv found for visualization.")

    with tab2:
        st.subheader("Explore Summaries by Cluster")
        
        sorted_clusters = sorted(cluster_ids.tolist())
        selected_cid = st.selectbox(
            "Select a Cluster to inspect:", 
            options=sorted_clusters,
            format_func=lambda x: f"Noise (-1)" if x == -1 else f"Cluster {x}"
        )
        
        if has_cluster_summaries and not df_cluster_summaries.empty:
            # Match cluster_id as strings in case of type mismatches between CSVs
            c_summary_row = df_cluster_summaries[df_cluster_summaries['cluster_id'].astype(str) == str(selected_cid)]
            if not c_summary_row.empty:
                overall_summary = c_summary_row.iloc[0]['cluster_summary']
                st.info(f"**Overall Cluster Analysis:**\n\n{overall_summary}")
        
        cluster_df = df[df["cluster_id"] == selected_cid]
        true_cluster_count = len(df_clusters[df_clusters["cluster_id"] == selected_cid])
        
        st.write(f"**Showing {len(cluster_df)} sampled blocks (out of {true_cluster_count} total blocks mapped to this cluster):**")
        
        for _, row in cluster_df.iterrows():
            with st.expander(f"Block ID: {row['block_id']}"):
                st.write(row["summary"])

    with tab3:
        st.subheader(f"Merged Dataset (Sampled: {n_sampled_blocks} rows)")
        st.dataframe(df, use_container_width=True, hide_index=True)

if __name__ == "__main__":
    main()