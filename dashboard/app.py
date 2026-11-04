"""Streamlit operations dashboard; reads through FastAPI, never direct SQL."""
from __future__ import annotations
import os
import httpx
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

API = os.getenv("ROBOSTREAM_API_URL", "http://localhost:8000").rstrip("/")
st.set_page_config(page_title="RoboStream", page_icon="🤖", layout="wide")


def fetch(route: str, params: dict | None = None):
    try:
        result = httpx.get(API + route, params=params, timeout=15)
        result.raise_for_status()
        return result.json()
    except (httpx.RequestError, httpx.HTTPStatusError, ValueError) as error:
        st.warning(f"API currently unavailable for {route}: {error}")
        return None


def empty(message="No data yet. Run the demo to generate and process a sample dataset."):
    st.info(message)


st.title("RoboStream")
st.caption("Robotics telemetry · episode reconstruction · explainable dataset quality")
page = st.sidebar.radio("Explore", ["Overview", "Robots & sessions", "Episode inspector",
                                   "Data quality", "Pipeline runs"])
st.sidebar.caption(f"API: {API}")
if st.sidebar.button("Refresh data"):
    st.rerun()

if page == "Overview":
    data = fetch("/api/v1/overview")
    if data:
        cols = st.columns(4)
        for col, label, key in zip(cols, ["Robots", "Sessions", "Episodes", "Processed samples"],
                                   ["robots", "sessions", "episodes", "processed_records"]):
            col.metric(label, data[key])
        st.metric("Ingestion rejected records", data["rejected_ingestion_records"])
        st.metric("Excellent episodes", f"{data['high_quality_percent']}%")
        if data["latest_run"]:
            st.subheader("Latest pipeline run")
            st.json(data["latest_run"])
        else:
            empty()
        analytics = fetch("/api/v1/analytics/sensors")
        if analytics and analytics.get("items"):
            st.subheader("Sensor summary (DuckDB / Parquet)")
            st.dataframe(analytics["items"], use_container_width=True)

elif page == "Robots & sessions":
    robots = fetch("/api/v1/robots", {"limit": 500})
    if robots and robots["items"]:
        robot_ids = [r["robot_id"] for r in robots["items"]]
        chosen = st.selectbox("Robot", robot_ids)
        meta = fetch(f"/api/v1/robots/{chosen}")
        if meta:
            st.metric("Sessions", meta["session_count"])
        sessions = fetch("/api/v1/sessions", {"robot_id": chosen, "limit": 500})
        if sessions and sessions["items"]:
            st.dataframe(sessions["items"], use_container_width=True)
        eps = fetch("/api/v1/episodes", {"robot_id": chosen, "limit": 500})
        if eps and eps["items"]:
            st.subheader("Associated episodes")
            st.dataframe(eps["items"], use_container_width=True)
    else:
        empty()

elif page == "Episode inspector":
    eps = fetch("/api/v1/episodes", {"limit": 500})
    if eps and eps["items"]:
        labels = {e["episode_id"]: f"{e['robot_id']} | {e['session_id']} | {e['task_id']} | {e['quality_status']}"
                  for e in eps["items"]}
        selected = st.selectbox("Episode", list(labels), format_func=lambda k: labels[k])
        detail = fetch(f"/api/v1/episodes/{selected}")
        limit = st.slider("Maximum plotted records", min_value=100, max_value=5000,
                          value=500, step=100)
        telemetry = fetch(f"/api/v1/episodes/{selected}/telemetry", {"limit": limit})
        if detail:
            cols = st.columns(4)
            cols[0].metric("Quality score", detail["quality_score"])
            cols[1].metric("Quality label", detail["quality_status"])
            cols[2].metric("Completion", detail["completion_status"])
            cols[3].metric("Records", detail["record_count"])
            with st.expander("Quality metrics and flags", expanded=True):
                st.json(detail.get("quality_metrics", {}))
        if telemetry and telemetry["items"]:
            df = pd.DataFrame(telemetry["items"])
            df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
            obs = df[df["event_type"] == "observation"].copy()
            if not obs.empty:
                for field, title in [("joint_positions", "Joint positions"),
                                     ("joint_velocities", "Joint velocities")]:
                    fig = go.Figure()
                    max_joints = max((len(v) for v in obs[field] if isinstance(v, list)), default=0)
                    for j in range(min(12, max_joints)):
                        fig.add_trace(go.Scatter(x=obs["timestamp"],
                                                 y=[v[j] if isinstance(v, list) and len(v) > j
                                                    else None for v in obs[field]],
                                                 name=f"Joint {j + 1}", mode="lines"))
                    fig.update_layout(title=title, xaxis_title="UTC timestamp", height=320)
                    st.plotly_chart(fig, use_container_width=True)
                if "gripper_position" in obs:
                    st.plotly_chart(px.line(obs, x="timestamp", y="gripper_position",
                                            title="Gripper position"), use_container_width=True)
                positions = [(v[0], v[1], v[2]) for v in obs["end_effector_position"]
                             if isinstance(v, list) and len(v) == 3]
                if positions:
                    x, y, z = zip(*positions)
                    st.plotly_chart(go.Figure([go.Scatter3d(x=x, y=y, z=z, mode="lines+markers",
                                                              marker={"size": 2})]).update_layout(
                        title="End effector trajectory", height=380), use_container_width=True)
                diffs = obs["timestamp"].diff().dropna().dt.total_seconds()
                if not diffs.empty:
                    st.plotly_chart(px.histogram(x=diffs, nbins=40,
                                                 title="Sampling interval distribution (seconds)"),
                                    use_container_width=True)
                st.dataframe(obs[["timestamp", "event_id", "gripper_state", "task_id"]],
                             use_container_width=True)
            else:
                empty("No observation samples in the displayed range")
        elif telemetry:
            empty("Episode has no readable telemetry")
    else:
        empty()

elif page == "Data quality":
    issues = fetch("/api/v1/issues", {"limit": 500})
    eps = fetch("/api/v1/episodes", {"limit": 500})
    if issues and issues["items"]:
        frame = pd.DataFrame(issues["items"])
        st.metric("Total issue records", issues["total"])
        st.plotly_chart(px.bar(frame["issue_code"].value_counts().rename_axis("code").reset_index(
            name="count"), x="code", y="count", title="Issue code frequency (up to 500 recent)"),
                        use_container_width=True)
        st.plotly_chart(px.pie(frame, names="severity", title="Issue severity"), use_container_width=True)
        severity = st.selectbox("Severity filter", ["all", "info", "warning", "error"])
        if severity != "all":
            frame = frame[frame["severity"] == severity]
        st.dataframe(frame, use_container_width=True)
    else:
        empty("No quality issues recorded")
    if eps and eps["items"]:
        st.plotly_chart(px.histogram(pd.DataFrame(eps["items"]), x="quality_score", nbins=20,
                                     title="Quality score distribution"), use_container_width=True)
        missing = []
        for e in eps["items"][:20]:
            quality = fetch(f"/api/v1/episodes/{e['episode_id']}/quality")
            if quality:
                for name, rate in quality.get("missing_value_rates", {}).items():
                    missing.append({"sensor": name, "missing_rate": rate})
        if missing:
            st.plotly_chart(px.bar(pd.DataFrame(missing).groupby("sensor", as_index=False).mean(),
                                   x="sensor", y="missing_rate", title="Average missingness (sampled episodes)"),
                            use_container_width=True)

elif page == "Pipeline runs":
    runs = fetch("/api/v1/pipeline/runs", {"limit": 100})
    if runs and runs["items"]:
        st.dataframe(runs["items"], use_container_width=True)
        selection = st.selectbox("Inspect run", [r["run_id"] for r in runs["items"]])
        report = fetch(f"/api/v1/pipeline/runs/{selection}/report")
        if report:
            st.json(report)
    else:
        empty()
