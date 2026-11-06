"""
Streaming Data Dashboard
Producer (WeatherAPI) -> Kafka -> Spark Structured Streaming -> MongoDB Atlas
Page 1 reads Kafka directly (real-time). Page 2 queries Atlas (history).
"""

import json
import os
import socket
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st
from dotenv import load_dotenv
from kafka import KafkaConsumer, TopicPartition
from kafka.errors import KafkaError
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from streamlit_autorefresh import st_autorefresh

load_dotenv(Path(__file__).parent / ".env")

st.set_page_config(
    page_title="Streaming Data Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

MONGO_URI = os.getenv("MONGO_URI", "")  # never shown in the UI
DEFAULT_DB = os.getenv("MONGO_DB", "weather_db")
DEFAULT_COLLECTION = os.getenv("MONGO_COLLECTION", "readings")
REQUIRED_FIELDS = ("timestamp", "value", "metric_type", "sensor_id")
BACKFILL = 200       # messages replayed when the dashboard connects
BUFFER_SIZE = 500    # rolling real-time buffer
TIME_RANGES = {
    "1h": timedelta(hours=1), "24h": timedelta(hours=24),
    "7d": timedelta(days=7), "30d": timedelta(days=30),
}
AGG_RULES = {"hourly": "1h", "daily": "1D", "weekly": "1W"}


def utc_now():
    """Naive UTC 'now' (all timestamps in the app are naive UTC)."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


# --------------------------------------------------------------------- sidebar
def setup_sidebar():
    st.sidebar.title("Dashboard Controls")

    st.sidebar.subheader("Data Source Configuration")
    kafka_broker = st.sidebar.text_input(
        "Kafka Broker", value=os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"))
    kafka_topic = st.sidebar.text_input(
        "Kafka Topic", value=os.getenv("KAFKA_TOPIC", "streaming-data"))

    st.sidebar.subheader("Storage Configuration")
    storage_type = st.sidebar.selectbox("Storage Type", ["MongoDB Atlas"])
    mongo_db = st.sidebar.text_input("Database", value=DEFAULT_DB)
    mongo_collection = st.sidebar.text_input("Collection", value=DEFAULT_COLLECTION)

    return {
        "kafka_broker": kafka_broker,
        "kafka_topic": kafka_topic,
        "storage_type": storage_type,
        "mongo_db": mongo_db,
        "mongo_collection": mongo_collection,
    }


# ----------------------------------------------------------------------- MongoDB
@st.cache_resource(show_spinner=False)
def get_mongo_client(uri):
    client = MongoClient(uri, serverSelectionTimeoutMS=8000)
    client.admin.command("ping")  # fail fast; failures are not cached
    return client


@st.cache_data(ttl=30, show_spinner=False)
def _fetch_historical(uri, db, coll, time_range, metrics, sensors):
    since = datetime.now(timezone.utc) - TIME_RANGES[time_range]
    query = {"timestamp": {"$gte": since}}
    if metrics:
        query["metric_type"] = {"$in": list(metrics)}
    if sensors:
        query["sensor_id"] = {"$in": list(sensors)}
    cursor = (
        get_mongo_client(uri)[db][coll]
        .find(query, {"_id": 0, "timestamp": 1, "value": 1, "metric_type": 1,
                      "sensor_id": 1, "location": 1})
        .sort("timestamp", 1)
        .limit(50000)
    )
    return pd.DataFrame(list(cursor))


@st.cache_data(ttl=60, show_spinner=False)
def _distinct(uri, db, coll, field):
    return sorted(get_mongo_client(uri)[db][coll].distinct(field))


@st.cache_data(ttl=15, show_spinner=False)
def _mongo_stats(uri, db, coll):
    c = get_mongo_client(uri)[db][coll]
    latest = c.find_one({}, {"_id": 0, "ingested_at": 1}, sort=[("ingested_at", -1)])
    return c.estimated_document_count(), latest


def query_historical_data(config, time_range="24h", metrics=None, sensors=None,
                          aggregation="raw"):
    """Query MongoDB Atlas. Returns a DataFrame (maybe empty) or None on error."""
    if not MONGO_URI:
        st.error("MONGO_URI is not set. Add it to environment/.env.")
        return None
    try:
        df = _fetch_historical(
            MONGO_URI, config["mongo_db"], config["mongo_collection"],
            time_range, tuple(metrics or ()), tuple(sensors or ()),
        )
    except PyMongoError as e:
        st.error(f"MongoDB query failed: {e}")
        return None

    if df.empty:
        return df
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    rule = AGG_RULES.get(aggregation)
    if rule:
        df = (df.set_index("timestamp")
                .groupby(["sensor_id", "metric_type"])["value"]
                .resample(rule).mean().dropna().reset_index())
    return df


# ------------------------------------------------------------------------ Kafka
def _safe_json(raw):
    try:
        return json.loads(raw.decode("utf-8"))
    except (ValueError, UnicodeDecodeError):
        return None  # malformed message: skipped later


def _get_consumer(broker, topic):
    key = f"kafka_consumer_{broker}_{topic}"
    consumer = st.session_state.get(key)
    if consumer is None:
        consumer = KafkaConsumer(
            bootstrap_servers=[broker],
            enable_auto_commit=False,
            value_deserializer=_safe_json,
        )
        partitions = consumer.partitions_for_topic(topic)
        if not partitions:
            consumer.close()
            raise KafkaError(f"Topic '{topic}' not found")
        tps = [TopicPartition(topic, p) for p in partitions]
        consumer.assign(tps)
        begins, ends = consumer.beginning_offsets(tps), consumer.end_offsets(tps)
        for tp in tps:  # start BACKFILL messages before the end so the chart isn't empty
            consumer.seek(tp, max(begins[tp], ends[tp] - BACKFILL))
        st.session_state[key] = consumer
    return consumer


def _parse_record(data):
    missing = [k for k in REQUIRED_FIELDS if k not in data]
    if missing:
        raise ValueError(f"missing fields {missing}")
    ts = datetime.fromisoformat(str(data["timestamp"]).replace("Z", "+00:00"))
    if ts.tzinfo is not None:
        ts = ts.astimezone(timezone.utc).replace(tzinfo=None)
    return {
        "timestamp": ts,
        "value": float(data["value"]),
        "metric_type": data["metric_type"],
        "sensor_id": data["sensor_id"],
        "location": data.get("location", data["sensor_id"]),
    }


def consume_kafka_data(config):
    """Poll Kafka into a rolling buffer. Returns a DataFrame (maybe empty) or None on error."""
    broker, topic = config["kafka_broker"], config["kafka_topic"]
    buf = st.session_state.setdefault(f"rt_buffer_{broker}_{topic}", [])
    skipped = 0
    try:
        consumer = _get_consumer(broker, topic)
        for _ in range(3):
            batches = consumer.poll(timeout_ms=1000, max_records=500)
            if not batches:
                break
            for msgs in batches.values():
                for m in msgs:
                    try:
                        buf.append(_parse_record(m.value))
                    except (ValueError, TypeError, AttributeError):
                        skipped += 1
        del buf[:-BUFFER_SIZE]
    except KafkaError as e:  # includes NoBrokersAvailable
        st.session_state.pop(f"kafka_consumer_{broker}_{topic}", None)  # retry next refresh
        st.error(f"Kafka error: {e}")
        return None
    except Exception as e:
        st.session_state.pop(f"kafka_consumer_{broker}_{topic}", None)
        st.error(f"Unexpected Kafka error: {e}")
        return None
    if skipped:
        st.caption(f"Skipped {skipped} malformed message(s).")
    return pd.DataFrame(buf)


# ------------------------------------------------------------------ pipeline status
def broker_reachable(broker):
    host, _, port = broker.partition(":")
    try:
        with socket.create_connection((host, int(port or 9092)), timeout=2):
            return True
    except (OSError, ValueError):
        return False


def show_pipeline_status(config):
    st.sidebar.subheader("Pipeline Status")
    st.sidebar.write("Kafka:", "🟢 up" if broker_reachable(config["kafka_broker"]) else "🔴 down")
    if not MONGO_URI:
        st.sidebar.write("MongoDB Atlas: 🔴 MONGO_URI missing")
        return
    try:
        count, latest = _mongo_stats(MONGO_URI, config["mongo_db"], config["mongo_collection"])
        st.sidebar.write(f"MongoDB Atlas: 🟢 {count:,} documents")
        if latest and latest.get("ingested_at"):
            st.sidebar.caption(f"Last Spark write: {latest['ingested_at']:%Y-%m-%d %H:%M:%S} UTC")
    except PyMongoError as e:
        st.sidebar.write("MongoDB Atlas: 🔴 unreachable")
        st.sidebar.caption(str(e)[:120])


# ----------------------------------------------------------------------- charts
def plot_series(df, title):
    """One line per city, one panel per metric (units differ, so no shared y-axis)."""
    n = df["metric_type"].nunique()
    fig = px.line(
        df.sort_values("timestamp"), x="timestamp", y="value",
        color="sensor_id", facet_row="metric_type", markers=True,
        title=title, template="plotly_white", height=max(350, 260 * n),
        labels={"timestamp": "Time (UTC)", "value": "Value", "sensor_id": "City"},
    )
    fig.update_yaxes(matches=None)
    fig.for_each_annotation(lambda a: a.update(text=a.text.split("=")[-1]))
    fig.update_layout(hovermode="x unified")
    st.plotly_chart(fig, width="stretch")


# ------------------------------------------------------------------------ views
def display_real_time_view(config, refresh_interval):
    st.header("📈 Real-time Streaming Dashboard")
    state = st.session_state.refresh_state
    st.info(f"**Auto-refresh:** {'🟢 Enabled' if state['auto_refresh'] else '🔴 Disabled'}"
            f" - Updates every {refresh_interval} seconds")

    with st.spinner("Fetching real-time data from Kafka..."):
        df = consume_kafka_data(config)

    if df is None:
        return
    if df.empty:
        st.info("No messages yet. Is producer.py running? WeatherAPI publishes a new "
                "reading every ~10-15 min per city; the last "
                f"{BACKFILL} Kafka messages are replayed when the dashboard connects.")
        return

    newest = df["timestamp"].max()
    age_min = (utc_now() - newest).total_seconds() / 60
    c1, c2, c3 = st.columns(3)
    c1.metric("Records in buffer", len(df))
    c2.metric("Cities", df["sensor_id"].nunique())
    c3.metric("Newest reading (UTC)", newest.strftime("%H:%M"), f"{age_min:.0f} min old",
              delta_color="off")

    plot_series(df, f"Live stream (last {len(df)} records)")
    with st.expander("📋 View Raw Data"):
        st.dataframe(df.sort_values("timestamp", ascending=False), width="stretch", height=300)


def display_historical_view(config):
    st.header("📊 Historical Data Analysis")
    st.caption(f"Source: MongoDB Atlas · {config['mongo_db']}.{config['mongo_collection']}"
               " · written by Spark Structured Streaming")
    if not MONGO_URI:
        st.error("MONGO_URI is not set. Add it to environment/.env.")
        return
    try:
        metric_options = _distinct(MONGO_URI, config["mongo_db"], config["mongo_collection"], "metric_type")
        sensor_options = _distinct(MONGO_URI, config["mongo_db"], config["mongo_collection"], "sensor_id")
    except PyMongoError as e:
        st.error(f"Could not read from MongoDB Atlas: {e}")
        return

    st.subheader("Data Filters")
    c1, c2, c3, c4 = st.columns(4)
    time_range = c1.selectbox("Time Range", list(TIME_RANGES), index=1)
    metrics = c2.multiselect("Metric Type", metric_options, help="Empty = all")
    sensors = c3.multiselect("City (sensor_id)", sensor_options, help="Empty = all")
    aggregation = c4.selectbox("Aggregation", ["raw", "hourly", "daily", "weekly"])

    df = query_historical_data(config, time_range, metrics, sensors, aggregation)
    if df is None:
        return
    if df.empty:
        st.info("No data in this range. Check that spark_streaming.py is running.")
        return

    st.subheader("Historical Data Table")
    st.dataframe(df, width="stretch", hide_index=True)

    st.subheader("Historical Trends")
    plot_series(df, f"Historical trend ({time_range}, {aggregation})")

    st.subheader("Data Summary")
    m1, m2 = st.columns(2)
    m1.metric("Total Records", len(df))
    m2.metric("Date Range (UTC)",
              f"{df['timestamp'].min():%Y-%m-%d %H:%M} to {df['timestamp'].max():%Y-%m-%d %H:%M}")
    st.dataframe(
        df.groupby(["metric_type", "sensor_id"])["value"]
          .agg(["count", "mean", "min", "max"]).round(2).reset_index(),
        width="stretch", hide_index=True,
    )


# ------------------------------------------------------------------------- main
def main():
    st.title("🚀 Streaming Data Dashboard")
    st.caption("WeatherAPI → Kafka → Spark Structured Streaming → MongoDB Atlas → Streamlit")

    if "refresh_state" not in st.session_state:
        st.session_state.refresh_state = {"last_refresh": datetime.now(), "auto_refresh": True}

    config = setup_sidebar()

    st.sidebar.subheader("Refresh Settings")
    st.session_state.refresh_state["auto_refresh"] = st.sidebar.checkbox(
        "Enable Auto Refresh", value=st.session_state.refresh_state["auto_refresh"])

    refresh_interval = 15  # defined even when auto-refresh is off
    if st.session_state.refresh_state["auto_refresh"]:
        refresh_interval = st.sidebar.slider("Refresh Interval (seconds)", 5, 60, 15)
        st_autorefresh(interval=refresh_interval * 1000, key="auto_refresh")

    if st.sidebar.button("🔄 Manual Refresh"):
        st.session_state.refresh_state["last_refresh"] = datetime.now()
        st.cache_data.clear()
        st.rerun()

    st.sidebar.markdown("---")
    show_pipeline_status(config)

    tab1, tab2 = st.tabs(["📈 Real-time Streaming", "📊 Historical Data"])
    with tab1:
        display_real_time_view(config, refresh_interval)
    with tab2:
        display_historical_view(config)


if __name__ == "__main__":
    main()