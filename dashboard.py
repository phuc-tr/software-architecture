import os
import statistics
import pandas as pd
import streamlit as st
import folium

from influxdb_client import InfluxDBClient
from streamlit_folium import st_folium
from streamlit_autorefresh import st_autorefresh

# ---------------- CONFIG ----------------
INFLUX_URL = os.getenv("INFLUX_URL", "http://localhost:8086")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN", "")
INFLUX_ORG = os.getenv("INFLUX_ORG", "univaq")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET", "sensor_data")

REFRESH_INTERVAL_SEC = 60
HISTORY_LENGTH = 10
META_FILE = "sensors_data/ca_meta.csv"

# ---------------- AUTO REFRESH ----------------
st_autorefresh(interval=REFRESH_INTERVAL_SEC * 1000, key="refresh")

# ---------------- PAGE SETUP ----------------
st.set_page_config(layout="wide", page_title="Live Traffic Monitor")
st.title("Live Map Dashboard with Traffic Sensor ")

# ---------------- LOAD METADATA ----------------
meta_df = pd.read_csv(META_FILE)
meta_df["ID"] = meta_df["ID"].astype(str)

# ---------------- INFLUX CONNECTION ----------------
client = InfluxDBClient(
    url=INFLUX_URL,
    token=INFLUX_TOKEN,
    org=INFLUX_ORG
)
query_api = client.query_api()

# ---------------- FUNCTIONS ----------------
def fetch_all_sensor_ids():
    query = f'''
    import "influxdata/influxdb/schema"
    schema.tagValues(
        bucket: "{INFLUX_BUCKET}",
        tag: "station_id"
    )
    '''
    result = query_api.query(query)
    return [r.get_value() for t in result for r in t.records]


def fetch_last_n(sensor_id, n):
    query = f'''
    from(bucket:"{INFLUX_BUCKET}")
      |> range(start: -2h)
      |> filter(fn: (r) =>
          r._measurement == "mqtt_traffic_data" and
          r.station_id == "{sensor_id}"
      )
      |> sort(columns: ["_time"], desc: true)
      |> limit(n:{n})
    '''
    result = query_api.query(query)
    return [r.get_value() for t in result for r in t.records]


def determine_trend(latest, previous):
    if len(previous) < 3:
        return "Normal"

    median_prev = statistics.median(previous)
    threshold = median_prev * 0.05

    if latest > median_prev + threshold:
        return "Increasing"
    elif latest < median_prev - threshold:
        return "Decreasing"
    return "Normal"


def trend_color(trend):
    return {
        "Increasing": "red",
        "Decreasing": "blue",
        "Normal": "green"
    }.get(trend, "gray")

# ---------------- MAIN DATA FETCH ----------------
rows = []
sensor_ids = fetch_all_sensor_ids()

for sid in sensor_ids:
    values = fetch_last_n(sid, HISTORY_LENGTH + 1)
    if len(values) < 2:
        continue

    latest = values[0]
    previous = values[1:]
    trend = determine_trend(latest, previous)

    meta = meta_df[meta_df["ID"] == sid]
    if meta.empty:
        continue

    meta = meta.iloc[0]

    rows.append({
        "Sensor ID": sid,
        "Latest Value": latest,
        "Trend": trend,
        "Previous 10": previous,
        "Lat": meta["Lat"],
        "Lng": meta["Lng"]
    })

df = pd.DataFrame(rows)

# ---------------- LAYOUT ----------------
col1, col2 = st.columns([1, 1.2])

with col1:
    st.subheader("Live Sensor Status")
    if not df.empty:
        st.dataframe(
            df[["Sensor ID", "Latest Value", "Trend", "Previous 10"]],
            use_container_width=True
        )
    else:
        st.info("No data available")

with col2:
    st.subheader("Traffic Map")

    if not df.empty:
        m = folium.Map(
            location=[df["Lat"].mean(), df["Lng"].mean()],
            zoom_start=10
        )

        for _, r in df.iterrows():
            folium.CircleMarker(
                location=[r["Lat"], r["Lng"]],
                radius=7,
                color=trend_color(r["Trend"]),
                fill=True,
                fill_color=trend_color(r["Trend"]),
                popup=f"""
                <b>Sensor:</b> {r['Sensor ID']}<br>
                <b>Latest:</b> {r['Latest Value']}<br>
                <b>Trend:</b> {r['Trend']}
                """
            ).add_to(m)

        st_folium(m, width=700, height=500)
    else:
        st.info("Map will appear once data is available")
