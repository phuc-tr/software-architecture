import pandas as pd
from influxdb_client import InfluxDBClient
from tabulate import tabulate
import statistics
import folium
import os
import time

# ---------------- CONFIG ----------------
INFLUX_URL = os.getenv("INFLUX_URL", "http://localhost:8086")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN", "")
INFLUX_ORG = os.getenv("INFLUX_ORG", "univaq")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET", "sensor_data")

FETCH_INTERVAL = 60     # seconds
HISTORY_LENGTH = 10
META_FILE = "sensors_data/ca_meta.csv"
MAP_FILE = "traffic_map.html"

# ----------------------------------------
print("🔗 Connecting to InfluxDB...")
client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
query_api = client.query_api()

# ----------------------------------------
print("Loading sensor metadata...")
meta_df = pd.read_csv(META_FILE)
meta_df["ID"] = meta_df["ID"].astype(str)

# ----------------------------------------
def fetch_all_sensor_ids():
    query = f'''
    import "influxdata/influxdb/schema"
    schema.tagValues(bucket: "{INFLUX_BUCKET}", tag: "station_id")
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
      |> limit(n:{n + 1})
    '''
    result = query_api.query(query)
    return [r.get_value() for t in result for r in t.records]

def determine_trend(latest, history):
    if len(history) < 3:
        return "Normal"

    median_prev = statistics.median(history)
    threshold = median_prev * 0.05

    if latest > median_prev + threshold:
        return "Increasing"
    elif latest < median_prev - threshold:
        return "Decreasing"
    else:
        return "Normal"

# ----------------------------------------
print("Live Traffic Monitor started (refresh every 1 min)...")

while True:
    table = []

    sensor_ids = fetch_all_sensor_ids()

    # Map center (California)
    fmap = folium.Map(location=[37.5, -121.5], zoom_start=7)

    for sid in sensor_ids:
        values = fetch_last_n(sid, HISTORY_LENGTH)
        if len(values) < 2:
            continue

        latest = values[0]
        previous = values[1:]
        trend = determine_trend(latest, previous)

        meta = meta_df[meta_df["ID"] == sid]
        if meta.empty:
            continue

        row = meta.iloc[0]
        lat, lng = row["Lat"], row["Lng"]

        color = {
            "Increasing": "red",
            "Decreasing": "blue",
            "Normal": "green"
        }[trend]

        # Add map marker
        folium.CircleMarker(
            location=[lat, lng],
            radius=6,
            color=color,
            fill=True,
            fill_opacity=0.8,
            popup=(
                f"Sensor: {sid}<br>"
                f"Latest: {latest}<br>"
                f"Trend: {trend}<br>"
                f"Road: {row['Fwy']} {row['Direction']}"
            )
        ).add_to(fmap)

        table.append([
            sid,
            latest,
            trend,
            row["Fwy"],
            row["Direction"],
            lat,
            lng
        ])

    # Save map
    fmap.save(MAP_FILE)

    # Terminal output
    print("\033c", end="")
    print("🚦 Live Traffic Trend Monitor\n")
    print(tabulate(
        table,
        headers=["Sensor ID", "Latest", "Trend", "Road", "Dir", "Lat", "Lng"],
        tablefmt="fancy_grid"
    ))
    print(f"\nMap updated → open '{MAP_FILE}' and refresh browser")

    time.sleep(FETCH_INTERVAL)
