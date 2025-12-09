import paho.mqtt.client as mqtt
import pandas as pd
import time
import ssl
import sys
import os

# --- Configuration ---
BROKER_ADDRESS = "ajtwy6h7wk989-ats.iot.eu-west-3.amazonaws.com"
BROKER_PORT = 8883
CSV_FILE = "data_ca_his_10days_2021.csv"
PUBLISH_INTERVAL_SECONDS = int(os.getenv("PUBLISH_INTERVAL_SECONDS", 5))
N_STATIONS = int(os.getenv("N_STATIONS", 5))

# --- PEM Credential Paths ---
CA_CERT_PATH = "/creds/ca.pem"
CLIENT_CERT_PATH = "/creds/cert.pem"
CLIENT_KEY_PATH = "/creds/key.pem"

# --- MQTT Callbacks ---

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        pass
    else:
        sys.exit(1)

def on_publish(client, userdata, mid):
    pass

# --- Data Loading ---

data = pd.read_csv(CSV_FILE)
print("Data loaded successfully.")
station_data = data.drop(columns=['Time'], errors='ignore') 
station_ids = station_data.columns.tolist()

if not station_ids:
    sys.exit(1)

# Apply N_STATIONS limit
station_ids_to_publish = station_ids[:N_STATIONS]
station_data_to_publish = station_data[station_ids_to_publish]


# --- Client Setup and Connection ---

client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1)
client.on_connect = on_connect
client.on_publish = on_publish

client.tls_set(
    ca_certs=CA_CERT_PATH,
    certfile=CLIENT_CERT_PATH,
    keyfile=CLIENT_KEY_PATH,
    tls_version=ssl.PROTOCOL_TLS_CLIENT
)

client.connect(BROKER_ADDRESS, BROKER_PORT, keepalive=60)
client.loop_start()

# --- Main Publishing Loop ---

try:
    for index, row in station_data_to_publish.iterrows():
        for station_id in station_ids_to_publish:
            topic = f"sensors/{station_id}"
            payload = str(row[station_id]) 
            
            client.publish(topic, payload, qos=1)

        time.sleep(PUBLISH_INTERVAL_SECONDS)

except KeyboardInterrupt:
    pass

finally:
    client.loop_stop() 
    client.disconnect()