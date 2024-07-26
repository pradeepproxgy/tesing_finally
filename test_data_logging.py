import paho.mqtt.client as mqtt
import pandas as pd
import os
import json
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(filename='mqtt_json.log', level=logging.INFO,
                    format='%(asctime)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

# Define MQTT broker details
broker_address = "de669efd434d46e09e6ba76d7c65279d.s2.eu.hivemq.cloud"
port = 8883  # Default MQTT over SSL/TLS port
username = "testdevice"
password = "testDevice@01"
topic = "topic/1/client/860181068984092/subscribe"

# Define CSV file path
csv_file = "mqtt_messages.csv"

# Create a DataFrame to hold the MQTT messages
if os.path.exists(csv_file) and os.path.getsize(csv_file) > 0:
    df = pd.read_csv(csv_file)
else:
    df = pd.DataFrame()

# MQTT callback for when a message is received
def on_message(client, userdata, message):
    try:
        payload = message.payload.decode('utf-8')
        json_data = json.loads(payload)  # Parse JSON string to dictionary
        logging.info(json_data)
        print(f"Received message: {json_data} on topic: {message.topic}")

        # Flatten JSON data
        flat_json = pd.json_normalize(json_data)

        # Add timestamp and topic columns
        flat_json['Timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        flat_json['Topic'] = message.topic

        global df
        # Append the flattened JSON data to the DataFrame
        df = pd.concat([df, flat_json], ignore_index=True)

        # Save the DataFrame to a CSV file
        df.to_csv(csv_file, index=False)
    except Exception as e:
        logging.error(f"Error processing message: {e}")
        print(f"Error processing message: {e}")

# MQTT setup
client = mqtt.Client()
client.username_pw_set(username, password)
client.on_message = on_message

# Set up TLS/SSL
client.tls_set()  # Use default TLS/SSL settings

try:
    client.connect(broker_address, port)
    client.subscribe(topic)
    client.loop_start()
except Exception as e:
    logging.error(f"Failed to connect to MQTT broker: {e}")
    print(f"Failed to connect to MQTT broker: {e}")

try:
    while True:
        pass  # Keep the script running
except KeyboardInterrupt:
    client.loop_stop()
    df.to_csv(csv_file, index=False)
    print("Script stopped and CSV file saved.")
