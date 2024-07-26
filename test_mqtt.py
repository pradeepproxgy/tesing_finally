import paho.mqtt.client as mqtt

# Define MQTT broker details
broker_address = "ec2-13-201-126-159.ap-south-1.compute.amazonaws.com"
port = 8883  # Default port for MQTT over SSL/TLS
username = "admin"
password = "proxgy@1234"
topic = "topic/1/client/SLTD001/subscribe"

# Callback function when the client receives a CONNACK response from the server
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected to broker")
        client.subscribe(topic)
    else:
        print("Connection failed with code", rc)

# Callback function when a message is received from the server
def on_message(client, userdata, message):
    print(f"Received message '{message.payload.decode()}' on topic '{message.topic}' with QoS {message.qos}")

# Create a new MQTT client instance
client = mqtt.Client()

# Set username and password
client.username_pw_set(username, password)

# Attach the callback functions
client.on_connect = on_connect
client.on_message = on_message

# Set up TLS/SSL (Use default settings)
client.tls_set()

# Connect to the broker
client.connect(broker_address, port)

# Start the loop to process received messages
client.loop_start()

# Keep the script running
try:
    while True:
        pass
except KeyboardInterrupt:
    print("Disconnecting from broker...")
    client.loop_stop()
    client.disconnect()
    print("Disconnected.")
