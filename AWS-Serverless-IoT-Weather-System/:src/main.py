import time
import json
import requests
from awsiot import mqtt_connection_builder
from awscrt import io, mqtt

# ==========================================
# 👇 YOUR CONFIGURATION IS NOW SET 👇
# ==========================================

ENDPOINT = "a1pssrdm8rhmnq-ats.iot.us-east-1.amazonaws.com"

# The specific certificate files you listed
PATH_TO_CERT = "certs/9f0f685c2e1a955b55b4950b81bed82a78b3d4eb3274d52f5770c750897f24b6-certificate.pem.crt"
PATH_TO_KEY = "certs/9f0f685c2e1a955b55b4950b81bed82a78b3d4eb3274d52f5770c750897f24b6-private.pem.key"
PATH_TO_ROOT = "certs/AmazonRootCA1.pem"

WEATHER_API_KEY = "b1f17e0ceb7b2aaa785f68bbb9531265"
CITY = "Buffalo" 

# ==========================================

def get_live_temperature():
    print(f"🌍 Checking weather for {CITY}...")
    url = f"http://api.openweathermap.org/data/2.5/weather?q={CITY}&appid={WEATHER_API_KEY}&units=metric"
    try:
        response = requests.get(url).json()
        if response.get('cod') != 200:
            print(f"❌ API Error: {response.get('message')}")
            return None
        temp = response['main']['temp']
        print(f"🌡️  Temperature found: {temp}°C")
        return temp
    except Exception as e:
        print(f"❌ Connection Error: {e}")
        return None

def main():
    # Spin up the MQTT Connection
    event_loop_group = io.EventLoopGroup(1)
    host_resolver = io.DefaultHostResolver(event_loop_group)
    client_bootstrap = io.ClientBootstrap(event_loop_group, host_resolver)

    mqtt_connection = mqtt_connection_builder.mtls_from_path(
        endpoint=ENDPOINT,
        cert_filepath=PATH_TO_CERT,
        pri_key_filepath=PATH_TO_KEY,
        ca_filepath=PATH_TO_ROOT,
        client_bootstrap=client_bootstrap,
        client_id="MacBook_Buffalo_Sensor",
        clean_session=False,
        keep_alive_secs=6
    )

    print(f"☁️  Connecting to AWS IoT Core...")
    try:
        connect_future = mqtt_connection.connect()
        connect_future.result() # Wait for connection
        print("✅ Connected!")
    except Exception as e:
        print(f"❌ Connection Failed. Check your cert paths! Error: {e}")
        return

    # Get Data
    temp = get_live_temperature()

    if temp is not None:
        payload = {
            "device_id": "MacBook_Sensor",
            "city": CITY,
            "temperature": temp
        }
        
        # Publish Data
        mqtt_connection.publish(
            topic="weather/data",
            payload=json.dumps(payload),
            qos=mqtt.QoS.AT_LEAST_ONCE
        )
        print(f"🚀 Sent to AWS: {json.dumps(payload)}")
    
    # Wait briefly to ensure message sends, then disconnect
    time.sleep(2)
    disconnect_future = mqtt_connection.disconnect()
    disconnect_future.result()
    print("🔌 Disconnected")

if __name__ == "__main__":
    main()