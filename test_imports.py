#!/usr/bin/env python3
"""
Simple test to verify MQTT subscriber imports work
"""
import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

try:
    from config import config
    print("✅ Config import: OK")
    
    print(f"📍 MQTT Host: {config.chirpstack_mqtt_host}")
    print(f"📍 MQTT Port: {config.chirpstack_mqtt_port}")
    print(f"📍 MQTT Username: {config.chirpstack_mqtt_username or 'None'}")
    print(f"📍 NATS URL: {config.supermq_nats_url}")
    print(f"📍 Redis URL: {config.redis_url}")
    
except Exception as e:
    print(f"❌ Config import failed: {e}")
    sys.exit(1)

try:
    import paho.mqtt.client as mqtt
    print("✅ paho-mqtt import: OK")
except Exception as e:
    print(f"❌ paho-mqtt import failed: {e}")
    sys.exit(1)

try:
    from mqtt_subscriber import ChirpStackMQTTSubscriber
    print("✅ MQTT subscriber import: OK")
except Exception as e:
    print(f"❌ MQTT subscriber import failed: {e}")
    sys.exit(1)

try:
    from route_map import RouteMapManager
    print("✅ Route map import: OK")
except Exception as e:
    print(f"❌ Route map import failed: {e}")
    sys.exit(1)

try:
    from message_forwarder import MessageForwarder
    print("✅ Message forwarder import: OK")
except Exception as e:
    print(f"❌ Message forwarder import failed: {e}")
    sys.exit(1)

print("🎉 All imports successful! Ready to build and run.")