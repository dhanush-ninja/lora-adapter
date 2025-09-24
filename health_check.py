#!/usr/bin/env python3
"""
Health Check Script for LoRa Adapter

Tests connectivity to Redis, NATS, and MQTT services.
"""
import asyncio
import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from src.config import config
import redis.asyncio as redis
import nats


async def test_redis():
    """Test Redis connectivity"""
    print(f"🔍 Testing Redis connection: {config.redis_url}")
    try:
        client = redis.from_url(config.redis_url)
        await client.ping()
        await client.close()
        print("✅ Redis connection: OK")
        return True
    except Exception as e:
        print(f"❌ Redis connection: FAILED - {e}")
        return False


async def test_nats():
    """Test NATS connectivity"""
    print(f"🔍 Testing NATS connection: {config.supermq_nats_url}")
    try:
        connect_opts = {"connect_timeout": 5}
        if config.supermq_nats_token:
            connect_opts["token"] = config.supermq_nats_token
        
        nc = await nats.connect(config.supermq_nats_url, **connect_opts)
        await nc.close()
        print("✅ NATS connection: OK")
        return True
    except Exception as e:
        print(f"❌ NATS connection: FAILED - {e}")
        return False


async def test_mqtt():
    """Test MQTT connectivity"""
    print(f"🔍 Testing MQTT connection: {config.chirpstack_mqtt_host}:{config.chirpstack_mqtt_port}")
    try:
        import paho.mqtt.client as mqtt
        
        # Create MQTT client
        client = mqtt.Client()
        
        # Set credentials if provided
        if config.chirpstack_mqtt_username and config.chirpstack_mqtt_password:
            client.username_pw_set(config.chirpstack_mqtt_username, config.chirpstack_mqtt_password)
        
        # Connect test
        result = client.connect(config.chirpstack_mqtt_host, config.chirpstack_mqtt_port, 5)
        
        if result == 0:
            client.disconnect()
            print("✅ MQTT connection: OK")
            return True
        else:
            print(f"❌ MQTT connection: FAILED - Connection result code {result}")
            return False
            
    except Exception as e:
        print(f"❌ MQTT connection: FAILED - {e}")
        return False


async def main():
    """Run all health checks"""
    print("🏥 LoRa Adapter Health Check")
    print("=" * 40)
    
    results = []
    
    # Test all services
    results.append(await test_redis())
    results.append(await test_nats())
    results.append(await test_mqtt())
    
    print("\n" + "=" * 40)
    
    if all(results):
        print("🎉 All services are healthy!")
        sys.exit(0)
    else:
        print("⚠️  Some services are not available")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())