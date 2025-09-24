# LoRa Adapter for SuperMQ

A Python-based LoRa adapter that bridges ChirpStack MQTT messages to SuperMQ NATS, providing seamless integration between LoRaWAN devices and the SuperMQ IoT platform.

## Features

- **ChirpStack Integration**: Subscribes to ChirpStack MQTT uplink events
- **SuperMQ Integration**: Publishes messages to SuperMQ NATS in protobuf format
- **Dynamic Route Mapping**: Uses Redis to map device EUI to client ID and application ID to channel ID
- **Event-Driven Updates**: Automatically updates route maps based on SuperMQ client/channel events
- **Docker Support**: Containerized deployment with docker-compose

## Architecture

```
ChirpStack MQTT ──→ LoRa Adapter ──→ SuperMQ NATS
                         │
                    Redis Route Map
                         ↑
                 SuperMQ Events (NATS)
```

## Configuration

Set environment variables:

```bash
# ChirpStack MQTT
CHIRPSTACK_MQTT_HOST=localhost
CHIRPSTACK_MQTT_PORT=1883
CHIRPSTACK_MQTT_USERNAME=
CHIRPSTACK_MQTT_PASSWORD=
CHIRPSTACK_MQTT_CLIENT_ID=lora-adapter
CHIRPSTACK_MQTT_QOS=1
CHIRPSTACK_MQTT_KEEPALIVE=60
CHIRPSTACK_MQTT_RECONNECT_MIN=1
CHIRPSTACK_MQTT_RECONNECT_MAX=30
CHIRPSTACK_MQTT_MAX_INFLIGHT=20
MQTT_QUEUE_SIZE=1000
MQTT_WORKERS=2

# SuperMQ NATS
SUPERMQ_NATS_URL=nats://localhost:4222
SUPERMQ_NATS_USER=supermq
SUPERMQ_NATS_PASSWORD=supermq
JS_BATCH_SIZE=32
JS_FETCH_TIMEOUT=1.0

# Redis
REDIS_URL=redis://localhost:6379
REDIS_SOCKET_TIMEOUT=5
REDIS_CONNECT_TIMEOUT=5
REDIS_HEALTH_CHECK_INTERVAL=30

# Logging
LOG_LEVEL=INFO
```

## Quick Start

```bash
# Using Docker Compose
docker compose up -d
```

## Route Mapping

The adapter maintains two Redis hash maps:
- `lora:dev_eui:routes` - Maps device EUI to SuperMQ client ID
- `lora:app_id:routes` - Maps application ID to SuperMQ channel ID
-  `lora:client_channel:connections` - Maps Client ID Connection  to Channel ID

These are automatically updated when SuperMQ client/channel events are received.

## Tuning and performance

- MQTT subscriber
     - QoS: Set `CHIRPSTACK_MQTT_QOS` (default 1) for at-least-once delivery.
     - Reconnect backoff: `CHIRPSTACK_MQTT_RECONNECT_MIN`/`MAX` control reconnection jitter.
     - Inflight: `CHIRPSTACK_MQTT_MAX_INFLIGHT` tunes max concurrent inflight messages.
     - Backpressure: The adapter uses a bounded `MQTT_QUEUE_SIZE` and `MQTT_WORKERS` to decouple network IO from processing.

- NATS JetStream
     - Batch fetch: `JS_BATCH_SIZE` controls number of events pulled per fetch (default 32).
     - Fetch timeout: `JS_FETCH_TIMEOUT` controls how long to wait for batch fill.

- Redis
     - Timeouts: `REDIS_SOCKET_TIMEOUT`, `REDIS_CONNECT_TIMEOUT`, `REDIS_HEALTH_CHECK_INTERVAL` for stability.
     - Reverse deletions use HSCAN + pipeline to avoid large HGETALL scans.

## Message Flow

1. ChirpStack publishes uplink event to `application/{app_id}/device/{dev_eui}/event/up`
2. Adapter extracts `dev_eui` and `app_id` from MQTT topic and payload
3. Adapter looks up client ID and channel ID from Redis route maps
4. Adapter converts message to SuperMQ protobuf format
5. Adapter publishes to NATS subject `m.{domain_id}.{channel_id}`