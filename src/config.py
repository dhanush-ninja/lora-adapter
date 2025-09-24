"""
Configuration Management for LoRa Adapter

Simple configuration using environment variables.
"""
import os
from typing import Optional


class LoRaAdapterConfig:
    """Configuration for LoRa Adapter"""
    
    def __init__(self):
        # ChirpStack MQTT Configuration
        self.chirpstack_mqtt_host: str = os.getenv("CHIRPSTACK_MQTT_HOST", "localhost")
        self.chirpstack_mqtt_port: int = int(os.getenv("CHIRPSTACK_MQTT_PORT", "1883"))
        self.chirpstack_mqtt_username: Optional[str] = os.getenv("CHIRPSTACK_MQTT_USERNAME")
        self.chirpstack_mqtt_password: Optional[str] = os.getenv("CHIRPSTACK_MQTT_PASSWORD")
        self.chirpstack_mqtt_client_id: Optional[str] = os.getenv("CHIRPSTACK_MQTT_CLIENT_ID")
        self.chirpstack_mqtt_qos: int = int(os.getenv("CHIRPSTACK_MQTT_QOS", "1"))
        self.chirpstack_mqtt_keepalive: int = int(os.getenv("CHIRPSTACK_MQTT_KEEPALIVE", "60"))
        self.chirpstack_mqtt_reconnect_min: int = int(os.getenv("CHIRPSTACK_MQTT_RECONNECT_MIN", "1"))
        self.chirpstack_mqtt_reconnect_max: int = int(os.getenv("CHIRPSTACK_MQTT_RECONNECT_MAX", "30"))
        self.chirpstack_mqtt_max_inflight: int = int(os.getenv("CHIRPSTACK_MQTT_MAX_INFLIGHT", "20"))
        self.mqtt_queue_size: int = int(os.getenv("MQTT_QUEUE_SIZE", "1000"))
        self.mqtt_workers: int = int(os.getenv("MQTT_WORKERS", "2"))
        
        # SuperMQ NATS Configuration
        self.supermq_nats_url: str = os.getenv("SUPERMQ_NATS_URL", "nats://localhost:4222")
        self.supermq_nats_user: Optional[str] = os.getenv("SUPERMQ_NATS_USER")
        self.supermq_nats_password: Optional[str] = os.getenv("SUPERMQ_NATS_PASSWORD")
        self.js_batch_size: int = int(os.getenv("JS_BATCH_SIZE", "32"))
        self.js_fetch_timeout: float = float(os.getenv("JS_FETCH_TIMEOUT", "1.0"))
        
        # Redis Configuration
        self.redis_url: str = os.getenv("REDIS_URL", "redis://localhost:6379")
        self.redis_socket_timeout: float = float(os.getenv("REDIS_SOCKET_TIMEOUT", "5"))
        self.redis_connect_timeout: float = float(os.getenv("REDIS_CONNECT_TIMEOUT", "5"))
        self.redis_health_check_interval: int = int(os.getenv("REDIS_HEALTH_CHECK_INTERVAL", "30"))
        
        # Logging Configuration
        self.log_level: str = os.getenv("LOG_LEVEL", "INFO")
        self.log_format: str = os.getenv("LOG_FORMAT", "%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        
        # Application Configuration
        self.adapter_name: str = os.getenv("ADAPTER_NAME", "lora-adapter")
        self.health_check_interval: int = int(os.getenv("HEALTH_CHECK_INTERVAL", "30"))


# Global config instance
config = LoRaAdapterConfig()