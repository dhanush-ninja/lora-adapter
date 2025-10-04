"""
ChirpStack MQTT Subscriber for LoRa Adapter

Subscribes to ChirpStack MQTT uplink events and forwards them to SuperMQ.
Based on ChirpStack integration docs: https://www.chirpstack.io/docs/chirpstack/integrations/events.html
"""
import asyncio
import json
import logging
import threading
from typing import Dict, Any, Optional
import paho.mqtt.client as mqtt

from route_map import RouteMapManager
from config import config

logger = logging.getLogger(__name__)


class ChirpStackMQTTSubscriber:
    """MQTT subscriber for ChirpStack uplink events"""
    
    def __init__(self, route_map: RouteMapManager, message_forwarder):
        self.route_map = route_map
        self.message_forwarder = message_forwarder
        self.running = False
        self.client = None
        self.loop = None
        self.mqtt_thread = None
        self.queue: Optional[asyncio.Queue] = None
        self.workers = []
        
        # Get MQTT config from environment
        self.mqtt_host = config.chirpstack_mqtt_host
        self.mqtt_port = config.chirpstack_mqtt_port
        self.mqtt_username = config.chirpstack_mqtt_username
        self.mqtt_password = config.chirpstack_mqtt_password
        # Tuning
        self.qos = max(0, min(2, int(config.chirpstack_mqtt_qos)))
        self.client_id = config.chirpstack_mqtt_client_id or f"lora-adapter-{id(self) & 0xffff}"
        self.keepalive = config.chirpstack_mqtt_keepalive
        self.reconnect_min = config.chirpstack_mqtt_reconnect_min
        self.reconnect_max = config.chirpstack_mqtt_reconnect_max
        self.max_inflight = config.chirpstack_mqtt_max_inflight
        self.queue_size = config.mqtt_queue_size
        self.worker_count = max(1, config.mqtt_workers)
        
        logger.info(f"MQTT config: {self.mqtt_host}:{self.mqtt_port}")
        if self.mqtt_username:
            logger.info("MQTT authentication enabled")
    
    def on_connect(self, client, userdata, flags, rc):
        """Callback for when the client receives a CONNACK response from the server"""
        if rc == 0:
            logger.info(f"Connected to ChirpStack MQTT at {self.mqtt_host}:{self.mqtt_port}")
            # Subscribe to uplink events
            topic = "application/+/device/+/event/up"
            client.subscribe(topic, qos=self.qos)
            logger.info(f"Subscribed to ChirpStack uplink events: {topic}")
        else:
            logger.error(f"Failed to connect to MQTT broker, return code {rc}")
    
    def on_disconnect(self, client, userdata, rc):
        """Callback for when the client disconnects from the broker"""
        logger.info("Disconnected from ChirpStack MQTT")
    
    def on_message(self, client, userdata, msg):
        """Callback for when a PUBLISH message is received from the server"""
        try:
            # Enqueue for async workers to process
            if self.loop and not self.loop.is_closed() and self.queue:
                try:
                    self.queue.put_nowait((msg.topic, bytes(msg.payload)))
                except asyncio.QueueFull:
                    logger.warning("MQTT queue full, dropping message to apply backpressure")
        except Exception as e:
            logger.error(f"Error in MQTT message callback: {e}")
    
    async def handle_uplink_message(self, topic: str, payload: bytes):
        """Handle ChirpStack uplink message"""
        try:
            # Extract application ID and device EUI from topic
            # Topic format: application/{app_id}/device/{dev_eui}/event/up
            topic_parts = topic.split('/')
            
            if len(topic_parts) >= 6 and topic_parts[0] == "application" and topic_parts[4] == "event" and topic_parts[5] == "up":
                app_id = topic_parts[1]
                dev_eui = topic_parts[3]
                
                # Parse message payload
                try:
                    chirpstack_payload = json.loads(payload.decode('utf-8'))
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to parse ChirpStack message payload: {e}")
                    return
                
                logger.info(f"Received uplink: app_id={app_id}, dev_eui={dev_eui}")
                logger.debug(f"Payload: {json.dumps(chirpstack_payload, indent=2)}")
                
                # Forward to SuperMQ
                await self.forward_to_supermq(app_id, dev_eui, chirpstack_payload)
            else:
                logger.warning(f"Unexpected topic format: {topic}")
                
        except Exception as e:
            logger.error(f"Error handling uplink message: {e}")
    
    async def start(self):
        """Start the MQTT subscriber"""
        self.running = True
        self.loop = asyncio.get_event_loop()
        self.queue = asyncio.Queue(maxsize=self.queue_size)
        
        # Create MQTT client
        self.client = mqtt.Client(client_id=self.client_id, clean_session=False)
        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect
        self.client.on_message = self.on_message
        # Tuning socket + inflight
        try:
            self.client.max_inflight_messages_set(self.max_inflight)
        except Exception:
            pass
        
        # Set credentials if provided
        if self.mqtt_username and self.mqtt_password:
            self.client.username_pw_set(self.mqtt_username, self.mqtt_password)
            logger.info("Using MQTT authentication")
        
        try:
            # Connect to MQTT broker
            logger.info(f"Connecting to ChirpStack MQTT at {self.mqtt_host}:{self.mqtt_port}")
            # configure automatic reconnect backoff
            try:
                self.client.reconnect_delay_set(min_delay=self.reconnect_min, max_delay=self.reconnect_max)
            except Exception:
                pass
            self.client.connect(self.mqtt_host, self.mqtt_port, keepalive=self.keepalive)
            
            # Start MQTT loop in a separate thread
            def mqtt_thread_func():
                self.client.loop_forever(retry_first_connection=True)
            
            self.mqtt_thread = threading.Thread(target=mqtt_thread_func, daemon=True)
            self.mqtt_thread.start()
            
            # Start worker tasks
            for i in range(self.worker_count):
                self.workers.append(asyncio.create_task(self._worker(i)))
            
            # Keep the main thread alive
            while self.running:
                await asyncio.sleep(1)
                
        except Exception as e:
            logger.error(f"ChirpStack MQTT subscriber error: {e}")
            raise

    async def _worker(self, worker_id: int):
        """Worker that consumes messages from queue and forwards them"""
        logger.info(f"MQTT worker-{worker_id} started")
        try:
            while self.running:
                topic, payload = await self.queue.get()
                try:
                    await self.handle_uplink_message(topic, payload)
                except Exception as e:
                    logger.error(f"Worker-{worker_id} error processing message: {e}")
                finally:
                    self.queue.task_done()
        except asyncio.CancelledError:
            logger.info(f"MQTT worker-{worker_id} cancelled")
        except Exception as e:
            logger.error(f"Worker-{worker_id} crashed: {e}")
    
    async def forward_to_supermq(self, app_id: str, dev_eui: str, payload: Dict[str, Any]):
        """Forward ChirpStack message to SuperMQ"""
        try:
            # Look up client and channel from route maps
            client_id = await self.route_map.get_client_route(dev_eui)
            if not client_id:
                logger.warning(f"No client route found for dev_eui: {dev_eui}")
                return
            
            channel_route = await self.route_map.get_channel_route(app_id)
            if not channel_route:
                logger.warning(f"No channel route found for app_id: {app_id}")
                return
            
            channel_id = channel_route["channel_id"]
            domain_id = channel_route["domain_id"]
            
            # Check if client is connected to the channel
            client_connection = await self.route_map.get_client_channel_connection(client_id)
            if not client_connection:
                logger.warning(f"Client {client_id} is not connected to any channel - dropping message")
                return
            
            # Verify the client is connected to the target channel
            if (client_connection.get("channel_id") != channel_id or 
                client_connection.get("domain_id") != domain_id):
                logger.warning(f"Client {client_id} is not connected to target channel {channel_id} (domain: {domain_id}) - dropping message")
                return
            
            logger.info(f"Forwarding message: {dev_eui} -> {client_id}, {app_id} -> {channel_id}")
            
            # Extract actual sensor data from ChirpStack payload
            sensor_data = self.extract_sensor_data(payload)
            
            metadata_fields = []
            if "device_name" in sensor_data:
                metadata_fields.append(f"deviceName='{sensor_data['device_name']}'")
            if "tags" in sensor_data:
                metadata_fields.append(f"tags={sensor_data['tags']}")
            if metadata_fields:
                logger.info(f"Including metadata: {', '.join(metadata_fields)}")
            
            # Forward to SuperMQ via message forwarder
            await self.message_forwarder.forward_message(
                client_id=client_id,
                channel_id=channel_id,
                domain_id=domain_id,
                payload=sensor_data,
                protocol="lora"
            )
            
        except Exception as e:
            logger.error(f"Error forwarding message to SuperMQ: {e}")
    
    def extract_sensor_data(self, chirpstack_payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract sensor data from ChirpStack uplink payload
        
        ChirpStack payload structure:
        {
          "applicationID": "1",
          "applicationName": "Test app",
          "deviceInfo": {
            "tenantId": "52f14cd4-c6f1-4fbd-8f87-4025e1d49242",
            "tenantName": "ChirpStack",
            "applicationId": "17c82e96-be03-4f38-aef3-f83d48582d97",
            "applicationName": "Test application",
            "deviceProfileId": "14855bf7-d10d-4aee-b618-ebfcb64dc7ad",
            "deviceProfileName": "Test device-profile",
            "deviceName": "Test device",
            "devEui": "0101010101010101",
            "tags": {
              "key": "value"
            }
          },
          "devEUI": "0102030405060708",
          "rxInfo": [...],
          "txInfo": {...},
          "adr": true,
          "dr": 5,
          "fCnt": 10,
          "fPort": 1,
          "data": "Zm9vYmFy", // base64 encoded
          "object": {            // decoded data (if decoder configured)
            "temperature": 20.5,
            "humidity": 65.2
          }
        }
        """
        
        sensor_data = {}
        if "object" in chirpstack_payload and chirpstack_payload["object"]:
            logger.debug("Using decoded sensor data from 'object' field")
            sensor_data.update(chirpstack_payload["object"])
    
        
        if "deviceInfo" in chirpstack_payload and chirpstack_payload["deviceInfo"]:
            device_info = chirpstack_payload["deviceInfo"]
            
            if "deviceName" in device_info:
                sensor_data["device_name"] = device_info["deviceName"]
                logger.debug(f"Added device name: {device_info['deviceName']}")
            
            if "tags" in device_info and device_info["tags"]:
                sensor_data["tags"] = device_info["tags"]
                logger.debug(f"Added device tags: {device_info['tags']}")

            if "applicationName" in device_info:
                sensor_data["app_name"] = device_info["applicationName"]
                
            # if "tenantName" in device_info:
            #     sensor_data["tenant_name"] = device_info["tenantName"]
            
            # if "deviceProfileName" in device_info:
            #     sensor_data["device_profile_name"] = device_info["deviceProfileName"]
        
        # Add raw data if no decoded object
        if "rxInfo" in chirpstack_payload:
            sensor_data["rx_info"] = chirpstack_payload["rxInfo"]
        if "txInfo" in chirpstack_payload:
            sensor_data["tx_info"] = chirpstack_payload["txInfo"]
        if "data" in chirpstack_payload and not ("object" in chirpstack_payload and chirpstack_payload["object"]):
            sensor_data["raw_data"] = chirpstack_payload["data"]
            logger.warning("No decoded sensor data available, including raw base64 data")
        
        return sensor_data
    
    async def stop(self):
        """Stop the MQTT subscriber"""
        self.running = False
        
        if self.client:
            self.client.disconnect()
            
        if self.mqtt_thread and self.mqtt_thread.is_alive():
            self.mqtt_thread.join(timeout=5)
        
        # Cancel workers
        for t in self.workers:
            t.cancel()
        if self.queue:
            # Drain queue quickly
            try:
                while not self.queue.empty():
                    self.queue.get_nowait()
                    self.queue.task_done()
            except Exception:
                pass
            
        logger.info("ChirpStack MQTT subscriber stopped")