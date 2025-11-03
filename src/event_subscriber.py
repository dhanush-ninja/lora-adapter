"""
SuperMQ NATS Event Subscriber for LoRa Adapter

Subscribes to SuperMQ events and updates route mappings automatically.
Based on the existing nats_event_subscriber.py with route map integration.
"""
import asyncio
import json
import logging
from typing import Dict, Any, Optional
import nats
from nats.errors import TimeoutError
import traceback

from route_map import RouteMapManager
from config import config

logger = logging.getLogger(__name__)


class SuperMQEventSubscriber:
    """NATS JetStream subscriber for SuperMQ events with route map updates"""

    def __init__(self, nats_url: str, user: str, password: str, route_map: RouteMapManager):
        self.nats_url = nats_url
        self.user = user
        self.password = password
        self.route_map = route_map
        self.nc = None
        self.js = None
        self.running = False
        self.consumer_name = "lora-adapter"
        self.reconnect_delay = 5  # seconds
        self.max_reconnect_delay = 60  # seconds
    
    async def connect(self):
        """Connect to NATS server with optional JetStream authentication"""
        connect_opts = {
            "connect_timeout": 10,
            "max_reconnect_attempts": -1,  # Unlimited reconnection attempts
            "reconnect_time_wait": 2,
            "allow_reconnect": True,
        }
        
        if self.user and self.password:
            connect_opts["user"] = self.user
            connect_opts["password"] = self.password
            logger.info("Connecting to NATS with authentication")
        else:
            logger.info("Connecting to NATS without authentication")
        
        try:
            logger.info(f"Attempting to connect to NATS at {self.nats_url}")
            
            # Add connection event handlers
            async def disconnected_cb():
                logger.warning("NATS connection lost!")
            
            async def reconnected_cb():
                logger.info("NATS connection restored!")
                # Reinitialize JetStream after reconnection
                try:
                    self.js = self.nc.jetstream()
                    await self.ensure_stream_exists()
                except Exception as e:
                    logger.error(f"Failed to reinitialize JetStream after reconnection: {e}")
            
            async def error_cb(e):
                logger.error(f"NATS connection error: {e}")
            
            async def closed_cb():
                logger.warning("NATS connection closed")
            
            connect_opts["disconnected_cb"] = disconnected_cb
            connect_opts["reconnected_cb"] = reconnected_cb
            connect_opts["error_cb"] = error_cb
            connect_opts["closed_cb"] = closed_cb
            
            self.nc = await asyncio.wait_for(
                nats.connect(self.nats_url, **connect_opts),
                timeout=15.0
            )
            self.js = self.nc.jetstream()
            logger.info(f"Successfully connected to NATS at {self.nats_url}")
        except asyncio.TimeoutError:
            logger.error(f"Connection timeout after 15 seconds to {self.nats_url}")
            raise
        except Exception as e:
            logger.error(f"Failed to connect to NATS: {e}")
            raise
    
    async def ensure_stream_exists(self):
        """Ensure the events stream exists"""
        try:
            stream_info = await self.js.stream_info("events")
            logger.info("Events stream already exists")
        except Exception:
            logger.info("Creating events stream...")
            try:
                stream_config = {
                    "name": "events",
                    "description": "SuperMQ stream for events",
                    "subjects": ["events.>"],
                    "retention": "limits",
                    "max_msgs_per_subject": 1000000000,
                    "max_age": 24 * 60 * 60 * 1000000000,  # 24 hours in nanoseconds
                    "max_msg_size": 1024 * 1024,  # 1MB
                    "storage": "file",
                }
                
                await self.js.add_stream(**stream_config)
                logger.info("Events stream created successfully")
            except Exception as e:
                logger.error(f"Could not create stream (may already exist): {e}")
    
    async def reconnect(self):
        """Reconnect to NATS with exponential backoff"""
        current_delay = self.reconnect_delay
        
        while self.running:
            try:
                logger.info(f"Attempting to reconnect to NATS...")
                
                # Close existing connection if any
                if self.nc and not self.nc.is_closed:
                    try:
                        await self.nc.close()
                    except Exception:
                        pass
                
                # Reconnect
                await self.connect()
                await self.ensure_stream_exists()
                logger.info("Successfully reconnected to NATS")
                return
                
            except Exception as e:
                logger.error(f"Reconnection failed: {e}")
                if self.running:
                    logger.info(f"Retrying reconnection in {current_delay} seconds...")
                    await asyncio.sleep(current_delay)
                    current_delay = min(current_delay * 2, self.max_reconnect_delay)
                else:
                    break
    
    def decode_message(self, msg):
        """Decode message data with multiple encoding attempts"""
        raw_data = msg.data
        
        logger.debug(f"Message subject: {msg.subject}")
        logger.debug(f"Message size: {len(raw_data)} bytes")
        
        # Try length-prefixed format (SuperMQ/NATS format)
        try:
            if len(raw_data) >= 3:
                for start_pos in range(0, min(10, len(raw_data))):
                    try:
                        json_part = raw_data[start_pos:].decode('utf-8')
                        if json_part.startswith('{'):
                            data = json.loads(json_part)
                            logger.debug("✅ Successfully decoded as length-prefixed JSON")
                            return data
                    except (UnicodeDecodeError, json.JSONDecodeError):
                        continue
        except Exception as e:
            logger.debug(f"❌ Length-prefixed decode error: {e}")
        
        # Try direct UTF-8 JSON
        try:
            decoded_str = raw_data.decode('utf-8')
            data = json.loads(decoded_str)
            logger.debug("✅ Successfully decoded as direct UTF-8 JSON")
            return data
        except (UnicodeDecodeError, json.JSONDecodeError):
            pass
        
        # Try Latin-1 encoding
        try:
            decoded_str = raw_data.decode('latin-1')
            json_start = decoded_str.find('{')
            if json_start >= 0:
                json_part = decoded_str[json_start:]
                data = json.loads(json_part)
                logger.debug(f"✅ Successfully decoded as Latin-1 JSON (offset: {json_start})")
                return data
        except (json.JSONDecodeError, Exception):
            pass
        
        logger.error("❌ Could not decode message with any known format")
        logger.error(f"Subject: {msg.subject}")
        logger.error(f"Raw hex: {raw_data[:100].hex()}")
        return None
    
    async def subscribe_to_client_events(self):
        """Subscribe to client events with reconnection handling"""
        current_delay = self.reconnect_delay
        
        while self.running:
            try:
                # Check if connection is still valid
                if not self.nc or self.nc.is_closed:
                    logger.info("NATS connection lost, attempting to reconnect...")
                    await self.reconnect()
                
                psub = await self.js.pull_subscribe(
                    subject="events.supermq.client.*",
                    durable=f"{self.consumer_name}-clients"
                )
                
                logger.info("Subscribed to client events: events.supermq.client.*")
                current_delay = self.reconnect_delay  # Reset delay on successful connection
                
                while self.running:
                    try:
                        msgs = await psub.fetch(
                            batch=getattr(config, 'js_batch_size', 32), 
                            timeout=getattr(config, 'js_fetch_timeout', 1.0)
                        )
                        for msg in msgs:
                            try:
                                await self.handle_client_event(msg)
                                await msg.ack()
                            except Exception as e:
                                logger.error(f"Client event processing failed, NAKing: {e}")
                                try:
                                    await msg.nak()
                                except Exception:
                                    pass
                    except TimeoutError:
                        continue  # Normal timeout, keep running
                    except Exception as e:
                        logger.error(f"Error in client event subscription: {e}")
                        break  # Break inner loop to reconnect
                        
            except Exception as e:
                logger.error(f"Failed to subscribe to client events: {e}")
                if self.running:
                    logger.info(f"Retrying client event subscription in {current_delay} seconds...")
                    await asyncio.sleep(current_delay)
                    current_delay = min(current_delay * 2, self.max_reconnect_delay)
    
    async def subscribe_to_channel_events(self):
        """Subscribe to channel events with reconnection handling"""
        current_delay = self.reconnect_delay
        
        while self.running:
            try:
                # Check if connection is still valid
                if not self.nc or self.nc.is_closed:
                    logger.info("NATS connection lost, attempting to reconnect...")
                    await self.reconnect()
                
                psub = await self.js.pull_subscribe(
                    subject="events.supermq.channel.*",
                    durable=f"{self.consumer_name}-channels"
                )
                
                logger.info("Subscribed to channel events: events.supermq.channel.*")
                current_delay = self.reconnect_delay  # Reset delay on successful connection
                
                while self.running:
                    try:
                        msgs = await psub.fetch(
                            batch=getattr(config, 'js_batch_size', 32), 
                            timeout=getattr(config, 'js_fetch_timeout', 1.0)
                        )
                        for msg in msgs:
                            try:
                                await self.handle_channel_event(msg)
                                await msg.ack()
                            except Exception as e:
                                logger.error(f"Channel event processing failed, NAKing: {e}")
                                try:
                                    await msg.nak()
                                except Exception:
                                    pass
                    except TimeoutError:
                        continue  # Normal timeout, keep running
                    except Exception as e:
                        logger.error(f"Error in channel event subscription: {e}")
                        break  # Break inner loop to reconnect
                        
            except Exception as e:
                logger.error(f"Failed to subscribe to channel events: {e}")
                if self.running:
                    logger.info(f"Retrying channel event subscription in {current_delay} seconds...")
                    await asyncio.sleep(current_delay)
                    current_delay = min(current_delay * 2, self.max_reconnect_delay)
    
    async def handle_client_event(self, msg):
        """Handle client events and update route maps"""
        try:
            data = self.decode_message(msg)
            if not data:
                return

            operation = data.get("operation", "")

            if operation in ["client.create", "client.update"]:
                await self.process_client_create_update(data)
            elif operation == "client.remove":
                await self.process_client_remove(data)
            else:
                logger.debug(f"Unhandled client operation: {operation}")
                
        except Exception as e:
            logger.error(f"Error handling client event: {e}")
    
    async def handle_channel_event(self, msg):
        """Handle channel events and update route maps"""
        try:
            data = self.decode_message(msg)
            if not data:
                return
                
            operation = data.get("operation", "")
       
            if operation in ["channel.create", "channel.update"]:
                await self.process_channel_create_update(data)
            elif operation == "channel.remove":
                await self.process_channel_remove(data)
            elif operation == "channel.connect":
                await self.process_channel_connect(data)
            elif operation == "channel.disconnect":
                await self.process_channel_disconnect(data)
            else:
                logger.debug(f"Unhandled channel operation: {operation}")
                
        except Exception as e:
            logger.error(f"Error handling channel event: {e}")
    
    async def process_client_create_update(self, data: Dict[str, Any]):
        """Process client create/update events and update route map"""
        try:
            client_id = data.get("id", "")
            metadata = data.get("metadata", {})
            
            # Extract LoRa DevEUI from metadata
            lora_metadata = metadata.get("lora")
            
            # If lora metadata is missing or empty, remove any existing routes for this client
            if not lora_metadata or not lora_metadata.get("dev_eui"):
                logger.info(f"Empty or missing LoRa metadata - removing client routes for: {client_id}")
                await self.route_map.remove_client_by_id(client_id)
                return
            
            dev_eui = lora_metadata.get("dev_eui", "")
            if not dev_eui:
                logger.warning("No dev_eui found in LoRa metadata")
                return
            
            logger.info(f"Updating client route: {dev_eui} -> {client_id}")
            await self.route_map.set_client_route(dev_eui, client_id)
            
        except Exception as e:
            logger.error(f"Error processing client create/update: {e}")
    
    async def process_client_remove(self, data: Dict[str, Any]):
        """Process client remove events and update route map"""
        try:
            client_id = data.get("id", "")
            logger.info(f"Removing client routes for: {client_id}")
            await self.route_map.remove_client_by_id(client_id)
            
        except Exception as e:
            logger.error(f"Error processing client remove: {e}")
    
    async def process_channel_create_update(self, data: Dict[str, Any]):
        """Process channel create/update events and update route map"""
        try:
            channel_id = data.get("id", "")
            domain_id = data.get("domain", "")
            metadata = data.get("metadata", {})
            
            # Extract LoRa AppID from metadata
            lora_metadata = metadata.get("lora")
            
            # If lora metadata is missing or empty, remove any existing routes for this channel
            if not lora_metadata or not lora_metadata.get("app_id"):
                logger.info(f"Empty or missing LoRa metadata - removing channel routes for: {channel_id} (domain: {domain_id})")
                await self.route_map.remove_channel_by_id(channel_id, domain_id)
                return
            
            app_id = lora_metadata.get("app_id", "")
            if not app_id:
                logger.warning("No app_id found in LoRa metadata")
                return
            
            logger.info(f"Updating channel route: {app_id} -> {channel_id} (domain: {domain_id})")
            await self.route_map.set_channel_route(app_id, channel_id, domain_id)
            
        except Exception as e:
            logger.error(f"Error processing channel create/update: {e}")
    
    async def process_channel_remove(self, data: Dict[str, Any]):
        """Process channel remove events and update route map"""
        try:
            channel_id = data.get("id", "")
            domain_id = data.get("domain", "")
            
            logger.info(f"Removing channel routes for: {channel_id} (domain: {domain_id})")
            await self.route_map.remove_channel_by_id(channel_id, domain_id)
            
        except Exception as e:
            logger.error(f"Error processing channel remove: {e}")
    
    async def process_channel_connect(self, data: Dict[str, Any]):
        """Process channel connect events - client connects to channel"""
        try:
            channel_id = data["channel_ids"][0]
            client_id = data["client_ids"][0]
            domain_id = data.get("domain", "")
            
            if not all([channel_id, client_id, domain_id]):
                logger.warning(f"Missing required fields in channel.connect: channel_id={channel_id}, client_id={client_id}, domain_id={domain_id}")
                return
            
            logger.info(f"Client connected to channel: {client_id} -> {channel_id} (domain: {domain_id})")
            await self.route_map.set_client_channel_connection(client_id, channel_id, domain_id)
            
        except Exception as e:
            
            logger.error(f"Error processing channel connect: {e}")
    
    async def process_channel_disconnect(self, data: Dict[str, Any]):
        """Process channel disconnect events - client disconnects from channel"""
        try:
            channel_id = data["channel_ids"][0]
            client_id = data["client_ids"][0]
            domain_id = data.get("domain", "")
            
            if not all([channel_id, client_id, domain_id]):
                logger.warning(f"Missing required fields in channel.disconnect: channel_id={channel_id}, client_id={client_id}, domain_id={domain_id}")
                return
            
            logger.info(f"Client disconnected from channel: {client_id} -X- {channel_id} (domain: {domain_id})")
            await self.route_map.remove_client_channel_connection(client_id, channel_id, domain_id)
            
        except Exception as e:
            logger.error(f"Error processing channel disconnect: {e}")
    
    async def start(self):
        """Start the event subscriber with robust error handling"""
        self.running = True
        
        # Initial connection with retry logic
        current_delay = self.reconnect_delay
        while self.running:
            try:
                await self.connect()
                await self.ensure_stream_exists()
                break  # Successfully connected
            except Exception as e:
                logger.error(f"Initial connection failed: {e}")
                if self.running:
                    logger.info(f"Retrying initial connection in {current_delay} seconds...")
                    await asyncio.sleep(current_delay)
                    current_delay = min(current_delay * 2, self.max_reconnect_delay)
                else:
                    return
        
        # Start both subscriptions concurrently
        tasks = [
            asyncio.create_task(self.subscribe_to_client_events()),
            asyncio.create_task(self.subscribe_to_channel_events())
        ]
        
        logger.info("SuperMQ event subscriber started")
        
        try:
            await asyncio.gather(*tasks, return_exceptions=True)
        except Exception as e:
            logger.error(f"Event subscriber error: {e}")
        finally:
            await self.stop()
    
    async def stop(self):
        """Stop the event subscriber"""
        logger.info("Stopping SuperMQ event subscriber...")
        self.running = False
        
        if self.nc and not self.nc.is_closed:
            try:
                await self.nc.close()
                logger.info("NATS connection closed")
            except Exception as e:
                logger.error(f"Error closing NATS connection: {e}")
        
        self.nc = None
        self.js = None