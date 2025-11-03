"""
Message Forwarder for LoRa Adapter

Forwards messages from ChirpStack MQTT to SuperMQ NATS in protobuf format.
Based on the protobuf_binary_publisher.py implementation.
"""
import asyncio
import json
import time
import logging
from typing import Dict, Any, Optional
import nats

logger = logging.getLogger(__name__)


class MessageForwarder:
    """Forwards messages from ChirpStack to SuperMQ in protobuf format"""
    
    def __init__(self, nats_url: str, user: str, password: str,):
        self.nats_url = nats_url
        self.user = user
        self.password = password
        self.nc: Optional[nats.NATS] = None
        self.reconnect_delay = 1  # seconds
        self.max_reconnect_delay = 30  # seconds
    
    async def connect(self):
        """Connect to NATS with reconnection support"""
        try:
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
            
            # Add connection event handlers
            async def disconnected_cb():
                logger.warning("Message forwarder NATS connection lost!")
            
            async def reconnected_cb():
                logger.info("Message forwarder NATS connection restored!")
            
            async def error_cb(e):
                logger.error(f"Message forwarder NATS connection error: {e}")
            
            async def closed_cb():
                logger.warning("Message forwarder NATS connection closed")
            
            connect_opts["disconnected_cb"] = disconnected_cb
            connect_opts["reconnected_cb"] = reconnected_cb
            connect_opts["error_cb"] = error_cb
            connect_opts["closed_cb"] = closed_cb
            
            self.nc = await nats.connect(self.nats_url, **connect_opts)
            logger.info(f"Connected to SuperMQ NATS at {self.nats_url}")
            
        except Exception as e:
            logger.error(f"Failed to connect to NATS: {e}")
            raise
    
    async def disconnect(self):
        """Disconnect from NATS"""
        if self.nc and not self.nc.is_closed:
            try:
                await self.nc.close()
                logger.info("Disconnected from NATS")
            except Exception as e:
                logger.error(f"Error disconnecting from NATS: {e}")
        
        self.nc = None
    
    def is_connected(self) -> bool:
        """Check if NATS connection is active"""
        return self.nc is not None and not self.nc.is_closed
    
    async def ensure_connected(self):
        """Ensure NATS connection is active, reconnect if needed"""
        if not self.nc or self.nc.is_closed:
            logger.info("NATS connection lost, attempting to reconnect...")
            current_delay = self.reconnect_delay
            
            while True:
                try:
                    await self.connect()
                    return
                except Exception as e:
                    logger.error(f"Reconnection failed: {e}")
                    logger.info(f"Retrying in {current_delay} seconds...")
                    await asyncio.sleep(current_delay)
                    current_delay = min(current_delay * 2, self.max_reconnect_delay)
    
    async def forward_message(
        self,
        client_id: str,
        channel_id: str,
        domain_id: str,
        payload: Dict[str, Any],
        protocol: str = "lora"
    ):
        """Forward message to SuperMQ in protobuf format"""
        try:
            # Ensure we have a valid connection
            await self.ensure_connected()
            
            # Convert payload to JSON bytes
            payload_json = json.dumps(payload)
            payload_bytes = payload_json.encode('utf-8')
            
            # Create protobuf binary message
            protobuf_data = self.encode_protobuf_message(
                channel=channel_id,
                domain=domain_id,
                subtopic="",
                publisher=client_id,
                protocol=protocol,
                payload_bytes=payload_bytes,
                created=time.time_ns()
            )
            
            # Publish to NATS
            subject = f"m.{domain_id}.{channel_id}"
            
            logger.info(f"Publishing to SuperMQ: {subject}")
            logger.debug(f"Payload: {payload_json}")
            logger.debug(f"Protobuf size: {len(protobuf_data)} bytes")
            
            await self.nc.publish(subject, protobuf_data)
            
            logger.info("✅ Message forwarded to SuperMQ successfully")
            
        except Exception as e:
            logger.error(f"Failed to forward message to SuperMQ: {e}")
            # Don't re-raise the exception to prevent blocking the MQTT subscriber
            # The message will be lost, but the adapter will continue running
    
    def encode_protobuf_message(
        self,
        channel: str,
        domain: str,
        subtopic: str,
        publisher: str,
        protocol: str,
        payload_bytes: bytes,
        created: int
    ) -> bytes:
        """
        Encode SuperMQ message in protobuf binary format
        
        Protobuf wire format:
        - Field 1 (channel): tag=0x0A, length-delimited string
        - Field 2 (domain): tag=0x12, length-delimited string  
        - Field 3 (subtopic): tag=0x1A, length-delimited string
        - Field 4 (publisher): tag=0x22, length-delimited string
        - Field 5 (protocol): tag=0x2A, length-delimited string
        - Field 6 (payload): tag=0x32, length-delimited bytes
        - Field 7 (created): tag=0x38, varint
        """
        
        def encode_string_field(field_number: int, value: str) -> bytes:
            """Encode a string field in protobuf format"""
            if not value:
                return b''
            
            tag = (field_number << 3) | 2  # Wire type 2 (length-delimited)
            data = value.encode('utf-8')
            length = len(data)
            
            # Encode varint length
            length_bytes = self.encode_varint(length)
            
            return bytes([tag]) + length_bytes + data
        
        def encode_bytes_field(field_number: int, value: bytes) -> bytes:
            """Encode a bytes field in protobuf format"""
            if not value:
                return b''
                
            tag = (field_number << 3) | 2  # Wire type 2 (length-delimited)
            length = len(value)
            
            # Encode varint length
            length_bytes = self.encode_varint(length)
            
            return bytes([tag]) + length_bytes + value
        
        def encode_varint_field(field_number: int, value: int) -> bytes:
            """Encode a varint field in protobuf format"""
            if value == 0:
                return b''
                
            tag = (field_number << 3) | 0  # Wire type 0 (varint)
            varint_bytes = self.encode_varint(value)
            
            return bytes([tag]) + varint_bytes
        
        # Encode all fields
        message_bytes = b''
        message_bytes += encode_string_field(1, channel)      # Field 1: channel
        message_bytes += encode_string_field(2, domain)       # Field 2: domain
        message_bytes += encode_string_field(3, subtopic)     # Field 3: subtopic
        message_bytes += encode_string_field(4, publisher)    # Field 4: publisher
        message_bytes += encode_string_field(5, protocol)     # Field 5: protocol
        message_bytes += encode_bytes_field(6, payload_bytes) # Field 6: payload
        message_bytes += encode_varint_field(7, created)      # Field 7: created
        
        return message_bytes
    
    def encode_varint(self, value: int) -> bytes:
        """Encode integer as protobuf varint"""
        result = b''
        while value >= 0x80:
            result += bytes([value & 0x7F | 0x80])
            value >>= 7
        result += bytes([value & 0x7F])
        return result