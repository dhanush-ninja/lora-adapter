import asyncio
import json
import logging
import sys
from typing import Dict, Any, Optional
import nats

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ProtobufDecoder:
    
    @staticmethod
    def decode_varint(data: bytes, offset: int) -> tuple[int, int]:
        result = 0
        shift = 0
        pos = offset
        
        while pos < len(data):
            byte = data[pos]
            result |= (byte & 0x7F) << shift
            pos += 1
            if (byte & 0x80) == 0:
                break
            shift += 7
            
        return result, pos
    
    @staticmethod
    def decode_message(data: bytes) -> Dict[str, Any]:
        fields = {}
        offset = 0
        
        while offset < len(data):
            tag_byte = data[offset]
            field_number = tag_byte >> 3
            wire_type = tag_byte & 0x07
            offset += 1
            
            if wire_type == 0:
                value, offset = ProtobufDecoder.decode_varint(data, offset)
                if field_number == 7:
                    fields['created'] = value
                    
            elif wire_type == 2:
                length, offset = ProtobufDecoder.decode_varint(data, offset)
                value_bytes = data[offset:offset + length]
                offset += length
                
                if field_number == 1: 
                    fields['channel'] = value_bytes.decode('utf-8')
                elif field_number == 2:
                    fields['domain'] = value_bytes.decode('utf-8')
                elif field_number == 3:
                    fields['subtopic'] = value_bytes.decode('utf-8')
                elif field_number == 4:
                    fields['publisher'] = value_bytes.decode('utf-8')
                elif field_number == 5: 
                    fields['protocol'] = value_bytes.decode('utf-8')
                elif field_number == 6:
                    try:
                        payload_str = value_bytes.decode('utf-8')
                        fields['payload'] = json.loads(payload_str)
                    except (UnicodeDecodeError, json.JSONDecodeError):
                        fields['payload'] = value_bytes.hex()
                        
        return fields


class NATSMessageViewer:
    
    def __init__(self, nats_url: str, user: Optional[str] = None, password: Optional[str] = None):
        self.nats_url = nats_url
        self.user = user
        self.password = password
        self.nc: Optional[nats.NATS] = None
        self.running = False
    
    async def connect(self):
        connect_opts = {
            "connect_timeout": 10,
            "max_reconnect_attempts": 3,
            "reconnect_time_wait": 2,
        }
        
        if self.user and self.password:
            connect_opts["user"] = self.user
            connect_opts["password"] = self.password
            logger.info(f"Connecting to NATS with authentication: {self.user}")
        else:
            logger.info("Connecting to NATS without authentication")
        
        try:
            self.nc = await nats.connect(self.nats_url, **connect_opts)
            logger.info(f"✅ Connected to NATS at {self.nats_url}")
        except Exception as e:
            logger.error(f"❌ Failed to connect to NATS: {e}")
            raise
    
    async def subscribe_to_messages(self, subject_pattern: str = "m.>"):
        """Subscribe to SuperMQ messages"""
        logger.info(f"🔍 Subscribing to: {subject_pattern}")
        
        async def message_handler(msg):
            await self.handle_message(msg)
        
        await self.nc.subscribe(subject_pattern, cb=message_handler)
        logger.info("✅ Subscription active")
        
        self.running = True
        try:
            while self.running:
                await asyncio.sleep(0.1)
        except KeyboardInterrupt:
            logger.info("🛑 Stopping subscriber...")
            self.running = False
    
    async def handle_message(self, msg):
        try:
            logger.info(f"\n📨 Received message on subject: {msg.subject}")
            logger.info(f"📏 Message size: {len(msg.data)} bytes")
            
            decoded = ProtobufDecoder.decode_message(msg.data)
            
            # Pretty print the decoded message
            print("=" * 80)
            print(f"🎯 SUBJECT: {msg.subject}")
            print(f"📅 TIMESTAMP: {decoded.get('created', 'N/A')}")
            print(f"🏷️  CHANNEL: {decoded.get('channel', 'N/A')}")
            print(f"🌐 DOMAIN: {decoded.get('domain', 'N/A')}")
            print(f"📡 PUBLISHER: {decoded.get('publisher', 'N/A')}")
            print(f"🔌 PROTOCOL: {decoded.get('protocol', 'N/A')}")
            print(f"📂 SUBTOPIC: {decoded.get('subtopic', 'N/A')}")
            print()
            
            if 'payload' in decoded:
                payload = decoded['payload']
                if isinstance(payload, dict):
                    print("📦 PAYLOAD (JSON):")
                    print(json.dumps(payload, indent=2, sort_keys=True))
                    
                    if 'device_name' in payload:
                        print(f"\n🏷️  Device Name: {payload['device_name']}")
                    if 'tags' in payload:
                        print(f"🏷️  Tags: {json.dumps(payload['tags'], indent=2)}")
                    if 'dev_eui' in payload:
                        print(f"🔢 Device EUI: {payload['dev_eui']}")
                    
                    sensor_fields = {k: v for k, v in payload.items() 
                                   if k not in ['device_name', 'tags', 'dev_eui', 'app_id', 'app_name', 'f_port', 'f_cnt', 'tenant_name', 'device_profile_name']}
                    if sensor_fields:
                        print(f"\n📊 Sensor Data:")
                        for key, value in sensor_fields.items():
                            print(f"   {key}: {value}")
                else:
                    print(f"📦 PAYLOAD (Raw): {payload}")
            else:
                print("📦 PAYLOAD: None")
            
            print("=" * 80)
            
        except Exception as e:
            logger.error(f"❌ Error handling message: {e}")
            logger.error(f"Raw data: {msg.data[:100].hex()}")
    
    async def disconnect(self):
        if self.nc:
            await self.nc.close()
            logger.info("🔌 Disconnected from NATS")


async def main():
    NATS_URL = "nats://10.10.0.104:4222"
    NATS_USER = "supermq"
    NATS_PASSWORD = "supermq"
    SUBJECT_PATTERN = "m.>"
    
    print("🚀 NATS Message Viewer for LoRa Adapter")
    print(f"📡 NATS URL: {NATS_URL}")
    print(f"👤 User: {NATS_USER}")
    print(f"🎯 Subject Pattern: {SUBJECT_PATTERN}")
    print("=" * 80)
    
    viewer = NATSMessageViewer(NATS_URL, NATS_USER, NATS_PASSWORD)
    
    try:
        await viewer.connect()
        await viewer.subscribe_to_messages(SUBJECT_PATTERN)
    except KeyboardInterrupt:
        logger.info("🛑 Interrupted by user")
    except Exception as e:
        logger.error(f"❌ Error: {e}")
    finally:
        await viewer.disconnect()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 Goodbye!")