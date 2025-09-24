"""
Main LoRa Adapter Application

Orchestrates the ChirpStack MQTT to SuperMQ NATS bridge with Redis route mapping.
"""
import asyncio
import logging
import signal
import sys
from typing import Optional

from config import config
from route_map import RouteMapManager
from event_subscriber import SuperMQEventSubscriber
from mqtt_subscriber import ChirpStackMQTTSubscriber
from message_forwarder import MessageForwarder


class LoRaAdapter:
    """Main LoRa Adapter application"""
    
    def __init__(self):
        self.setup_logging()
        self.logger = logging.getLogger(__name__)
        
        # Components
        self.route_map: Optional[RouteMapManager] = None
        self.event_subscriber: Optional[SuperMQEventSubscriber] = None
        self.mqtt_subscriber: Optional[ChirpStackMQTTSubscriber] = None
        self.message_forwarder: Optional[MessageForwarder] = None
        
        # Control
        self.running = False
        self.shutdown_event = asyncio.Event()
    
    def setup_logging(self):
        """Setup logging configuration"""
        logging.basicConfig(
            level=getattr(logging, config.log_level.upper()),
            format=config.log_format,
            handlers=[
                logging.StreamHandler(sys.stdout)
            ]
        )
        
        # Reduce noise from external libraries
        logging.getLogger("nats").setLevel(logging.WARNING)
        logging.getLogger("asyncio_mqtt").setLevel(logging.WARNING)
        logging.getLogger("redis").setLevel(logging.WARNING)
    
    def setup_signal_handlers(self):
        """Setup signal handlers for graceful shutdown"""
        def signal_handler(signum, frame):
            self.logger.info(f"Received signal {signum}, initiating shutdown...")
            self.shutdown_event.set()
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
    
    async def initialize_components(self):
        """Initialize all adapter components"""
        try:
            self.logger.info("Initializing LoRa Adapter components...")
            
            # Wait a bit for services to be ready
            self.logger.info("Waiting for services to be ready...")
            await asyncio.sleep(5)
            
            # Initialize route map manager
            self.route_map = RouteMapManager(config.redis_url)
            await self.route_map.connect()
            self.logger.info("✅ Route map manager initialized")
            
            # Initialize message forwarder
            self.message_forwarder = MessageForwarder(
                nats_url=config.supermq_nats_url,
                user=config.supermq_nats_user,
                password=config.supermq_nats_password,
            )
            await self.message_forwarder.connect()
            self.logger.info("✅ Message forwarder initialized")
            
            # Initialize SuperMQ event subscriber
            self.event_subscriber = SuperMQEventSubscriber(
                nats_url=config.supermq_nats_url,
                user=config.supermq_nats_user,
                password=config.supermq_nats_password,
                route_map=self.route_map
            )
            self.logger.info("✅ SuperMQ event subscriber initialized")
            
            # Initialize ChirpStack MQTT subscriber
            self.mqtt_subscriber = ChirpStackMQTTSubscriber(
                route_map=self.route_map,
                message_forwarder=self.message_forwarder
            )
            self.logger.info("✅ ChirpStack MQTT subscriber initialized")
            
            self.logger.info("🚀 All components initialized successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize components: {e}")
            raise
    
    
    async def run(self):
        """Run the LoRa Adapter"""
        try:
            self.running = True
            self.setup_signal_handlers()
            
            # Initialize components
            await self.initialize_components()
            
            # Start all services concurrently
            tasks = [
                asyncio.create_task(self.event_subscriber.start()),
                asyncio.create_task(self.mqtt_subscriber.start()),
            ]
            
            self.logger.info("🎯 LoRa Adapter started successfully")
            self.logger.info(f"📡 ChirpStack MQTT: {config.chirpstack_mqtt_host}:{config.chirpstack_mqtt_port}")
            self.logger.info(f"🚀 SuperMQ NATS: {config.supermq_nats_url}")
            self.logger.info(f"🗄️  Redis: {config.redis_url}")
            
            # Wait for shutdown signal
            await self.shutdown_event.wait()
            
            # Cancel all tasks
            self.logger.info("Cancelling all tasks...")
            for task in tasks:
                task.cancel()
            
            # Wait for tasks to complete
            await asyncio.gather(*tasks, return_exceptions=True)
            
        except Exception as e:
            self.logger.error(f"LoRa Adapter error: {e}")
            raise
        finally:
            await self.cleanup()
    
    async def cleanup(self):
        """Cleanup all resources"""
        self.running = False
        
        try:
            # Stop components
            if self.event_subscriber:
                await self.event_subscriber.stop()
            
            if self.mqtt_subscriber:
                await self.mqtt_subscriber.stop()
            
            if self.message_forwarder:
                await self.message_forwarder.disconnect()
            
            if self.route_map:
                await self.route_map.disconnect()
            
            self.logger.info("✅ LoRa Adapter stopped gracefully")
            
        except Exception as e:
            self.logger.error(f"Error during cleanup: {e}")


async def main():
    """Main entry point"""
    adapter = LoRaAdapter()
    await adapter.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\\nShutdown requested by user")
    except Exception as e:
        print(f"Fatal error: {e}")
        sys.exit(1)