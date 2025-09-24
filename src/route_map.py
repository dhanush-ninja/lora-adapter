"""
Redis Route Map Manager for LoRa Adapter

Manages the mapping between LoRa identifiers and SuperMQ entities:
- dev_eui -> client_id
- app_id -> channel_id (with domain_id)

Based on the Go implementation in supermq-contrib/lora/routemap.go
"""
import asyncio
import logging
from typing import Optional, Dict, Any
import redis.asyncio as redis
import json
from config import config

logger = logging.getLogger(__name__)


class RouteMapManager:
    """Manages LoRa device and application route mappings in Redis"""
    
    # Redis key patterns (matching Go implementation)
    CLIENT_ROUTES_KEY = "lora:dev_eui:routes"  # Hash: dev_eui -> client_id
    CHANNEL_ROUTES_KEY = "lora:app_id:routes"  # Hash: app_id -> {"channel_id": ..., "domain_id": ...}
    CLIENT_CHANNEL_CONNECTIONS_KEY = "lora:client_channel:connections"  # Hash: client_id -> {"channel_id": ..., "domain_id": ...}
    
    def __init__(self, redis_url: str = "redis://localhost:6379"):
        self.redis_url = redis_url
        self.redis_client: Optional[redis.Redis] = None
    
    async def connect(self):
        """Connect to Redis with retry logic"""
        max_retries = 5
        retry_delay = 2
        
        for attempt in range(max_retries):
            try:
                # Configure connection with timeouts and health checks
                self.redis_client = redis.from_url(
                    self.redis_url,
                    socket_timeout=getattr(config, 'redis_socket_timeout', 5),
                    socket_connect_timeout=getattr(config, 'redis_connect_timeout', 5),
                    health_check_interval=getattr(config, 'redis_health_check_interval', 30),
                )
                # Test connection
                await self.redis_client.ping()
                logger.info(f"Connected to Redis at {self.redis_url}")
                return
            except Exception as e:
                logger.warning(f"Redis connection attempt {attempt + 1}/{max_retries} failed: {e}")
                if attempt < max_retries - 1:
                    logger.info(f"Retrying Redis connection in {retry_delay} seconds...")
                    await asyncio.sleep(retry_delay)
                    retry_delay *= 2  # Exponential backoff
                else:
                    logger.error(f"Failed to connect to Redis after {max_retries} attempts")
                    raise
    
    async def disconnect(self):
        """Disconnect from Redis"""
        if self.redis_client:
            await self.redis_client.close()
            logger.info("Disconnected from Redis")
    
    # ============ CLIENT ROUTE OPERATIONS ============
    
    async def set_client_route(self, dev_eui: str, client_id: str):
        """
        Set route mapping for a LoRa device
        Maps dev_eui -> client_id
        """
        try:
            await self.redis_client.hset(
                self.CLIENT_ROUTES_KEY,
                dev_eui,
                client_id
            )
            logger.info(f"Set client route: {dev_eui} -> {client_id}")
        except Exception as e:
            logger.error(f"Failed to set client route for {dev_eui}: {e}")
            raise
    
    async def get_client_route(self, dev_eui: str) -> Optional[str]:
        """
        Get client ID for a device EUI
        Returns client_id or None if not found
        """
        try:
            client_id = await self.redis_client.hget(
                self.CLIENT_ROUTES_KEY,
                dev_eui
            )
            if client_id:
                client_id = client_id.decode('utf-8')
                logger.debug(f"Found client route: {dev_eui} -> {client_id}")
                return client_id
            else:
                logger.warning(f"No client route found for dev_eui: {dev_eui}")
                return None
        except Exception as e:
            logger.error(f"Failed to get client route for {dev_eui}: {e}")
            return None
    
    async def remove_client_route(self, dev_eui: str):
        """Remove client route mapping"""
        try:
            result = await self.redis_client.hdel(self.CLIENT_ROUTES_KEY, dev_eui)
            if result:
                logger.info(f"Removed client route for dev_eui: {dev_eui}")
            else:
                logger.warning(f"No client route found to remove for dev_eui: {dev_eui}")
        except Exception as e:
            logger.error(f"Failed to remove client route for {dev_eui}: {e}")
    
    async def remove_client_by_id(self, client_id: str):
        """Remove client route by client ID (reverse lookup) using HSCAN"""
        try:
            cursor: int = 0
            dev_euis_to_remove = []
            while True:
                cursor, items = await self.redis_client.hscan(self.CLIENT_ROUTES_KEY, cursor=cursor, count=500)
                for dev_eui_bytes, stored_client_id_bytes in items.items():
                    if stored_client_id_bytes.decode('utf-8') == client_id:
                        dev_euis_to_remove.append(dev_eui_bytes.decode('utf-8'))
                if cursor == 0:
                    break
            
            if dev_euis_to_remove:
                # Use pipeline for batch delete
                pipe = self.redis_client.pipeline()
                for dev_eui in dev_euis_to_remove:
                    pipe.hdel(self.CLIENT_ROUTES_KEY, dev_eui)
                await pipe.execute()
                logger.info(f"Removed client routes for client_id {client_id}: {len(dev_euis_to_remove)} items")
            else:
                logger.debug(f"No client routes found to remove for client_id: {client_id}")
        except Exception as e:
            logger.error(f"Failed to remove client routes for client_id {client_id}: {e}")
    
    
    async def set_channel_route(self, app_id: str, channel_id: str, domain_id: str):
        """
        Set route mapping for a LoRa application
        Maps app_id -> {"channel_id": ..., "domain_id": ...}
        """
        try:
            route_data = {
                "channel_id": channel_id,
                "domain_id": domain_id
            }
            
            await self.redis_client.hset(
                self.CHANNEL_ROUTES_KEY,
                app_id,
                json.dumps(route_data)
            )
            logger.info(f"Set channel route: {app_id} -> {channel_id} (domain: {domain_id})")
        except Exception as e:
            logger.error(f"Failed to set channel route for {app_id}: {e}")
            raise
    
    async def get_channel_route(self, app_id: str) -> Optional[Dict[str, str]]:
        """
        Get channel and domain ID for an application ID
        Returns {"channel_id": ..., "domain_id": ...} or None if not found
        """
        try:
            route_data = await self.redis_client.hget(
                self.CHANNEL_ROUTES_KEY,
                app_id
            )
            if route_data:
                route_info = json.loads(route_data.decode('utf-8'))
                logger.debug(f"Found channel route: {app_id} -> {route_info}")
                return route_info
            else:
                logger.warning(f"No channel route found for app_id: {app_id}")
                return None
        except Exception as e:
            logger.error(f"Failed to get channel route for {app_id}: {e}")
            return None
    
    async def remove_channel_route(self, app_id: str):
        """Remove channel route mapping"""
        try:
            result = await self.redis_client.hdel(self.CHANNEL_ROUTES_KEY, app_id)
            if result:
                logger.info(f"Removed channel route for app_id: {app_id}")
            else:
                logger.warning(f"No channel route found to remove for app_id: {app_id}")
        except Exception as e:
            logger.error(f"Failed to remove channel route for {app_id}: {e}")
    
    async def remove_channel_by_id(self, channel_id: str, domain_id: str):
        """Remove channel route by channel ID and domain ID (reverse lookup) using HSCAN"""
        try:
            cursor: int = 0
            app_ids_to_remove = []
            while True:
                cursor, items = await self.redis_client.hscan(self.CHANNEL_ROUTES_KEY, cursor=cursor, count=500)
                for app_id_bytes, route_data_bytes in items.items():
                    try:
                        route_info = json.loads(route_data_bytes.decode('utf-8'))
                        if route_info.get("channel_id") == channel_id and route_info.get("domain_id") == domain_id:
                            app_ids_to_remove.append(app_id_bytes.decode('utf-8'))
                    except json.JSONDecodeError:
                        continue
                if cursor == 0:
                    break
            
            if app_ids_to_remove:
                pipe = self.redis_client.pipeline()
                for app_id in app_ids_to_remove:
                    pipe.hdel(self.CHANNEL_ROUTES_KEY, app_id)
                await pipe.execute()
                logger.info(f"Removed channel routes for channel_id {channel_id} (domain {domain_id}): {len(app_ids_to_remove)} items")
            else:
                logger.debug(f"No channel routes found to remove for channel_id: {channel_id} (domain: {domain_id})")
        except Exception as e:
            logger.error(f"Failed to remove channel routes for channel_id {channel_id}: {e}")
    
    
    async def set_client_channel_connection(self, client_id: str, channel_id: str, domain_id: str):
        """Set client-channel connection mapping"""
        try:
            connection_data = {
                "channel_id": channel_id,
                "domain_id": domain_id
            }
            
            await self.redis_client.hset(
                self.CLIENT_CHANNEL_CONNECTIONS_KEY,
                client_id,
                json.dumps(connection_data)
            )
            logger.info(f"Set client-channel connection: {client_id} -> {channel_id} (domain: {domain_id})")
        except Exception as e:
            logger.error(f"Failed to set client-channel connection for {client_id}: {e}")
            raise
    
    async def get_client_channel_connection(self, client_id: str) -> Optional[Dict[str, str]]:
        """Get channel connection for a client"""
        try:
            connection_data = await self.redis_client.hget(
                self.CLIENT_CHANNEL_CONNECTIONS_KEY,
                client_id
            )
            if connection_data:
                connection_info = json.loads(connection_data.decode('utf-8'))
                logger.debug(f"Found client-channel connection: {client_id} -> {connection_info}")
                return connection_info
            else:
                logger.debug(f"No client-channel connection found for client_id: {client_id}")
                return None
        except Exception as e:
            logger.error(f"Failed to get client-channel connection for {client_id}: {e}")
            return None
    
    async def remove_client_channel_connection(self, client_id: str, channel_id: str, domain_id: str):
        """Remove client-channel connection mapping"""
        try:
            result = await self.redis_client.hdel(self.CLIENT_CHANNEL_CONNECTIONS_KEY, client_id)
            if result:
                logger.info(f"Removed client-channel connection for client_id: {client_id}")
            else:
                logger.warning(f"No client-channel connection found to remove for client_id: {client_id}")
        except Exception as e:
            logger.error(f"Failed to remove client-channel connection for {client_id}: {e}")
    
    async def get_all_client_routes(self) -> Dict[str, str]:
        """Get all client routes"""
        try:
            routes = await self.redis_client.hgetall(self.CLIENT_ROUTES_KEY)
            return {
                dev_eui.decode('utf-8'): client_id.decode('utf-8')
                for dev_eui, client_id in routes.items()
            }
        except Exception as e:
            logger.error(f"Failed to get all client routes: {e}")
            return {}
    
    async def get_all_channel_routes(self) -> Dict[str, Dict[str, str]]:
        """Get all channel routes"""
        try:
            routes = await self.redis_client.hgetall(self.CHANNEL_ROUTES_KEY)
            result = {}
            for app_id, route_data in routes.items():
                try:
                    route_info = json.loads(route_data.decode('utf-8'))
                    result[app_id.decode('utf-8')] = route_info
                except json.JSONDecodeError:
                    continue
            return result
        except Exception as e:
            logger.error(f"Failed to get all channel routes: {e}")
            return {}
    
    async def clear_all_routes(self):
        """Clear all route mappings (for testing)"""
        try:
            await self.redis_client.delete(self.CLIENT_ROUTES_KEY)
            await self.redis_client.delete(self.CHANNEL_ROUTES_KEY)
            await self.redis_client.delete(self.CLIENT_CHANNEL_CONNECTIONS_KEY)
            logger.info("Cleared all route mappings")
        except Exception as e:
            logger.error(f"Failed to clear all routes: {e}")
    
    async def health_check(self) -> bool:
        """Check if Redis connection is healthy"""
        try:
            await self.redis_client.ping()
            return True
        except Exception:
            return False