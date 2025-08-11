"""
RabbitMQ Client module for IAQ Sensor Agent

This module provides functionality to connect to RabbitMQ server and publish
sensor data messages with automatic reconnection and error handling.
"""
import pika
import json
import time
import logging
from typing import Dict, Any, Optional, Callable
from datetime import datetime
import threading
from dataclasses import dataclass


logger = logging.getLogger(__name__)


@dataclass
class RabbitMQConfig:
    """Configuration for RabbitMQ connection"""
    host: str = 'localhost'
    port: int = 5672
    username: str = 'guest'
    password: str = 'guest'
    queue: str = 'sensor_data'
    exchange: str = ''
    routing_key: str = ''
    durable: bool = True
    connection_timeout: int = 30
    heartbeat: int = 600
    retry_delay: int = 5
    max_retries: int = 3


class RabbitMQClientError(Exception):
    """Custom exception for RabbitMQ client errors"""
    pass


class RabbitMQClient:
    """
    RabbitMQ Client for publishing sensor data messages
    
    Features:
    - Automatic connection management
    - Reconnection with exponential backoff
    - Message publishing with confirmation
    - Connection health monitoring
    - Thread-safe operations
    """
    
    def __init__(self, config: RabbitMQConfig):
        """
        Initialize RabbitMQ Client
        
        Args:
            config: RabbitMQ configuration object
        """
        self.config = config
        self.connection: Optional[pika.BlockingConnection] = None
        self.channel: Optional[pika.channel.Channel] = None
        self.is_connected = False
        self.is_connecting = False
        self._lock = threading.Lock()
        self._connection_attempts = 0
        self._last_connection_attempt = 0
        
        # Setup routing key if not provided
        if not self.config.routing_key:
            self.config.routing_key = self.config.queue
    
    def connect(self) -> bool:
        """
        Establish connection to RabbitMQ server
        
        Returns:
            True if connection successful, False otherwise
        """
        with self._lock:
            if self.is_connected:
                return True
            
            if self.is_connecting:
                return False
            
            self.is_connecting = True
            
            try:
                logger.info(f"Connecting to RabbitMQ at {self.config.host}:{self.config.port}")
                
                # Create connection parameters
                credentials = pika.PlainCredentials(
                    self.config.username, 
                    self.config.password
                )
                
                parameters = pika.ConnectionParameters(
                    host=self.config.host,
                    port=self.config.port,
                    credentials=credentials,
                    connection_attempts=1,
                    retry_delay=1,
                    socket_timeout=self.config.connection_timeout,
                    heartbeat=self.config.heartbeat,
                    blocked_connection_timeout=300
                )
                
                # Establish connection
                self.connection = pika.BlockingConnection(parameters)
                self.channel = self.connection.channel()
                
                # Setup queue
                self._setup_queue()
                
                # Enable delivery confirmations
                self.channel.confirm_delivery()
                
                self.is_connected = True
                self._connection_attempts = 0
                logger.info("Successfully connected to RabbitMQ")
                
                return True
                
            except Exception as e:
                logger.error(f"Failed to connect to RabbitMQ: {e}")
                self._cleanup_connection()
                return False
            
            finally:
                self.is_connecting = False
    
    def disconnect(self):
        """Disconnect from RabbitMQ server"""
        with self._lock:
            if self.is_connected:
                logger.info("Disconnecting from RabbitMQ")
                self._cleanup_connection()
                logger.info("Disconnected from RabbitMQ")
    
    def _setup_queue(self):
        """Setup queue with proper configuration"""
        if not self.channel:
            raise RabbitMQClientError("Channel not available")
        
        # Declare exchange if specified
        if self.config.exchange:
            self.channel.exchange_declare(
                exchange=self.config.exchange,
                exchange_type='direct',
                durable=self.config.durable
            )
        
        # Declare queue
        self.channel.queue_declare(
            queue=self.config.queue,
            durable=self.config.durable
        )
        
        # Bind queue to exchange if specified
        if self.config.exchange:
            self.channel.queue_bind(
                exchange=self.config.exchange,
                queue=self.config.queue,
                routing_key=self.config.routing_key
            )
        
        logger.info(f"Queue '{self.config.queue}' declared successfully")
    
    def _cleanup_connection(self):
        """Clean up connection and channel"""
        try:
            if self.channel and not self.channel.is_closed:
                self.channel.close()
        except Exception as e:
            logger.warning(f"Error closing channel: {e}")
        
        try:
            if self.connection and not self.connection.is_closed:
                self.connection.close()
        except Exception as e:
            logger.warning(f"Error closing connection: {e}")
        
        self.channel = None
        self.connection = None
        self.is_connected = False
    
    def _reconnect(self) -> bool:
        """
        Attempt to reconnect with exponential backoff
        
        Returns:
            True if reconnection successful, False otherwise
        """
        current_time = time.time()
        
        # Check if we should attempt reconnection
        if (current_time - self._last_connection_attempt) < self.config.retry_delay:
            return False
        
        self._last_connection_attempt = current_time
        self._connection_attempts += 1
        
        if self._connection_attempts > self.config.max_retries:
            logger.error(f"Max reconnection attempts ({self.config.max_retries}) exceeded")
            return False
        
        # Calculate backoff delay
        backoff_delay = min(self.config.retry_delay * (2 ** (self._connection_attempts - 1)), 60)
        
        logger.info(f"Attempting reconnection #{self._connection_attempts} after {backoff_delay}s delay")
        time.sleep(backoff_delay)
        
        # Clean up existing connection
        self._cleanup_connection()
        
        # Attempt reconnection
        return self.connect()
    
    def is_connection_healthy(self) -> bool:
        """
        Check if connection is healthy
        
        Returns:
            True if connection is healthy, False otherwise
        """
        try:
            if not self.is_connected or not self.connection or not self.channel:
                return False
            
            if self.connection.is_closed or self.channel.is_closed:
                return False
            
            # Try to declare a temporary queue to test connection
            self.channel.queue_declare(queue='', exclusive=True, auto_delete=True)
            return True
            
        except Exception as e:
            logger.warning(f"Connection health check failed: {e}")
            return False
    
    def send_message(self, message: Dict[str, Any], retry_on_failure: bool = True) -> bool:
        """
        Send message to RabbitMQ queue
        
        Args:
            message: Dictionary containing message data
            retry_on_failure: Whether to retry on connection failure
            
        Returns:
            True if message sent successfully, False otherwise
        """
        if not isinstance(message, dict):
            raise RabbitMQClientError("Message must be a dictionary")
        
        # Ensure connection is established
        if not self.is_connected or not self.is_connection_healthy():
            if retry_on_failure:
                logger.info("Connection not healthy, attempting to reconnect")
                if not self._reconnect():
                    logger.error("Failed to reconnect to RabbitMQ")
                    return False
            else:
                logger.error("Connection not available and retry disabled")
                return False
        
        try:
            # Add metadata to message
            enriched_message = {
                **message,
                'sent_at': datetime.utcnow().isoformat() + 'Z',
                'agent_id': 'iaq_sensor_agent'
            }
            
            # Convert message to JSON
            message_body = json.dumps(enriched_message, ensure_ascii=False)
            
            # Publish message
            # Note: basic_publish returns None on success, raises exception on failure
            self.channel.basic_publish(
                exchange=self.config.exchange,
                routing_key=self.config.routing_key,
                body=message_body,
                properties=pika.BasicProperties(
                    delivery_mode=2 if self.config.durable else 1,  # Make message persistent
                    content_type='application/json',
                    timestamp=int(time.time())
                ),
                mandatory=True
            )
            
            # If we reach here without exception, the message was delivered successfully
            logger.debug(f"Message sent successfully to queue '{self.config.queue}'")
            return True
                
        except pika.exceptions.UnroutableError:
            logger.error(f"Message could not be routed to queue '{self.config.queue}'")
            return False
        except pika.exceptions.NackError:
            logger.error("Message was rejected by the server")
            return False
        except Exception as e:
            logger.error(f"Error sending message: {e}")
            
            # Mark connection as unhealthy
            self.is_connected = False
            
            # Retry if enabled
            if retry_on_failure and self._reconnect():
                logger.info("Retrying message send after reconnection")
                return self.send_message(message, retry_on_failure=False)
            
            return False
    
    def send_sensor_reading(self, sensor_reading) -> bool:
        """
        Send sensor reading message
        
        Args:
            sensor_reading: SensorReading object
            
        Returns:
            True if message sent successfully, False otherwise
        """
        try:
            # Convert sensor reading to JSON message format
            message = sensor_reading.to_json_message()
            return self.send_message(message)
        except Exception as e:
            logger.error(f"Error sending sensor reading: {e}")
            return False
    
    def get_connection_info(self) -> Dict[str, Any]:
        """
        Get connection information
        
        Returns:
            Dictionary containing connection information
        """
        return {
            'host': self.config.host,
            'port': self.config.port,
            'queue': self.config.queue,
            'exchange': self.config.exchange,
            'routing_key': self.config.routing_key,
            'is_connected': self.is_connected,
            'connection_attempts': self._connection_attempts,
            'last_connection_attempt': self._last_connection_attempt
        }
    
    def __enter__(self):
        """Context manager entry"""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.disconnect()


def create_rabbitmq_client(
    host: str = 'localhost',
    port: int = 5672,
    username: str = 'guest',
    password: str = 'guest',
    queue: str = 'sensor_data',
    **kwargs
) -> RabbitMQClient:
    """
    Factory function to create RabbitMQ client
    
    Args:
        host: RabbitMQ server host
        port: RabbitMQ server port
        username: Username for authentication
        password: Password for authentication
        queue: Queue name
        **kwargs: Additional configuration parameters
        
    Returns:
        RabbitMQClient instance
    """
    config = RabbitMQConfig(
        host=host,
        port=port,
        username=username,
        password=password,
        queue=queue,
        **kwargs
    )
    
    return RabbitMQClient(config)


def create_rabbitmq_client_from_env() -> RabbitMQClient:
    """
    Create RabbitMQ client from environment variables
    
    Expected environment variables:
    - RABBITMQ_HOST
    - RABBITMQ_PORT
    - RABBITMQ_USERNAME
    - RABBITMQ_PASSWORD
    - RABBITMQ_QUEUE
    - RABBITMQ_EXCHANGE (optional)
    
    Returns:
        RabbitMQClient instance
    """
    import os
    
    config = RabbitMQConfig(
        host=os.getenv('RABBITMQ_HOST', 'localhost'),
        port=int(os.getenv('RABBITMQ_PORT', '5672')),
        username=os.getenv('RABBITMQ_USERNAME', 'guest'),
        password=os.getenv('RABBITMQ_PASSWORD', 'guest'),
        queue=os.getenv('RABBITMQ_QUEUE', 'sensor_data'),
        exchange=os.getenv('RABBITMQ_EXCHANGE', ''),
        routing_key=os.getenv('RABBITMQ_ROUTING_KEY', ''),
        durable=os.getenv('RABBITMQ_DURABLE', 'true').lower() == 'true',
        connection_timeout=int(os.getenv('RABBITMQ_CONNECTION_TIMEOUT', '30')),
        heartbeat=int(os.getenv('RABBITMQ_HEARTBEAT', '600')),
        retry_delay=int(os.getenv('RABBITMQ_RETRY_DELAY', '5')),
        max_retries=int(os.getenv('RABBITMQ_MAX_RETRIES', '3'))
    )
    
    return RabbitMQClient(config)