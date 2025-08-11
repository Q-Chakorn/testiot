"""
RabbitMQ Consumer module for DataLogger Agent

This module provides functionality to consume messages from RabbitMQ queue
sent by the IAQ Sensor Agent and process them for database storage.
"""
import pika
import json
import logging
import time
import threading
from typing import Dict, Any, Callable, Optional
from dataclasses import dataclass
from datetime import datetime
import signal
import sys

logger = logging.getLogger(__name__)


@dataclass
class RabbitMQConfig:
    """Configuration for RabbitMQ connection"""
    host: str = 'localhost'
    port: int = 5672
    username: str = 'guest'
    password: str = 'guest'
    queue_name: str = 'iaq_data_queue'
    exchange_name: str = 'iot_data'
    routing_key: str = 'sensor.iaq'
    prefetch_count: int = 10
    auto_ack: bool = False
    durable_queue: bool = True


class RabbitMQConsumerError(Exception):
    """Custom exception for RabbitMQ consumer errors"""
    pass


class RabbitMQConsumer:
    """
    RabbitMQ Consumer for receiving sensor data messages
    
    Features:
    - Automatic reconnection on connection loss
    - Message acknowledgment handling
    - Graceful shutdown
    - Error handling and dead letter queue support
    - Performance monitoring
    """
    
    def __init__(self, config: RabbitMQConfig, message_handler: Callable[[Dict[str, Any]], bool]):
        """
        Initialize RabbitMQ Consumer
        
        Args:
            config: RabbitMQ configuration
            message_handler: Function to handle received messages
                           Should return True if message processed successfully
        """
        self.config = config
        self.message_handler = message_handler
        self.connection: Optional[pika.BlockingConnection] = None
        self.channel: Optional[pika.channel.Channel] = None
        self.is_consuming = False
        self.should_stop = False
        
        # Statistics
        self.messages_received = 0
        self.messages_processed = 0
        self.messages_failed = 0
        self.last_message_time: Optional[datetime] = None
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        logger.info(f"Received signal {signum}, initiating graceful shutdown...")
        self.stop_consuming()
    
    def _create_connection(self) -> bool:
        """
        Create connection to RabbitMQ server
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            # Connection parameters
            credentials = pika.PlainCredentials(self.config.username, self.config.password)
            parameters = pika.ConnectionParameters(
                host=self.config.host,
                port=self.config.port,
                credentials=credentials,
                heartbeat=600,  # 10 minutes heartbeat
                blocked_connection_timeout=300,  # 5 minutes timeout
            )
            
            logger.info(f"Connecting to RabbitMQ at {self.config.host}:{self.config.port}")
            self.connection = pika.BlockingConnection(parameters)
            self.channel = self.connection.channel()
            
            # Declare exchange (skip if using default exchange)
            if self.config.exchange_name and self.config.exchange_name != "":
                self.channel.exchange_declare(
                    exchange=self.config.exchange_name,
                    exchange_type='topic',
                    durable=True
                )
            
            # Declare queue
            self.channel.queue_declare(
                queue=self.config.queue_name,
                durable=self.config.durable_queue
            )
            
            # Bind queue to exchange (skip if using default exchange)
            if self.config.exchange_name and self.config.exchange_name != "":
                self.channel.queue_bind(
                    exchange=self.config.exchange_name,
                    queue=self.config.queue_name,
                    routing_key=self.config.routing_key
                )
            
            # Set QoS (prefetch count)
            self.channel.basic_qos(prefetch_count=self.config.prefetch_count)
            
            logger.info("RabbitMQ connection established successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to RabbitMQ: {e}")
            self._cleanup_connection()
            return False
    
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
    
    def _reconnect_with_backoff(self, max_retries: int = 5) -> bool:
        """
        Reconnect to RabbitMQ with exponential backoff
        
        Args:
            max_retries: Maximum number of retry attempts
            
        Returns:
            True if reconnection successful
        """
        for attempt in range(max_retries):
            if self.should_stop:
                return False
            
            wait_time = min(2 ** attempt, 60)  # Exponential backoff, max 60 seconds
            logger.info(f"Reconnection attempt {attempt + 1}/{max_retries} in {wait_time} seconds...")
            
            time.sleep(wait_time)
            
            if self._create_connection():
                return True
        
        logger.error(f"Failed to reconnect after {max_retries} attempts")
        return False
    
    def _process_message(self, channel, method, properties, body):
        """
        Process received message
        
        Args:
            channel: RabbitMQ channel
            method: Delivery method
            properties: Message properties
            body: Message body
        """
        self.messages_received += 1
        self.last_message_time = datetime.now()
        
        try:
            # Parse JSON message
            message_str = body.decode('utf-8')
            message_data = json.loads(message_str)
            
            logger.debug(f"Received message: {message_data}")
            
            # Validate message structure
            if not self._validate_message(message_data):
                logger.error(f"Invalid message format: {message_data}")
                self.messages_failed += 1
                
                if not self.config.auto_ack:
                    channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                return
            
            # Process message using handler
            success = self.message_handler(message_data)
            
            if success:
                self.messages_processed += 1
                logger.debug(f"Message processed successfully: {method.delivery_tag}")
                
                if not self.config.auto_ack:
                    channel.basic_ack(delivery_tag=method.delivery_tag)
            else:
                self.messages_failed += 1
                logger.error(f"Message processing failed: {method.delivery_tag}")
                
                if not self.config.auto_ack:
                    # Reject message and don't requeue (send to DLQ if configured)
                    channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON message: {e}")
            self.messages_failed += 1
            
            if not self.config.auto_ack:
                channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        
        except Exception as e:
            logger.error(f"Unexpected error processing message: {e}")
            self.messages_failed += 1
            
            if not self.config.auto_ack:
                channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
    
    def _validate_message(self, message: Dict[str, Any]) -> bool:
        """
        Validate message structure
        
        Args:
            message: Message data to validate
            
        Returns:
            True if message is valid
        """
        required_fields = ['timestamp', 'datetime', 'device_id', 'measurements']
        
        # Check required fields
        if not all(field in message for field in required_fields):
            missing_fields = [field for field in required_fields if field not in message]
            logger.error(f"Missing required fields: {missing_fields}")
            return False
        
        # Check measurements structure
        measurements = message.get('measurements', {})
        if not isinstance(measurements, dict):
            logger.error("Measurements field must be a dictionary")
            return False
        
        # Check if measurements contain expected sensor data
        expected_measurements = ['temperature', 'humidity', 'co2']
        if not any(measurement in measurements for measurement in expected_measurements):
            logger.error(f"No valid measurements found. Expected one of: {expected_measurements}")
            return False
        
        # Validate data types
        try:
            int(message['timestamp'])
            datetime.fromisoformat(message['datetime'].replace('Z', '+00:00'))
            str(message['device_id'])
            
            # Validate measurement values are numeric
            for key, value in measurements.items():
                float(value)
            
            return True
            
        except (ValueError, TypeError) as e:
            logger.error(f"Invalid data types in message: {e}")
            return False
    
    def start_consuming(self):
        """Start consuming messages from RabbitMQ queue"""
        if self.is_consuming:
            logger.warning("Consumer is already running")
            return
        
        logger.info("Starting RabbitMQ consumer...")
        
        while not self.should_stop:
            try:
                # Create connection if not exists
                if not self.connection or self.connection.is_closed:
                    if not self._create_connection():
                        if not self._reconnect_with_backoff():
                            logger.error("Failed to establish connection, stopping consumer")
                            break
                
                # Setup consumer
                self.channel.basic_consume(
                    queue=self.config.queue_name,
                    on_message_callback=self._process_message,
                    auto_ack=self.config.auto_ack
                )
                
                self.is_consuming = True
                logger.info(f"Started consuming from queue: {self.config.queue_name}")
                
                # Start consuming (blocking call)
                self.channel.start_consuming()
                
            except pika.exceptions.ConnectionClosedByBroker:
                logger.warning("Connection closed by broker, attempting to reconnect...")
                self.is_consuming = False
                self._cleanup_connection()
                
                if not self._reconnect_with_backoff():
                    break
            
            except pika.exceptions.AMQPChannelError as e:
                logger.error(f"AMQP Channel error: {e}")
                self.is_consuming = False
                self._cleanup_connection()
                
                if not self._reconnect_with_backoff():
                    break
            
            except pika.exceptions.AMQPConnectionError as e:
                logger.error(f"AMQP Connection error: {e}")
                self.is_consuming = False
                self._cleanup_connection()
                
                if not self._reconnect_with_backoff():
                    break
            
            except KeyboardInterrupt:
                logger.info("Received keyboard interrupt, stopping consumer...")
                self.stop_consuming()
                break
            
            except Exception as e:
                logger.error(f"Unexpected error in consumer: {e}")
                self.is_consuming = False
                time.sleep(5)  # Wait before retry
        
        logger.info("RabbitMQ consumer stopped")
    
    def stop_consuming(self):
        """Stop consuming messages gracefully"""
        logger.info("Stopping RabbitMQ consumer...")
        self.should_stop = True
        
        if self.is_consuming and self.channel:
            try:
                self.channel.stop_consuming()
                self.is_consuming = False
            except Exception as e:
                logger.error(f"Error stopping consumer: {e}")
        
        self._cleanup_connection()
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get consumer statistics
        
        Returns:
            Dictionary containing consumer statistics
        """
        return {
            'is_consuming': self.is_consuming,
            'messages_received': self.messages_received,
            'messages_processed': self.messages_processed,
            'messages_failed': self.messages_failed,
            'success_rate': (
                self.messages_processed / self.messages_received * 100
                if self.messages_received > 0 else 0
            ),
            'last_message_time': self.last_message_time.isoformat() if self.last_message_time else None,
            'connection_status': 'connected' if (self.connection and not self.connection.is_closed) else 'disconnected'
        }
    
    def health_check(self) -> Dict[str, Any]:
        """
        Perform health check
        
        Returns:
            Dictionary containing health check results
        """
        health_info = {
            'is_healthy': False,
            'connection_status': 'disconnected',
            'queue_accessible': False,
            'last_message_age_seconds': None,
            'error': None
        }
        
        try:
            # Check connection
            if self.connection and not self.connection.is_closed:
                health_info['connection_status'] = 'connected'
                
                # Check if queue is accessible
                if self.channel and not self.channel.is_closed:
                    # Try to get queue info (passive declare)
                    method = self.channel.queue_declare(queue=self.config.queue_name, passive=True)
                    health_info['queue_accessible'] = True
                    health_info['queue_message_count'] = method.method.message_count
            
            # Check last message age
            if self.last_message_time:
                age_seconds = (datetime.now() - self.last_message_time).total_seconds()
                health_info['last_message_age_seconds'] = age_seconds
            
            # Overall health status
            health_info['is_healthy'] = (
                health_info['connection_status'] == 'connected' and
                health_info['queue_accessible']
            )
            
        except Exception as e:
            health_info['error'] = str(e)
            logger.error(f"Health check failed: {e}")
        
        return health_info


def create_rabbitmq_consumer(config: RabbitMQConfig, message_handler: Callable[[Dict[str, Any]], bool]) -> RabbitMQConsumer:
    """
    Factory function to create RabbitMQ consumer
    
    Args:
        config: RabbitMQ configuration
        message_handler: Function to handle received messages
        
    Returns:
        RabbitMQConsumer instance
    """
    return RabbitMQConsumer(config, message_handler)