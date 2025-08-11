"""
Main application for DataLogger Agent

This module integrates all components (database, RabbitMQ consumer, validator, logger)
to create a complete data logging service for IoT sensor data.
"""
import os
import sys
import logging
import signal
import time
import threading
from typing import Dict, Any
from datetime import datetime
import structlog
from dotenv import load_dotenv
import json
from http.server import HTTPServer, BaseHTTPRequestHandler

# Import our modules
from database import create_database_connection_from_env
from rabbitmq_consumer import RabbitMQConsumer, RabbitMQConfig
from data_validator import create_data_validator
from data_logger import create_data_logger
from db_health_check import DatabaseHealthChecker

# Load environment variables
load_dotenv()

# Configure structured logging
structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
        structlog.processors.JSONRenderer()
    ],
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    wrapper_class=structlog.stdlib.BoundLogger,
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger(__name__)


class HealthCheckHandler(BaseHTTPRequestHandler):
    """HTTP handler for health check endpoints"""
    
    def __init__(self, agent_instance, *args, **kwargs):
        self.agent = agent_instance
        super().__init__(*args, **kwargs)
    
    def do_GET(self):
        """Handle GET requests for health checks"""
        try:
            if self.path == '/health':
                # Basic health check
                health_info = self.agent.get_health_status()
                self._send_json_response(200, health_info)
            elif self.path == '/health/detailed':
                # Detailed health check
                detailed_health = self.agent.get_detailed_health_status()
                self._send_json_response(200, detailed_health)
            elif self.path == '/health/database':
                # Database-specific health check
                db_health = self.agent.get_database_health_status()
                self._send_json_response(200, db_health)
            else:
                self._send_json_response(404, {'error': 'Endpoint not found'})
        except Exception as e:
            self._send_json_response(500, {'error': str(e)})
    
    def _send_json_response(self, status_code, data):
        """Send JSON response"""
        self.send_response(status_code)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        response = json.dumps(data, indent=2, default=str)
        self.wfile.write(response.encode())
    
    def log_message(self, format, *args):
        """Override to use our logger"""
        logger.info("Health check request", path=self.path, client=self.client_address[0])


class DataLoggerAgent:
    """
    Main DataLogger Agent application
    
    Integrates all components to provide a complete data logging service
    """
    
    def __init__(self):
        """Initialize the DataLogger Agent"""
        self.db_connection = None
        self.validator = None
        self.data_logger = None
        self.rabbitmq_consumer = None
        self.is_running = False
        self.health_check_server = None
        self.health_check_thread = None
        self.last_message_time = None
        
        # Setup signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        logger.info("DataLogger Agent initialized")
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        logger.info(f"Received signal {signum}, initiating shutdown...")
        self.shutdown()
    
    def initialize_components(self):
        """Initialize all components"""
        try:
            logger.info("Initializing DataLogger Agent components...")
            
            # Initialize database connection
            logger.info("Connecting to database...")
            self.db_connection = create_database_connection_from_env()
            
            if not self.db_connection.connect():
                raise Exception("Failed to connect to database")
            
            # Initialize database schema
            if not self.db_connection.initialize_schema():
                logger.warning("Failed to initialize database schema")
            
            # Initialize data validator
            logger.info("Initializing data validator...")
            strict_mode = os.getenv('VALIDATOR_STRICT_MODE', 'true').lower() == 'true'
            self.validator = create_data_validator(strict_mode=strict_mode)
            
            # Initialize data logger
            logger.info("Initializing data logger...")
            batch_size = int(os.getenv('BATCH_SIZE', '100'))
            batch_timeout = float(os.getenv('BATCH_TIMEOUT', '30.0'))
            
            self.data_logger = create_data_logger(
                db_connection=self.db_connection,
                validator=self.validator,
                batch_size=batch_size,
                batch_timeout=batch_timeout
            )
            
            # Initialize RabbitMQ consumer
            logger.info("Initializing RabbitMQ consumer...")
            rabbitmq_config = RabbitMQConfig(
                host=os.getenv('RABBITMQ_HOST', 'localhost'),
                port=int(os.getenv('RABBITMQ_PORT', '5672')),
                username=os.getenv('RABBITMQ_USERNAME', 'guest'),
                password=os.getenv('RABBITMQ_PASSWORD', 'guest'),
                queue_name=os.getenv('RABBITMQ_QUEUE', 'iaq_data_queue'),
                exchange_name=os.getenv('RABBITMQ_EXCHANGE', 'iot_data'),
                routing_key=os.getenv('RABBITMQ_ROUTING_KEY', 'sensor.iaq')
            )
            
            self.rabbitmq_consumer = RabbitMQConsumer(
                config=rabbitmq_config,
                message_handler=self._handle_message
            )
            
            logger.info("All components initialized successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to initialize components: {e}")
            return False
    
    def _handle_message(self, message: Dict[str, Any]) -> bool:
        """
        Handle incoming RabbitMQ message
        
        Args:
            message: Sensor data message
            
        Returns:
            True if message was processed successfully
        """
        try:
            logger.debug("Processing sensor data message", device_id=message.get('device_id'))
            
            # Update last message time for health tracking
            self.last_message_time = datetime.now()
            
            # Log the sensor data
            success = self.data_logger.log_sensor_data(message)
            
            if success:
                logger.debug("Message processed successfully", device_id=message.get('device_id'))
            else:
                logger.error("Failed to process message", device_id=message.get('device_id'))
            
            return success
            
        except Exception as e:
            logger.error(f"Error handling message: {e}", device_id=message.get('device_id'))
            return False
    
    def start(self):
        """Start the DataLogger Agent"""
        try:
            logger.info("Starting DataLogger Agent...")
            
            if not self.initialize_components():
                logger.error("Failed to initialize components, exiting")
                return False
            
            # Perform health checks
            if not self._health_check():
                logger.error("Health check failed, exiting")
                return False
            
            # Start health check server
            self._start_health_check_server()
            
            self.is_running = True
            logger.info("DataLogger Agent started successfully")
            
            # Start consuming messages (blocking call)
            self.rabbitmq_consumer.start_consuming()
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to start DataLogger Agent: {e}")
            return False
    
    def shutdown(self):
        """Shutdown the DataLogger Agent gracefully"""
        if not self.is_running:
            return
        
        logger.info("Shutting down DataLogger Agent...")
        self.is_running = False
        
        try:
            # Stop RabbitMQ consumer
            if self.rabbitmq_consumer:
                logger.info("Stopping RabbitMQ consumer...")
                self.rabbitmq_consumer.stop_consuming()
            
            # Shutdown data logger (flush pending data)
            if self.data_logger:
                logger.info("Shutting down data logger...")
                self.data_logger.shutdown()
            
            # Stop health check server
            if self.health_check_server:
                logger.info("Stopping health check server...")
                self.health_check_server.shutdown()
                if self.health_check_thread:
                    self.health_check_thread.join(timeout=5)
            
            # Close database connection
            if self.db_connection:
                logger.info("Closing database connection...")
                self.db_connection.disconnect()
            
            logger.info("DataLogger Agent shutdown complete")
            
        except Exception as e:
            logger.error(f"Error during shutdown: {e}")
    
    def _health_check(self) -> bool:
        """
        Perform basic health checks on startup
        
        Returns:
            True if all health checks pass
        """
        try:
            logger.info("Performing health checks...")
            
            # Check database connection
            db_health = self.db_connection.health_check()
            if not db_health.get('is_connected', False):
                logger.error("Database health check failed", health=db_health)
                return False
            
            # Check RabbitMQ connection (if consumer exists)
            if self.rabbitmq_consumer:
                try:
                    consumer_health = self.rabbitmq_consumer.health_check()
                    if not consumer_health.get('is_healthy', False):
                        logger.warning("RabbitMQ consumer health check failed", health=consumer_health)
                        # Don't fail startup for RabbitMQ issues - it will retry
                except Exception as e:
                    logger.warning(f"RabbitMQ health check error: {e}")
            
            logger.info("All health checks passed")
            return True
            
        except Exception as e:
            logger.error(f"Health check failed with exception: {e}")
            return False
    
    def get_health_status(self) -> Dict[str, Any]:
        """Get basic health status"""
        return {
            'status': 'healthy' if self.is_running else 'stopped',
            'timestamp': datetime.now().isoformat(),
            'uptime_seconds': time.time() - (self.last_message_time.timestamp() if self.last_message_time else time.time()),
            'components': {
                'database': self.db_connection.health_check() if self.db_connection else {'status': 'not_initialized'},
                'rabbitmq': self._get_rabbitmq_health(),
                'validator': {'status': 'active' if self.validator else 'not_initialized'},
                'data_logger': {'status': 'active' if self.data_logger else 'not_initialized'}
            }
        }
    
    def get_detailed_health_status(self) -> Dict[str, Any]:
        """Get detailed health status including database checks"""
        basic_status = self.get_health_status()
        
        # Add detailed database health check
        if self.db_connection:
            try:
                db_checker = DatabaseHealthChecker(self.db_connection.connection_string)
                detailed_db_health = db_checker.run_health_check()
                basic_status['detailed_database_health'] = detailed_db_health
            except Exception as e:
                basic_status['detailed_database_health'] = {
                    'error': str(e),
                    'status': 'error'
                }
        
        # Add message processing stats
        basic_status['message_processing'] = {
            'last_message_time': self.last_message_time.isoformat() if self.last_message_time else None,
            'is_receiving_messages': bool(self.last_message_time and (datetime.now() - self.last_message_time).seconds < 300)
        }
        
        return basic_status
    
    def get_database_health_status(self) -> Dict[str, Any]:
        """Get database-specific health status"""
        if not self.db_connection:
            return {'error': 'Database connection not initialized'}
        
        try:
            db_checker = DatabaseHealthChecker(self.db_connection.connection_string)
            return db_checker.run_health_check()
        except Exception as e:
            return {'error': str(e), 'status': 'error'}
    
    def _get_rabbitmq_health(self) -> Dict[str, Any]:
        """Get RabbitMQ health status"""
        if not self.rabbitmq_consumer:
            return {'status': 'not_initialized'}
        
        try:
            return self.rabbitmq_consumer.health_check()
        except Exception as e:
            return {'status': 'error', 'error': str(e)}
    
    def _start_health_check_server(self):
        """Start HTTP server for health check endpoints"""
        try:
            health_check_port = int(os.getenv('HEALTH_CHECK_PORT', '8080'))
            
            # Create handler class with agent reference
            handler_class = lambda *args, **kwargs: HealthCheckHandler(self, *args, **kwargs)
            
            self.health_check_server = HTTPServer(('0.0.0.0', health_check_port), handler_class)
            
            # Start server in separate thread
            self.health_check_thread = threading.Thread(
                target=self.health_check_server.serve_forever,
                daemon=True
            )
            self.health_check_thread.start()
            
            logger.info(f"Health check server started on port {health_check_port}")
            
        except Exception as e:
            logger.error(f"Failed to start health check server: {e}")
            return False
    
    def get_status(self) -> Dict[str, Any]:
        """Get current status of all components"""
        status = {
            'is_running': self.is_running,
            'components': {}
        }
        
        try:
            # Database status
            if self.db_connection:
                status['components']['database'] = self.db_connection.health_check()
            
            # Data logger status
            if self.data_logger:
                status['components']['data_logger'] = self.data_logger.get_stats()
            
            # RabbitMQ consumer status
            if self.rabbitmq_consumer:
                status['components']['rabbitmq_consumer'] = self.rabbitmq_consumer.get_stats()
            
        except Exception as e:
            status['error'] = str(e)
        
        return status


def setup_logging():
    """Setup logging configuration"""
    log_level = os.getenv('LOG_LEVEL', 'INFO').upper()
    
    # Configure standard logging
    logging.basicConfig(
        level=getattr(logging, log_level),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Set specific logger levels
    logging.getLogger('pika').setLevel(logging.WARNING)
    logging.getLogger('psycopg2').setLevel(logging.WARNING)


def main():
    """Main entry point"""
    try:
        # Setup logging
        setup_logging()
        
        logger.info("Starting DataLogger Agent application...")
        
        # Create and start the agent
        agent = DataLoggerAgent()
        
        # Start the agent (blocking call)
        success = agent.start()
        
        if not success:
            logger.error("Failed to start DataLogger Agent")
            sys.exit(1)
        
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)
    finally:
        logger.info("DataLogger Agent application terminated")


if __name__ == '__main__':
    main()