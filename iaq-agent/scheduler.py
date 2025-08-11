"""
Data Scheduler module for IAQ Sensor Agent

This module provides functionality to schedule and coordinate the reading of
sensor data from CSV files and publishing to RabbitMQ at regular intervals.
"""
import time
import signal
import threading
from datetime import datetime, timedelta
from typing import Optional, Iterator, Dict, Any, Callable
import logging
from dataclasses import dataclass
from pathlib import Path
import os

from csv_reader import CSVReader, create_csv_reader, CSVReaderError
from multi_csv_reader import MultiCSVReader, create_multi_csv_reader, MultiCSVReaderError
from rabbitmq_client import RabbitMQClient, create_rabbitmq_client_from_env, RabbitMQClientError
from models import SensorReading


logger = logging.getLogger(__name__)


@dataclass
class SchedulerConfig:
    """Configuration for the data scheduler"""
    csv_file_path: str = None  # Single CSV file path (legacy)
    csv_directory_path: str = None  # Directory containing multiple CSV files
    device_id: str = None  # Single device ID (legacy)
    file_pattern: str = "*.csv"  # Pattern to match CSV files in directory
    reading_mode: str = "chronological"  # "chronological" or "round_robin"
    send_interval: int = 5  # seconds
    batch_size: int = 1  # number of readings per batch
    max_retries: int = 3
    retry_delay: int = 2  # seconds
    enable_replay: bool = True  # replay data when reaching end of file
    start_from_beginning: bool = True  # start from beginning of CSV file
    log_level: str = 'INFO'
    stats_interval: int = 60  # seconds between stats logging


class SchedulerError(Exception):
    """Custom exception for scheduler errors"""
    pass


class DataScheduler:
    """
    Data Scheduler for coordinating CSV reading and RabbitMQ publishing
    
    Features:
    - Scheduled data sending at configurable intervals
    - CSV data replay for continuous operation
    - Error handling and recovery
    - Performance monitoring and statistics
    - Graceful shutdown handling
    - Signal handling for clean termination
    """
    
    def __init__(self, config: SchedulerConfig, rabbitmq_client: Optional[RabbitMQClient] = None):
        """
        Initialize Data Scheduler
        
        Args:
            config: Scheduler configuration
            rabbitmq_client: Optional RabbitMQ client (will create from env if not provided)
        """
        self.config = config
        self.rabbitmq_client = rabbitmq_client or create_rabbitmq_client_from_env()
        self.csv_reader: Optional[CSVReader] = None
        self.multi_csv_reader: Optional[MultiCSVReader] = None
        self.data_iterator: Optional[Iterator[SensorReading]] = None
        self.is_multi_file_mode = False
        
        # State management
        self.is_running = False
        self.is_stopping = False
        self.start_time: Optional[datetime] = None
        self.last_stats_time: Optional[datetime] = None
        
        # Statistics
        self.stats = {
            'messages_sent': 0,
            'messages_failed': 0,
            'csv_rows_processed': 0,
            'csv_errors': 0,
            'rabbitmq_errors': 0,
            'replay_count': 0,
            'uptime_seconds': 0
        }
        
        # Threading
        self._main_thread: Optional[threading.Thread] = None
        self._stats_thread: Optional[threading.Thread] = None
        self._shutdown_event = threading.Event()
        
        # Setup logging
        self._setup_logging()
        
        # Setup signal handlers
        self._setup_signal_handlers()
        
        # Initialize CSV reader(s)
        self._initialize_csv_readers()
    
    def _setup_logging(self):
        """Setup logging configuration"""
        log_level = getattr(logging, self.config.log_level.upper(), logging.INFO)
        logging.getLogger(__name__).setLevel(log_level)
        
        logger.info(f"Scheduler initialized with config:")
        if self.config.csv_directory_path:
            logger.info(f"  CSV directory: {self.config.csv_directory_path}")
            logger.info(f"  File pattern: {self.config.file_pattern}")
            logger.info(f"  Reading mode: {self.config.reading_mode}")
        else:
            logger.info(f"  CSV file: {self.config.csv_file_path}")
            logger.info(f"  Device ID: {self.config.device_id}")
        logger.info(f"  Send interval: {self.config.send_interval}s")
        logger.info(f"  Batch size: {self.config.batch_size}")
        logger.info(f"  Replay enabled: {self.config.enable_replay}")
    
    def _setup_signal_handlers(self):
        """Setup signal handlers for graceful shutdown"""
        def signal_handler(signum, frame):
            logger.info(f"Received signal {signum}, initiating graceful shutdown...")
            self.stop()
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
    
    def _initialize_csv_readers(self):
        """Initialize CSV reader(s) and data iterator"""
        try:
            # Determine if we're using single file or multi-file mode
            if self.config.csv_directory_path:
                # Multi-file mode
                self.is_multi_file_mode = True
                self.multi_csv_reader = create_multi_csv_reader(
                    self.config.csv_directory_path, 
                    self.config.file_pattern
                )
                
                # Get summary info for logging
                summary = self.multi_csv_reader.get_summary()
                logger.info(f"Multi-CSV mode initialized:")
                logger.info(f"  Directory: {summary['directory_path']}")
                logger.info(f"  Devices: {summary['device_count']}")
                logger.info(f"  Total records: {summary['total_estimated_records']}")
                logger.info(f"  Total size: {summary['total_size_mb']} MB")
                logger.info(f"  Device IDs: {', '.join(summary['devices'])}")
                
            else:
                # Single file mode (legacy)
                self.is_multi_file_mode = False
                if not self.config.csv_file_path or not self.config.device_id:
                    raise SchedulerError("Either csv_directory_path or both csv_file_path and device_id must be provided")
                
                self.csv_reader = create_csv_reader(self.config.csv_file_path, self.config.device_id)
                
                # Get file info for logging
                file_info = self.csv_reader.get_file_info()
                logger.info(f"Single CSV file loaded: {file_info['estimated_records']} records, "
                           f"{file_info['file_size_mb']} MB")
            
            # Initialize data iterator
            self._reset_data_iterator()
            
        except (CSVReaderError, MultiCSVReaderError) as e:
            raise SchedulerError(f"Failed to initialize CSV reader(s): {e}") from e
    
    def _reset_data_iterator(self):
        """Reset data iterator to beginning of CSV file(s)"""
        if self.is_multi_file_mode and self.multi_csv_reader:
            if self.config.reading_mode == "round_robin":
                self.data_iterator = self.multi_csv_reader.read_iterator_round_robin()
                logger.debug("Data iterator reset to beginning of CSV files (round-robin mode)")
            else:
                self.data_iterator = self.multi_csv_reader.read_iterator_combined()
                logger.debug("Data iterator reset to beginning of CSV files (chronological mode)")
        elif self.csv_reader:
            self.data_iterator = self.csv_reader.read_iterator()
            logger.debug("Data iterator reset to beginning of CSV file")
    
    def start(self):
        """Start the scheduler"""
        if self.is_running:
            logger.warning("Scheduler is already running")
            return
        
        logger.info("Starting IAQ Sensor Agent Scheduler...")
        
        # Connect to RabbitMQ
        if not self.rabbitmq_client.connect():
            raise SchedulerError("Failed to connect to RabbitMQ")
        
        logger.info("Connected to RabbitMQ successfully")
        
        # Initialize state
        self.is_running = True
        self.is_stopping = False
        self.start_time = datetime.utcnow()
        self.last_stats_time = self.start_time
        self._shutdown_event.clear()
        
        # Start main processing thread
        self._main_thread = threading.Thread(target=self._main_loop, daemon=False)
        self._main_thread.start()
        
        # Start statistics thread
        self._stats_thread = threading.Thread(target=self._stats_loop, daemon=True)
        self._stats_thread.start()
        
        logger.info("Scheduler started successfully")
    
    def stop(self):
        """Stop the scheduler gracefully"""
        if not self.is_running:
            logger.warning("Scheduler is not running")
            return
        
        logger.info("Stopping scheduler...")
        self.is_stopping = True
        self._shutdown_event.set()
        
        # Wait for main thread to finish
        if self._main_thread and self._main_thread.is_alive():
            logger.info("Waiting for main thread to finish...")
            self._main_thread.join(timeout=10)
            
            if self._main_thread.is_alive():
                logger.warning("Main thread did not finish within timeout")
        
        # Disconnect from RabbitMQ
        try:
            self.rabbitmq_client.disconnect()
            logger.info("Disconnected from RabbitMQ")
        except Exception as e:
            logger.error(f"Error disconnecting from RabbitMQ: {e}")
        
        self.is_running = False
        logger.info("Scheduler stopped")
        
        # Log final statistics
        self._log_statistics(final=True)
    
    def _main_loop(self):
        """Main processing loop"""
        logger.info("Main processing loop started")
        
        try:
            while not self._shutdown_event.is_set():
                loop_start_time = time.time()
                
                try:
                    # Process batch of data
                    self._process_data_batch()
                    
                except Exception as e:
                    logger.error(f"Error in main loop: {e}")
                    self.stats['rabbitmq_errors'] += 1
                    
                    # Handle critical errors
                    if isinstance(e, (SchedulerError, RabbitMQClientError)):
                        logger.error("Critical error encountered, attempting recovery...")
                        if not self._attempt_recovery():
                            logger.error("Recovery failed, stopping scheduler")
                            break
                
                # Calculate sleep time to maintain interval
                loop_duration = time.time() - loop_start_time
                sleep_time = max(0, self.config.send_interval - loop_duration)
                
                if sleep_time > 0:
                    if self._shutdown_event.wait(timeout=sleep_time):
                        break  # Shutdown requested
                else:
                    logger.warning(f"Processing took {loop_duration:.2f}s, "
                                 f"longer than interval {self.config.send_interval}s")
        
        except Exception as e:
            logger.error(f"Fatal error in main loop: {e}")
        
        finally:
            logger.info("Main processing loop ended")
    
    def _process_data_batch(self):
        """Process a batch of sensor readings"""
        if not self.data_iterator:
            raise SchedulerError("Data iterator not initialized")
        
        readings_sent = 0
        
        for _ in range(self.config.batch_size):
            try:
                # Get next reading
                reading = next(self.data_iterator)
                self.stats['csv_rows_processed'] += 1
                
                # Send to RabbitMQ
                success = self._send_reading_with_retry(reading)
                
                if success:
                    readings_sent += 1
                    self.stats['messages_sent'] += 1
                    logger.debug(f"Sent reading: {reading.device_id} at {reading.datetime}")
                else:
                    self.stats['messages_failed'] += 1
                    logger.warning(f"Failed to send reading: {reading.device_id} at {reading.datetime}")
                
            except StopIteration:
                # End of CSV file reached
                if self.config.enable_replay:
                    logger.info("End of CSV file reached, replaying from beginning...")
                    self._reset_data_iterator()
                    self.stats['replay_count'] += 1
                    
                    # Try to get first reading from replayed data
                    try:
                        reading = next(self.data_iterator)
                        self.stats['csv_rows_processed'] += 1
                        
                        success = self._send_reading_with_retry(reading)
                        if success:
                            readings_sent += 1
                            self.stats['messages_sent'] += 1
                        else:
                            self.stats['messages_failed'] += 1
                    except StopIteration:
                        logger.error("CSV file appears to be empty")
                        raise SchedulerError("CSV file is empty")
                else:
                    logger.info("End of CSV file reached, replay disabled, stopping...")
                    self.stop()
                    break
            
            except CSVReaderError as e:
                logger.error(f"CSV reading error: {e}")
                self.stats['csv_errors'] += 1
                continue  # Skip this reading and continue
        
        if readings_sent > 0:
            logger.debug(f"Batch processed: {readings_sent}/{self.config.batch_size} readings sent")
    
    def _send_reading_with_retry(self, reading: SensorReading) -> bool:
        """Send reading with retry logic"""
        for attempt in range(self.config.max_retries + 1):
            try:
                success = self.rabbitmq_client.send_sensor_reading(reading)
                
                if success:
                    return True
                
                if attempt < self.config.max_retries:
                    logger.warning(f"Send failed, retrying in {self.config.retry_delay}s "
                                 f"(attempt {attempt + 1}/{self.config.max_retries + 1})")
                    time.sleep(self.config.retry_delay)
                
            except Exception as e:
                logger.error(f"Error sending reading (attempt {attempt + 1}): {e}")
                if attempt < self.config.max_retries:
                    time.sleep(self.config.retry_delay)
        
        return False
    
    def _attempt_recovery(self) -> bool:
        """Attempt to recover from errors"""
        logger.info("Attempting error recovery...")
        
        try:
            # Check RabbitMQ connection health
            if not self.rabbitmq_client.is_connection_healthy():
                logger.info("RabbitMQ connection unhealthy, attempting reconnection...")
                self.rabbitmq_client.disconnect()
                
                if self.rabbitmq_client.connect():
                    logger.info("RabbitMQ reconnection successful")
                else:
                    logger.error("RabbitMQ reconnection failed")
                    return False
            
            # Reset CSV iterator if needed
            if not self.data_iterator:
                logger.info("Resetting CSV data iterator...")
                self._reset_data_iterator()
            
            logger.info("Error recovery completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error during recovery: {e}")
            return False
    
    def _stats_loop(self):
        """Statistics logging loop"""
        while not self._shutdown_event.is_set():
            if self._shutdown_event.wait(timeout=self.config.stats_interval):
                break  # Shutdown requested
            
            self._log_statistics()
    
    def _log_statistics(self, final: bool = False):
        """Log current statistics"""
        if not self.start_time:
            return
        
        current_time = datetime.utcnow()
        uptime = (current_time - self.start_time).total_seconds()
        self.stats['uptime_seconds'] = uptime
        
        # Calculate rates
        messages_per_second = self.stats['messages_sent'] / uptime if uptime > 0 else 0
        success_rate = (self.stats['messages_sent'] / 
                       (self.stats['messages_sent'] + self.stats['messages_failed']) * 100
                       if (self.stats['messages_sent'] + self.stats['messages_failed']) > 0 else 0)
        
        log_prefix = "FINAL STATS" if final else "STATS"
        
        logger.info(f"{log_prefix}: Uptime: {uptime:.0f}s, "
                   f"Messages sent: {self.stats['messages_sent']}, "
                   f"Failed: {self.stats['messages_failed']}, "
                   f"Rate: {messages_per_second:.2f}/s, "
                   f"Success: {success_rate:.1f}%")
        
        if self.stats['replay_count'] > 0:
            logger.info(f"{log_prefix}: CSV replays: {self.stats['replay_count']}, "
                       f"Rows processed: {self.stats['csv_rows_processed']}, "
                       f"CSV errors: {self.stats['csv_errors']}")
        
        self.last_stats_time = current_time
    
    def get_status(self) -> Dict[str, Any]:
        """Get current scheduler status"""
        current_time = datetime.utcnow()
        uptime = (current_time - self.start_time).total_seconds() if self.start_time else 0
        
        return {
            'is_running': self.is_running,
            'is_stopping': self.is_stopping,
            'uptime_seconds': uptime,
            'start_time': self.start_time.isoformat() if self.start_time else None,
            'config': {
                'csv_file_path': self.config.csv_file_path,
                'device_id': self.config.device_id,
                'send_interval': self.config.send_interval,
                'batch_size': self.config.batch_size,
                'enable_replay': self.config.enable_replay
            },
            'statistics': self.stats.copy(),
            'rabbitmq_connection': self.rabbitmq_client.get_connection_info()
        }
    
    def wait_for_completion(self):
        """Wait for scheduler to complete (blocking)"""
        if self._main_thread and self._main_thread.is_alive():
            try:
                self._main_thread.join()
            except KeyboardInterrupt:
                logger.info("Keyboard interrupt received, stopping scheduler...")
                self.stop()


def create_scheduler_from_env() -> DataScheduler:
    """
    Create scheduler from environment variables
    
    Expected environment variables:
    Multi-file mode (preferred):
    - CSV_DIRECTORY_PATH: Path to directory containing CSV files
    - CSV_FILE_PATTERN: Pattern to match CSV files (default: "*.csv")
    - CSV_READING_MODE: Reading mode - "chronological" or "round_robin" (default: "chronological")
    
    Single-file mode (legacy):
    - CSV_DATA_PATH: Path to CSV file
    - DEVICE_ID: Device identifier
    
    Common configuration:
    - SEND_INTERVAL: Send interval in seconds (default: 5)
    - BATCH_SIZE: Number of readings per batch (default: 1)
    - MAX_RETRIES: Maximum retry attempts (default: 3)
    - RETRY_DELAY: Delay between retries in seconds (default: 2)
    - ENABLE_REPLAY: Enable CSV replay (default: true)
    - LOG_LEVEL: Logging level (default: INFO)
    - STATS_INTERVAL: Statistics logging interval (default: 60)
    
    Returns:
        DataScheduler instance
    """
    # Check if multi-file mode is configured
    csv_directory_path = os.getenv('CSV_DIRECTORY_PATH')
    csv_data_path = os.getenv('CSV_DATA_PATH')
    
    if csv_directory_path:
        # Multi-file mode
        config = SchedulerConfig(
            csv_directory_path=csv_directory_path,
            file_pattern=os.getenv('CSV_FILE_PATTERN', '*.csv'),
            reading_mode=os.getenv('CSV_READING_MODE', 'chronological'),
            send_interval=int(os.getenv('SEND_INTERVAL', '5')),
            batch_size=int(os.getenv('BATCH_SIZE', '1')),
            max_retries=int(os.getenv('MAX_RETRIES', '3')),
            retry_delay=int(os.getenv('RETRY_DELAY', '2')),
            enable_replay=os.getenv('ENABLE_REPLAY', 'true').lower() == 'true',
            start_from_beginning=os.getenv('START_FROM_BEGINNING', 'true').lower() == 'true',
            log_level=os.getenv('LOG_LEVEL', 'INFO'),
            stats_interval=int(os.getenv('STATS_INTERVAL', '60'))
        )
    else:
        # Single-file mode (legacy) - default to sample_data directory if no specific file
        if not csv_data_path:
            csv_data_path = './sample_data'
            # Check if it's a directory
            if os.path.isdir(csv_data_path):
                config = SchedulerConfig(
                    csv_directory_path=csv_data_path,
                    file_pattern='*.csv',
                    reading_mode='chronological',
                    send_interval=int(os.getenv('SEND_INTERVAL', '5')),
                    batch_size=int(os.getenv('BATCH_SIZE', '1')),
                    max_retries=int(os.getenv('MAX_RETRIES', '3')),
                    retry_delay=int(os.getenv('RETRY_DELAY', '2')),
                    enable_replay=os.getenv('ENABLE_REPLAY', 'true').lower() == 'true',
                    start_from_beginning=os.getenv('START_FROM_BEGINNING', 'true').lower() == 'true',
                    log_level=os.getenv('LOG_LEVEL', 'INFO'),
                    stats_interval=int(os.getenv('STATS_INTERVAL', '60'))
                )
            else:
                # Fallback to single file
                config = SchedulerConfig(
                    csv_file_path='./sample_data/sample_iaq_data_Room101.csv',
                    device_id=os.getenv('DEVICE_ID', 'room_101'),
                    send_interval=int(os.getenv('SEND_INTERVAL', '5')),
                    batch_size=int(os.getenv('BATCH_SIZE', '1')),
                    max_retries=int(os.getenv('MAX_RETRIES', '3')),
                    retry_delay=int(os.getenv('RETRY_DELAY', '2')),
                    enable_replay=os.getenv('ENABLE_REPLAY', 'true').lower() == 'true',
                    start_from_beginning=os.getenv('START_FROM_BEGINNING', 'true').lower() == 'true',
                    log_level=os.getenv('LOG_LEVEL', 'INFO'),
                    stats_interval=int(os.getenv('STATS_INTERVAL', '60'))
                )
        else:
            # Check if the path is a directory or file
            if os.path.isdir(csv_data_path):
                # Directory mode
                config = SchedulerConfig(
                    csv_directory_path=csv_data_path,
                    file_pattern='*.csv',
                    reading_mode='chronological',
                    send_interval=int(os.getenv('SEND_INTERVAL', '5')),
                    batch_size=int(os.getenv('BATCH_SIZE', '1')),
                    max_retries=int(os.getenv('MAX_RETRIES', '3')),
                    retry_delay=int(os.getenv('RETRY_DELAY', '2')),
                    enable_replay=os.getenv('ENABLE_REPLAY', 'true').lower() == 'true',
                    start_from_beginning=os.getenv('START_FROM_BEGINNING', 'true').lower() == 'true',
                    log_level=os.getenv('LOG_LEVEL', 'INFO'),
                    stats_interval=int(os.getenv('STATS_INTERVAL', '60'))
                )
            else:
                # Single file mode
                config = SchedulerConfig(
                    csv_file_path=csv_data_path,
                    device_id=os.getenv('DEVICE_ID', 'room_101'),
                    send_interval=int(os.getenv('SEND_INTERVAL', '5')),
                    batch_size=int(os.getenv('BATCH_SIZE', '1')),
                    max_retries=int(os.getenv('MAX_RETRIES', '3')),
                    retry_delay=int(os.getenv('RETRY_DELAY', '2')),
                    enable_replay=os.getenv('ENABLE_REPLAY', 'true').lower() == 'true',
                    start_from_beginning=os.getenv('START_FROM_BEGINNING', 'true').lower() == 'true',
                    log_level=os.getenv('LOG_LEVEL', 'INFO'),
                    stats_interval=int(os.getenv('STATS_INTERVAL', '60'))
                )
    
    return DataScheduler(config)


def create_scheduler(
    csv_file_path: str = None,
    device_id: str = None,
    csv_directory_path: str = None,
    send_interval: int = 5,
    **kwargs
) -> DataScheduler:
    """
    Factory function to create scheduler
    
    Args:
        csv_file_path: Path to CSV file (single-file mode)
        device_id: Device identifier (single-file mode)
        csv_directory_path: Path to directory containing CSV files (multi-file mode)
        send_interval: Send interval in seconds
        **kwargs: Additional configuration parameters
        
    Returns:
        DataScheduler instance
    """
    if csv_directory_path:
        # Multi-file mode
        config = SchedulerConfig(
            csv_directory_path=csv_directory_path,
            send_interval=send_interval,
            **kwargs
        )
    else:
        # Single-file mode
        if not csv_file_path or not device_id:
            raise ValueError("Either csv_directory_path or both csv_file_path and device_id must be provided")
        
        config = SchedulerConfig(
            csv_file_path=csv_file_path,
            device_id=device_id,
            send_interval=send_interval,
            **kwargs
        )
    
    return DataScheduler(config)


def create_multi_file_scheduler(
    csv_directory_path: str,
    send_interval: int = 5,
    file_pattern: str = "*.csv",
    reading_mode: str = "chronological",
    **kwargs
) -> DataScheduler:
    """
    Factory function to create multi-file scheduler
    
    Args:
        csv_directory_path: Path to directory containing CSV files
        send_interval: Send interval in seconds
        file_pattern: Pattern to match CSV files
        reading_mode: Reading mode - "chronological" or "round_robin"
        **kwargs: Additional configuration parameters
        
    Returns:
        DataScheduler instance
    """
    config = SchedulerConfig(
        csv_directory_path=csv_directory_path,
        file_pattern=file_pattern,
        reading_mode=reading_mode,
        send_interval=send_interval,
        **kwargs
    )
    
    return DataScheduler(config)