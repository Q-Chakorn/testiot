"""
Data Logger module for DataLogger Agent

This module provides functionality to log sensor data to TimescaleDB
with batch processing, data transformation, and error handling.
"""
import logging
import time
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime
from dataclasses import dataclass
from threading import Lock
import json

from database import DatabaseConnection, DatabaseError
from data_validator import DataValidator, ValidationResult, ValidationSeverity

logger = logging.getLogger(__name__)


@dataclass
class LoggingStats:
    """Statistics for data logging operations"""
    messages_received: int = 0
    messages_logged: int = 0
    messages_failed: int = 0
    validation_errors: int = 0
    database_errors: int = 0
    batch_operations: int = 0
    last_log_time: Optional[datetime] = None


class DataLoggerError(Exception):
    """Custom exception for data logger errors"""
    pass


class DataLogger:
    """
    Data logger for sensor data with batch processing and validation
    
    Features:
    - Data validation and sanitization
    - Batch processing for better performance
    - Data transformation to raw_data format
    - Error handling and retry logic
    - Performance monitoring
    - Data deduplication
    """
    
    def __init__(
        self, 
        db_connection: DatabaseConnection, 
        validator: DataValidator,
        batch_size: int = 100,
        batch_timeout: float = 30.0,
        enable_deduplication: bool = True,
        max_retries: int = 3
    ):
        """
        Initialize data logger
        
        Args:
            db_connection: Database connection instance
            validator: Data validator instance
            batch_size: Number of records to batch before writing
            batch_timeout: Maximum time to wait before writing partial batch
            enable_deduplication: Enable data deduplication
            max_retries: Maximum number of retry attempts for failed operations
        """
        self.db_connection = db_connection
        self.validator = validator
        self.batch_size = batch_size
        self.batch_timeout = batch_timeout
        self.enable_deduplication = enable_deduplication
        self.max_retries = max_retries
        
        # Batch processing
        self.batch_buffer: List[Dict[str, Any]] = []
        self.batch_lock = Lock()
        self.last_batch_time = time.time()
        
        # Statistics
        self.stats = LoggingStats()
        self.stats_lock = Lock()
        
        # Deduplication cache (simple in-memory cache)
        self.dedup_cache: Dict[str, float] = {}
        self.dedup_cache_size = 10000
        
        logger.info(f"DataLogger initialized (batch_size={batch_size}, timeout={batch_timeout}s)")
    
    def log_sensor_data(self, data: Dict[str, Any]) -> bool:
        """
        Log sensor data to database
        
        Args:
            data: Sensor data dictionary
            
        Returns:
            True if data was successfully logged or queued for logging
        """
        try:
            with self.stats_lock:
                self.stats.messages_received += 1
            
            # Validate data
            validation_result = self.validator.validate_sensor_data(data)
            
            if not validation_result.is_valid:
                logger.error(f"Data validation failed: {validation_result.errors}")
                with self.stats_lock:
                    self.stats.validation_errors += 1
                    self.stats.messages_failed += 1
                return False
            
            # Log warnings if any
            if validation_result.warnings:
                logger.warning(f"Data validation warnings: {validation_result.warnings}")
            
            # Use sanitized data
            sanitized_data = validation_result.sanitized_data or data
            
            # Check for duplicates
            if self.enable_deduplication and self._is_duplicate(sanitized_data):
                logger.debug(f"Duplicate data detected, skipping: {sanitized_data.get('device_id')}")
                return True  # Consider as success since it's a duplicate
            
            # Convert to raw_data format
            raw_data_records = self.convert_to_raw_data_format(sanitized_data)
            
            if not raw_data_records:
                logger.error("Failed to convert data to raw_data format")
                with self.stats_lock:
                    self.stats.messages_failed += 1
                return False
            
            # Add to batch buffer
            with self.batch_lock:
                self.batch_buffer.extend(raw_data_records)
                
                # Check if batch is ready
                if len(self.batch_buffer) >= self.batch_size:
                    self._flush_batch()
                elif time.time() - self.last_batch_time >= self.batch_timeout:
                    self._flush_batch()
            
            return True
            
        except Exception as e:
            logger.error(f"Unexpected error logging sensor data: {e}")
            with self.stats_lock:
                self.stats.messages_failed += 1
            return False
    
    def convert_to_raw_data_format(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Convert sensor data to raw_data table format
        
        Args:
            data: Validated sensor data
            
        Returns:
            List of raw_data records
        """
        try:
            records = []
            
            # Extract common fields
            timestamp = data.get('timestamp')
            datetime_str = data.get('datetime')
            device_id = data.get('device_id')
            measurements = data.get('measurements', {})
            
            # Convert datetime string to datetime object
            try:
                if isinstance(datetime_str, str):
                    # Handle different datetime formats
                    if datetime_str.endswith('Z'):
                        dt = datetime.fromisoformat(datetime_str.replace('Z', '+00:00'))
                    else:
                        dt = datetime.fromisoformat(datetime_str)
                else:
                    dt = datetime.fromtimestamp(timestamp)
            except (ValueError, TypeError) as e:
                logger.error(f"Error parsing datetime: {e}")
                dt = datetime.fromtimestamp(timestamp)
            
            # Create a record for each measurement
            for datapoint, value in measurements.items():
                record = {
                    'timestamp': timestamp,
                    'datetime': dt,
                    'device_id': device_id,
                    'datapoint': datapoint,
                    'value': str(value)  # Store as string for flexibility
                }
                records.append(record)
            
            logger.debug(f"Converted {len(records)} records for device {device_id}")
            return records
            
        except Exception as e:
            logger.error(f"Error converting data to raw_data format: {e}")
            return []
    
    def batch_insert(self, records: List[Dict[str, Any]]) -> bool:
        """
        Insert batch of records into database
        
        Args:
            records: List of raw_data records
            
        Returns:
            True if batch insert successful
        """
        if not records:
            return True
        
        try:
            # Prepare SQL query
            insert_query = """
                INSERT INTO raw_data (timestamp, datetime, device_id, datapoint, value)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (datetime, device_id, datapoint) DO UPDATE SET
                    timestamp = EXCLUDED.timestamp,
                    value = EXCLUDED.value,
                    created_at = NOW()
            """
            
            # Prepare parameters
            params_list = []
            for record in records:
                params = (
                    record['timestamp'],
                    record['datetime'],
                    record['device_id'],
                    record['datapoint'],
                    record['value']
                )
                params_list.append(params)
            
            # Execute batch insert with retry logic
            for attempt in range(self.max_retries):
                try:
                    rows_affected = self.db_connection.execute_many(insert_query, params_list)
                    
                    logger.info(f"Batch insert successful: {rows_affected} rows affected")
                    
                    with self.stats_lock:
                        self.stats.messages_logged += len(records)
                        self.stats.batch_operations += 1
                        self.stats.last_log_time = datetime.now()
                    
                    return True
                    
                except DatabaseError as e:
                    logger.error(f"Database error on attempt {attempt + 1}: {e}")
                    if attempt == self.max_retries - 1:
                        raise
                    time.sleep(2 ** attempt)  # Exponential backoff
            
            return False
            
        except Exception as e:
            logger.error(f"Failed to insert batch: {e}")
            with self.stats_lock:
                self.stats.database_errors += 1
                self.stats.messages_failed += len(records)
            return False
    
    def _flush_batch(self):
        """Flush current batch to database"""
        if not self.batch_buffer:
            return
        
        try:
            # Get current batch
            current_batch = self.batch_buffer.copy()
            self.batch_buffer.clear()
            self.last_batch_time = time.time()
            
            logger.debug(f"Flushing batch of {len(current_batch)} records")
            
            # Insert batch
            success = self.batch_insert(current_batch)
            
            if not success:
                logger.error(f"Failed to flush batch of {len(current_batch)} records")
                # Could implement retry queue here
            
        except Exception as e:
            logger.error(f"Error flushing batch: {e}")
    
    def _is_duplicate(self, data: Dict[str, Any]) -> bool:
        """
        Check if data is a duplicate based on device_id and timestamp
        
        Args:
            data: Sensor data to check
            
        Returns:
            True if data is considered a duplicate
        """
        try:
            device_id = data.get('device_id')
            timestamp = data.get('timestamp')
            
            if not device_id or timestamp is None:
                return False
            
            # Create cache key
            cache_key = f"{device_id}:{timestamp}"
            
            # Check cache
            if cache_key in self.dedup_cache:
                return True
            
            # Add to cache
            self.dedup_cache[cache_key] = time.time()
            
            # Clean cache if it gets too large
            if len(self.dedup_cache) > self.dedup_cache_size:
                self._clean_dedup_cache()
            
            return False
            
        except Exception as e:
            logger.error(f"Error checking for duplicates: {e}")
            return False
    
    def _clean_dedup_cache(self):
        """Clean old entries from deduplication cache"""
        try:
            current_time = time.time()
            cache_ttl = 3600  # 1 hour TTL
            
            # Remove old entries
            keys_to_remove = [
                key for key, timestamp in self.dedup_cache.items()
                if current_time - timestamp > cache_ttl
            ]
            
            for key in keys_to_remove:
                del self.dedup_cache[key]
            
            logger.debug(f"Cleaned {len(keys_to_remove)} entries from dedup cache")
            
        except Exception as e:
            logger.error(f"Error cleaning dedup cache: {e}")
    
    def force_flush(self):
        """Force flush any pending batch data"""
        with self.batch_lock:
            if self.batch_buffer:
                logger.info(f"Force flushing {len(self.batch_buffer)} pending records")
                self._flush_batch()
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get logging statistics
        
        Returns:
            Dictionary containing logging statistics
        """
        with self.stats_lock:
            return {
                'messages_received': self.stats.messages_received,
                'messages_logged': self.stats.messages_logged,
                'messages_failed': self.stats.messages_failed,
                'validation_errors': self.stats.validation_errors,
                'database_errors': self.stats.database_errors,
                'batch_operations': self.stats.batch_operations,
                'success_rate': (
                    self.stats.messages_logged / self.stats.messages_received * 100
                    if self.stats.messages_received > 0 else 0
                ),
                'last_log_time': self.stats.last_log_time.isoformat() if self.stats.last_log_time else None,
                'pending_batch_size': len(self.batch_buffer),
                'dedup_cache_size': len(self.dedup_cache)
            }
    
    def health_check(self) -> Dict[str, Any]:
        """
        Perform health check
        
        Returns:
            Dictionary containing health check results
        """
        health_info = {
            'is_healthy': False,
            'database_connected': False,
            'validator_ready': False,
            'batch_processing': False,
            'last_activity': None,
            'error': None
        }
        
        try:
            # Check database connection
            db_health = self.db_connection.health_check()
            health_info['database_connected'] = db_health.get('is_connected', False)
            
            # Check validator
            health_info['validator_ready'] = self.validator is not None
            
            # Check batch processing
            with self.batch_lock:
                health_info['batch_processing'] = True
                health_info['pending_records'] = len(self.batch_buffer)
            
            # Check last activity
            with self.stats_lock:
                if self.stats.last_log_time:
                    health_info['last_activity'] = self.stats.last_log_time.isoformat()
                    
                    # Check if we've been inactive for too long
                    time_since_last = (datetime.now() - self.stats.last_log_time).total_seconds()
                    if time_since_last > 3600:  # 1 hour
                        health_info['warning'] = f"No activity for {time_since_last} seconds"
            
            # Overall health
            health_info['is_healthy'] = (
                health_info['database_connected'] and
                health_info['validator_ready'] and
                health_info['batch_processing']
            )
            
        except Exception as e:
            health_info['error'] = str(e)
            logger.error(f"Health check failed: {e}")
        
        return health_info
    
    def shutdown(self):
        """Graceful shutdown - flush any pending data"""
        logger.info("DataLogger shutting down...")
        
        try:
            # Flush any pending batch data
            self.force_flush()
            
            # Clear caches
            self.dedup_cache.clear()
            
            logger.info("DataLogger shutdown complete")
            
        except Exception as e:
            logger.error(f"Error during shutdown: {e}")


def create_data_logger(
    db_connection: DatabaseConnection,
    validator: DataValidator,
    **kwargs
) -> DataLogger:
    """
    Factory function to create data logger
    
    Args:
        db_connection: Database connection instance
        validator: Data validator instance
        **kwargs: Additional logger parameters
        
    Returns:
        DataLogger instance
    """
    return DataLogger(db_connection, validator, **kwargs)