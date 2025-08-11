"""
Multi-CSV Reader module for IAQ Sensor Agent

This module provides functionality to read sensor data from multiple CSV files
in a directory and convert them to SensorReading objects with proper device mapping.
"""
import csv
import os
import glob
from datetime import datetime
from typing import Iterator, List, Optional, Dict, Any, Tuple
import logging
from pathlib import Path
import re

from models import SensorReading
from csv_reader import CSVReader, CSVReaderError


logger = logging.getLogger(__name__)


class MultiCSVReaderError(Exception):
    """Custom exception for multi-CSV reading errors"""
    pass


class MultiCSVReader:
    """
    Multi-CSV Reader class for reading IAQ sensor data from multiple CSV files
    
    Handles multiple CSV files in a directory, automatically extracting device IDs
    from filenames and managing data from multiple sensors.
    """
    
    def __init__(self, directory_path: str, file_pattern: str = "*.csv"):
        """
        Initialize Multi-CSV Reader
        
        Args:
            directory_path: Path to the directory containing CSV files
            file_pattern: Pattern to match CSV files (default: "*.csv")
        """
        self.directory_path = Path(directory_path)
        self.file_pattern = file_pattern
        self.csv_readers: Dict[str, CSVReader] = {}
        self.device_files: Dict[str, str] = {}
        
        self._validate_directory()
        self._discover_csv_files()
    
    def _validate_directory(self):
        """Validate that the directory exists and is readable"""
        if not self.directory_path.exists():
            raise MultiCSVReaderError(f"Directory not found: {self.directory_path}")
        
        if not self.directory_path.is_dir():
            raise MultiCSVReaderError(f"Path is not a directory: {self.directory_path}")
        
        if not os.access(self.directory_path, os.R_OK):
            raise MultiCSVReaderError(f"Directory is not readable: {self.directory_path}")
    
    def _extract_device_id_from_filename(self, filename: str) -> str:
        """
        Extract device ID from filename
        
        Args:
            filename: Name of the CSV file
            
        Returns:
            Device ID extracted from filename
        """
        # Try to extract room number from filename like "sample_iaq_data_Room101.csv"
        room_match = re.search(r'Room(\d+)', filename, re.IGNORECASE)
        if room_match:
            return f"room_{room_match.group(1).lower()}"
        
        # Try to extract device ID from filename like "device_123.csv"
        device_match = re.search(r'device[_-]?(\w+)', filename, re.IGNORECASE)
        if device_match:
            return f"device_{device_match.group(1).lower()}"
        
        # Try to extract sensor ID from filename like "sensor_abc.csv"
        sensor_match = re.search(r'sensor[_-]?(\w+)', filename, re.IGNORECASE)
        if sensor_match:
            return f"sensor_{sensor_match.group(1).lower()}"
        
        # Fallback: use filename without extension as device ID
        stem = Path(filename).stem
        # Clean up the filename to make it a valid device ID
        device_id = re.sub(r'[^a-zA-Z0-9_-]', '_', stem).lower()
        return device_id
    
    def _discover_csv_files(self):
        """Discover and initialize CSV readers for all CSV files in directory"""
        csv_files = list(self.directory_path.glob(self.file_pattern))
        
        if not csv_files:
            raise MultiCSVReaderError(f"No CSV files found in {self.directory_path} matching pattern {self.file_pattern}")
        
        logger.info(f"Discovered {len(csv_files)} CSV files in {self.directory_path}")
        
        for csv_file in csv_files:
            try:
                # Extract device ID from filename
                device_id = self._extract_device_id_from_filename(csv_file.name)
                
                # Create CSV reader for this file
                csv_reader = CSVReader(str(csv_file), device_id)
                
                # Store the reader and file mapping
                self.csv_readers[device_id] = csv_reader
                self.device_files[device_id] = str(csv_file)
                
                logger.info(f"Initialized CSV reader for device '{device_id}' from file '{csv_file.name}'")
                
            except CSVReaderError as e:
                logger.error(f"Failed to initialize CSV reader for {csv_file}: {e}")
                # Continue with other files instead of failing completely
                continue
        
        if not self.csv_readers:
            raise MultiCSVReaderError("No valid CSV files could be initialized")
        
        logger.info(f"Successfully initialized {len(self.csv_readers)} CSV readers")
    
    def get_device_ids(self) -> List[str]:
        """
        Get list of all device IDs
        
        Returns:
            List of device IDs
        """
        return list(self.csv_readers.keys())
    
    def get_file_info(self) -> Dict[str, Dict[str, Any]]:
        """
        Get information about all CSV files
        
        Returns:
            Dictionary mapping device IDs to file information
        """
        file_info = {}
        for device_id, csv_reader in self.csv_readers.items():
            try:
                info = csv_reader.get_file_info()
                file_info[device_id] = info
            except Exception as e:
                logger.error(f"Error getting file info for device {device_id}: {e}")
                file_info[device_id] = {'error': str(e)}
        
        return file_info
    
    def read_all_devices(self) -> Dict[str, List[SensorReading]]:
        """
        Read all sensor readings from all devices
        
        Returns:
            Dictionary mapping device IDs to lists of SensorReading objects
        """
        all_readings = {}
        
        for device_id, csv_reader in self.csv_readers.items():
            try:
                readings = csv_reader.read_all()
                all_readings[device_id] = readings
                logger.info(f"Read {len(readings)} readings from device {device_id}")
            except CSVReaderError as e:
                logger.error(f"Error reading from device {device_id}: {e}")
                all_readings[device_id] = []
        
        return all_readings
    
    def read_all_combined(self) -> List[SensorReading]:
        """
        Read all sensor readings from all devices combined into a single list
        
        Returns:
            Combined list of SensorReading objects from all devices
        """
        combined_readings = []
        
        for device_id, csv_reader in self.csv_readers.items():
            try:
                readings = csv_reader.read_all()
                combined_readings.extend(readings)
                logger.info(f"Added {len(readings)} readings from device {device_id}")
            except CSVReaderError as e:
                logger.error(f"Error reading from device {device_id}: {e}")
                continue
        
        # Sort by timestamp to maintain chronological order
        combined_readings.sort(key=lambda x: x.timestamp)
        
        logger.info(f"Combined {len(combined_readings)} readings from {len(self.csv_readers)} devices")
        return combined_readings
    
    def read_iterator_combined(self) -> Iterator[SensorReading]:
        """
        Read sensor readings from all devices as a combined iterator
        
        This method reads from all files simultaneously and yields readings
        in chronological order (sorted by timestamp).
        
        Yields:
            SensorReading objects in chronological order
        """
        # Collect all readings first (for sorting)
        all_readings = []
        
        for device_id, csv_reader in self.csv_readers.items():
            try:
                for reading in csv_reader.read_iterator():
                    all_readings.append(reading)
            except CSVReaderError as e:
                logger.error(f"Error reading from device {device_id}: {e}")
                continue
        
        # Sort by timestamp
        all_readings.sort(key=lambda x: x.timestamp)
        
        # Yield in chronological order
        for reading in all_readings:
            yield reading
    
    def read_iterator_round_robin(self) -> Iterator[SensorReading]:
        """
        Read sensor readings using round-robin approach across all devices
        
        This method alternates between devices to provide more balanced
        data distribution when processing.
        
        Yields:
            SensorReading objects in round-robin order
        """
        # Create iterators for each device
        device_iterators = {}
        for device_id, csv_reader in self.csv_readers.items():
            try:
                device_iterators[device_id] = csv_reader.read_iterator()
            except CSVReaderError as e:
                logger.error(f"Error creating iterator for device {device_id}: {e}")
                continue
        
        if not device_iterators:
            return
        
        # Round-robin through devices
        device_ids = list(device_iterators.keys())
        current_device_index = 0
        
        while device_iterators:
            device_id = device_ids[current_device_index % len(device_ids)]
            
            try:
                # Try to get next reading from current device
                reading = next(device_iterators[device_id])
                yield reading
                
                # Move to next device
                current_device_index += 1
                
            except StopIteration:
                # This device is exhausted, remove it
                del device_iterators[device_id]
                device_ids.remove(device_id)
                
                # Adjust index if needed
                if device_ids and current_device_index >= len(device_ids):
                    current_device_index = 0
            
            except Exception as e:
                logger.error(f"Error reading from device {device_id}: {e}")
                # Remove problematic device
                if device_id in device_iterators:
                    del device_iterators[device_id]
                if device_id in device_ids:
                    device_ids.remove(device_id)
    
    def get_summary(self) -> Dict[str, Any]:
        """
        Get summary information about all CSV files
        
        Returns:
            Summary dictionary with statistics
        """
        file_info = self.get_file_info()
        
        total_records = 0
        total_size_mb = 0
        device_count = len(self.csv_readers)
        
        for device_id, info in file_info.items():
            if 'error' not in info:
                total_records += info.get('estimated_records', 0)
                total_size_mb += info.get('file_size_mb', 0)
        
        return {
            'directory_path': str(self.directory_path),
            'device_count': device_count,
            'total_estimated_records': total_records,
            'total_size_mb': round(total_size_mb, 2),
            'devices': list(self.csv_readers.keys()),
            'file_pattern': self.file_pattern
        }


def create_multi_csv_reader(directory_path: str, file_pattern: str = "*.csv") -> MultiCSVReader:
    """
    Factory function to create a multi-CSV reader instance
    
    Args:
        directory_path: Path to directory containing CSV files
        file_pattern: Pattern to match CSV files
        
    Returns:
        MultiCSVReader instance
        
    Raises:
        MultiCSVReaderError: If directory validation fails
    """
    return MultiCSVReader(directory_path, file_pattern)


def validate_csv_directory(directory_path: str, file_pattern: str = "*.csv") -> Dict[str, Any]:
    """
    Validate CSV directory and return validation results
    
    Args:
        directory_path: Path to directory containing CSV files
        file_pattern: Pattern to match CSV files
        
    Returns:
        Dictionary containing validation results
    """
    validation_result = {
        'is_valid': False,
        'errors': [],
        'warnings': [],
        'summary': {}
    }
    
    try:
        # Create temporary reader to validate directory
        temp_reader = MultiCSVReader(directory_path, file_pattern)
        
        # Get summary information
        validation_result['summary'] = temp_reader.get_summary()
        validation_result['is_valid'] = True
        
        # Check if we have any devices
        if validation_result['summary']['device_count'] == 0:
            validation_result['errors'].append("No valid CSV files found in directory")
            validation_result['is_valid'] = False
        
    except MultiCSVReaderError as e:
        validation_result['errors'].append(str(e))
    except Exception as e:
        validation_result['errors'].append(f"Unexpected error: {e}")
    
    return validation_result