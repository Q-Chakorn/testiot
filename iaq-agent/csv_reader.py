"""
CSV Reader module for IAQ Sensor Agent

This module provides functionality to read sensor data from CSV files
and convert them to SensorReading objects with proper datetime handling.
"""
import csv
import os
from datetime import datetime
from typing import Iterator, List, Optional, Dict, Any
import logging
from pathlib import Path

from models import SensorReading


logger = logging.getLogger(__name__)


class CSVReaderError(Exception):
    """Custom exception for CSV reading errors"""
    pass


class CSVReader:
    """
    CSV Reader class for reading IAQ sensor data from CSV files
    
    Handles datetime parsing, data validation, and conversion to SensorReading objects.
    """
    
    def __init__(self, file_path: str, device_id: str):
        """
        Initialize CSV Reader
        
        Args:
            file_path: Path to the CSV file
            device_id: Device identifier for the sensor readings
        """
        self.file_path = Path(file_path)
        self.device_id = device_id
        self._validate_file()
    
    def _validate_file(self):
        """Validate that the CSV file exists and is readable"""
        if not self.file_path.exists():
            raise CSVReaderError(f"CSV file not found: {self.file_path}")
        
        if not self.file_path.is_file():
            raise CSVReaderError(f"Path is not a file: {self.file_path}")
        
        if not os.access(self.file_path, os.R_OK):
            raise CSVReaderError(f"CSV file is not readable: {self.file_path}")
    
    def _parse_datetime(self, datetime_str: str) -> datetime:
        """
        Parse datetime string from CSV format
        
        Args:
            datetime_str: Datetime string in format 'YYYY-MM-DD HH:MM:SS.ff'
            
        Returns:
            datetime object
            
        Raises:
            CSVReaderError: If datetime format is invalid
        """
        try:
            # Handle the CSV format: '2024-12-27 00:00:00.00'
            return datetime.strptime(datetime_str, '%Y-%m-%d %H:%M:%S.%f')
        except ValueError as e:
            # Try alternative format without microseconds
            try:
                return datetime.strptime(datetime_str, '%Y-%m-%d %H:%M:%S')
            except ValueError:
                raise CSVReaderError(f"Invalid datetime format: {datetime_str}. Expected format: YYYY-MM-DD HH:MM:SS.ff") from e
    
    def _validate_csv_row(self, row: Dict[str, str], line_number: int) -> None:
        """
        Validate CSV row data
        
        Args:
            row: Dictionary containing CSV row data
            line_number: Line number for error reporting
            
        Raises:
            CSVReaderError: If row data is invalid
        """
        required_columns = ['datetime', 'temperature', 'humidity', 'co2']
        
        # Check for required columns
        missing_columns = [col for col in required_columns if col not in row or not row[col].strip()]
        if missing_columns:
            raise CSVReaderError(f"Line {line_number}: Missing or empty columns: {missing_columns}")
        
        # Validate numeric values
        numeric_columns = ['temperature', 'humidity', 'co2']
        for col in numeric_columns:
            try:
                value = float(row[col])
                
                # Basic range validation
                if col == 'temperature' and not (-50 <= value <= 100):
                    logger.warning(f"Line {line_number}: Temperature {value}°C is out of typical range (-50 to 100°C)")
                elif col == 'humidity' and not (0 <= value <= 100):
                    logger.warning(f"Line {line_number}: Humidity {value}% is out of valid range (0 to 100%)")
                elif col == 'co2' and not (0 <= value <= 10000):
                    logger.warning(f"Line {line_number}: CO2 {value}ppm is out of typical range (0 to 10000ppm)")
                    
            except ValueError as e:
                raise CSVReaderError(f"Line {line_number}: Invalid numeric value for {col}: {row[col]}") from e
    
    def read_all(self) -> List[SensorReading]:
        """
        Read all sensor readings from the CSV file
        
        Returns:
            List of SensorReading objects
            
        Raises:
            CSVReaderError: If file reading or data parsing fails
        """
        readings = []
        
        try:
            with open(self.file_path, 'r', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                
                # Validate CSV headers
                expected_headers = {'datetime', 'temperature', 'humidity', 'co2'}
                actual_headers = set(reader.fieldnames or [])
                
                if not expected_headers.issubset(actual_headers):
                    missing_headers = expected_headers - actual_headers
                    raise CSVReaderError(f"Missing required CSV headers: {missing_headers}")
                
                for line_number, row in enumerate(reader, start=2):  # Start at 2 because header is line 1
                    try:
                        self._validate_csv_row(row, line_number)
                        
                        # Parse datetime
                        dt = self._parse_datetime(row['datetime'])
                        
                        # Create measurements dictionary
                        measurements = {
                            'temperature': float(row['temperature']),
                            'humidity': float(row['humidity']),
                            'co2': float(row['co2'])
                        }
                        
                        # Create SensorReading object
                        reading = SensorReading(
                            timestamp=int(dt.timestamp()),
                            datetime=dt,
                            device_id=self.device_id,
                            measurements=measurements
                        )
                        
                        readings.append(reading)
                        
                    except (CSVReaderError, ValueError) as e:
                        logger.error(f"Error processing line {line_number}: {e}")
                        # Continue processing other lines instead of failing completely
                        continue
                        
        except IOError as e:
            raise CSVReaderError(f"Error reading CSV file {self.file_path}: {e}") from e
        
        logger.info(f"Successfully read {len(readings)} sensor readings from {self.file_path}")
        return readings
    
    def read_iterator(self) -> Iterator[SensorReading]:
        """
        Read sensor readings as an iterator (memory efficient for large files)
        
        Yields:
            SensorReading objects one at a time
            
        Raises:
            CSVReaderError: If file reading or data parsing fails
        """
        try:
            with open(self.file_path, 'r', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                
                # Validate CSV headers
                expected_headers = {'datetime', 'temperature', 'humidity', 'co2'}
                actual_headers = set(reader.fieldnames or [])
                
                if not expected_headers.issubset(actual_headers):
                    missing_headers = expected_headers - actual_headers
                    raise CSVReaderError(f"Missing required CSV headers: {missing_headers}")
                
                for line_number, row in enumerate(reader, start=2):
                    try:
                        self._validate_csv_row(row, line_number)
                        
                        # Parse datetime
                        dt = self._parse_datetime(row['datetime'])
                        
                        # Create measurements dictionary
                        measurements = {
                            'temperature': float(row['temperature']),
                            'humidity': float(row['humidity']),
                            'co2': float(row['co2'])
                        }
                        
                        # Create SensorReading object
                        reading = SensorReading(
                            timestamp=int(dt.timestamp()),
                            datetime=dt,
                            device_id=self.device_id,
                            measurements=measurements
                        )
                        
                        yield reading
                        
                    except (CSVReaderError, ValueError) as e:
                        logger.error(f"Error processing line {line_number}: {e}")
                        # Continue processing other lines
                        continue
                        
        except IOError as e:
            raise CSVReaderError(f"Error reading CSV file {self.file_path}: {e}") from e
    
    def get_file_info(self) -> Dict[str, Any]:
        """
        Get information about the CSV file
        
        Returns:
            Dictionary containing file information
        """
        try:
            stat = self.file_path.stat()
            
            # Count lines in file
            with open(self.file_path, 'r', encoding='utf-8') as f:
                line_count = sum(1 for _ in f) - 1  # Subtract 1 for header
            
            return {
                'file_path': str(self.file_path),
                'file_size_bytes': stat.st_size,
                'file_size_mb': round(stat.st_size / (1024 * 1024), 2),
                'last_modified': datetime.fromtimestamp(stat.st_mtime),
                'estimated_records': line_count,
                'device_id': self.device_id
            }
        except Exception as e:
            logger.error(f"Error getting file info: {e}")
            return {'error': str(e)}


def create_csv_reader(file_path: str, device_id: str) -> CSVReader:
    """
    Factory function to create a CSV reader instance
    
    Args:
        file_path: Path to the CSV file
        device_id: Device identifier
        
    Returns:
        CSVReader instance
        
    Raises:
        CSVReaderError: If file validation fails
    """
    return CSVReader(file_path, device_id)


def read_csv_file(file_path: str, device_id: str) -> List[SensorReading]:
    """
    Convenience function to read all data from a CSV file
    
    Args:
        file_path: Path to the CSV file
        device_id: Device identifier
        
    Returns:
        List of SensorReading objects
        
    Raises:
        CSVReaderError: If file reading fails
    """
    reader = create_csv_reader(file_path, device_id)
    return reader.read_all()


def validate_csv_file(file_path: str) -> Dict[str, Any]:
    """
    Validate CSV file format and return validation results
    
    Args:
        file_path: Path to the CSV file
        
    Returns:
        Dictionary containing validation results
    """
    validation_result = {
        'is_valid': False,
        'errors': [],
        'warnings': [],
        'file_info': {}
    }
    
    try:
        # Create a temporary reader to validate file
        temp_reader = CSVReader(file_path, 'validation_device')
        validation_result['file_info'] = temp_reader.get_file_info()
        
        # Try to read first few rows to validate format
        sample_readings = []
        for i, reading in enumerate(temp_reader.read_iterator()):
            sample_readings.append(reading)
            if i >= 2:  # Read first 3 rows for validation
                break
        
        if sample_readings:
            validation_result['is_valid'] = True
            validation_result['sample_count'] = len(sample_readings)
            validation_result['first_reading'] = {
                'datetime': sample_readings[0].datetime.isoformat(),
                'measurements': sample_readings[0].measurements
            }
        else:
            validation_result['errors'].append("No valid data rows found in CSV file")
            
    except CSVReaderError as e:
        validation_result['errors'].append(str(e))
    except Exception as e:
        validation_result['errors'].append(f"Unexpected error: {e}")
    
    return validation_result