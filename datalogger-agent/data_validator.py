"""
Data Validator module for DataLogger Agent

This module provides functionality to validate and sanitize sensor data
received from RabbitMQ before storing in the database.
"""
import logging
import re
from typing import Dict, Any, List, Optional, Union
from datetime import datetime
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)


class ValidationError(Exception):
    """Custom exception for data validation errors"""
    pass


class ValidationSeverity(Enum):
    """Severity levels for validation issues"""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


@dataclass
class ValidationResult:
    """Result of data validation"""
    is_valid: bool
    errors: List[str]
    warnings: List[str]
    sanitized_data: Optional[Dict[str, Any]] = None
    severity: ValidationSeverity = ValidationSeverity.INFO


class DataValidator:
    """
    Data validator for sensor data
    
    Features:
    - Comprehensive data validation
    - Data sanitization and normalization
    - Configurable validation rules
    - Detailed validation reporting
    - Support for different sensor types
    """
    
    def __init__(self, strict_mode: bool = True, max_value_length: int = 1000):
        """
        Initialize data validator
        
        Args:
            strict_mode: If True, reject data with any validation errors
            max_value_length: Maximum length for string values
        """
        self.strict_mode = strict_mode
        self.max_value_length = max_value_length
        
        # Define expected sensor measurements
        self.expected_measurements = {
            'temperature': {'min': -50.0, 'max': 100.0, 'unit': '°C'},
            'humidity': {'min': 0.0, 'max': 100.0, 'unit': '%'},
            'co2': {'min': 0.0, 'max': 10000.0, 'unit': 'ppm'},
            'pressure': {'min': 800.0, 'max': 1200.0, 'unit': 'hPa'},
            'pm25': {'min': 0.0, 'max': 1000.0, 'unit': 'μg/m³'},
            'pm10': {'min': 0.0, 'max': 1000.0, 'unit': 'μg/m³'},
            'voc': {'min': 0.0, 'max': 10000.0, 'unit': 'ppb'},
            'noise': {'min': 0.0, 'max': 150.0, 'unit': 'dB'}
        }
        
        # Regex patterns for validation
        self.device_id_pattern = re.compile(r'^[a-zA-Z0-9_-]+$')
        self.numeric_pattern = re.compile(r'^-?[0-9]+\.?[0-9]*$')
        
        logger.info(f"DataValidator initialized (strict_mode={strict_mode})")
    
    def validate_sensor_data(self, data: Dict[str, Any]) -> ValidationResult:
        """
        Validate complete sensor data message
        
        Args:
            data: Sensor data dictionary
            
        Returns:
            ValidationResult with validation status and details
        """
        errors = []
        warnings = []
        sanitized_data = {}
        
        try:
            # Validate required fields
            required_fields = ['timestamp', 'datetime', 'device_id', 'measurements']
            for field in required_fields:
                if field not in data:
                    errors.append(f"Missing required field: {field}")
                    continue
                
                # Validate and sanitize each field
                if field == 'timestamp':
                    result = self._validate_timestamp(data[field])
                elif field == 'datetime':
                    result = self._validate_datetime(data[field])
                elif field == 'device_id':
                    result = self._validate_device_id(data[field])
                elif field == 'measurements':
                    result = self._validate_measurements(data[field])
                
                if result['errors']:
                    errors.extend(result['errors'])
                if result['warnings']:
                    warnings.extend(result['warnings'])
                if result['sanitized_value'] is not None:
                    sanitized_data[field] = result['sanitized_value']
            
            # Additional validation checks
            if 'timestamp' in sanitized_data and 'datetime' in sanitized_data:
                timestamp_check = self._validate_timestamp_consistency(
                    sanitized_data['timestamp'], 
                    sanitized_data['datetime']
                )
                if timestamp_check['warnings']:
                    warnings.extend(timestamp_check['warnings'])
            
            # Determine overall validation result
            is_valid = len(errors) == 0 if self.strict_mode else len(errors) == 0
            severity = self._determine_severity(errors, warnings)
            
            # Log validation results
            if errors:
                logger.error(f"Validation failed for device {data.get('device_id', 'unknown')}: {errors}")
            elif warnings:
                logger.warning(f"Validation warnings for device {data.get('device_id', 'unknown')}: {warnings}")
            else:
                logger.debug(f"Validation passed for device {data.get('device_id', 'unknown')}")
            
            return ValidationResult(
                is_valid=is_valid,
                errors=errors,
                warnings=warnings,
                sanitized_data=sanitized_data if is_valid else None,
                severity=severity
            )
            
        except Exception as e:
            logger.error(f"Unexpected error during validation: {e}")
            return ValidationResult(
                is_valid=False,
                errors=[f"Validation error: {str(e)}"],
                warnings=[],
                severity=ValidationSeverity.CRITICAL
            )
    
    def _validate_timestamp(self, timestamp: Any) -> Dict[str, Any]:
        """Validate timestamp field"""
        errors = []
        warnings = []
        sanitized_value = None
        
        try:
            # Convert to integer
            if isinstance(timestamp, str):
                timestamp_int = int(float(timestamp))
            elif isinstance(timestamp, (int, float)):
                timestamp_int = int(timestamp)
            else:
                errors.append(f"Invalid timestamp type: {type(timestamp)}")
                return {'errors': errors, 'warnings': warnings, 'sanitized_value': sanitized_value}
            
            # Validate timestamp range (reasonable range for IoT data)
            min_timestamp = 946684800  # 2000-01-01
            max_timestamp = 4102444800  # 2100-01-01
            
            if timestamp_int < min_timestamp:
                errors.append(f"Timestamp too old: {timestamp_int}")
            elif timestamp_int > max_timestamp:
                errors.append(f"Timestamp too far in future: {timestamp_int}")
            else:
                sanitized_value = timestamp_int
                
                # Check if timestamp is in the future
                current_timestamp = int(datetime.now().timestamp())
                if timestamp_int > current_timestamp + 300:  # 5 minutes tolerance
                    warnings.append(f"Timestamp is in the future: {timestamp_int}")
        
        except (ValueError, TypeError) as e:
            errors.append(f"Invalid timestamp format: {e}")
        
        return {'errors': errors, 'warnings': warnings, 'sanitized_value': sanitized_value}
    
    def _validate_datetime(self, datetime_str: Any) -> Dict[str, Any]:
        """Validate datetime field"""
        errors = []
        warnings = []
        sanitized_value = None
        
        try:
            if not isinstance(datetime_str, str):
                errors.append(f"Datetime must be string, got {type(datetime_str)}")
                return {'errors': errors, 'warnings': warnings, 'sanitized_value': sanitized_value}
            
            # Parse datetime string (support multiple formats)
            datetime_formats = [
                '%Y-%m-%dT%H:%M:%S.%fZ',
                '%Y-%m-%dT%H:%M:%SZ',
                '%Y-%m-%dT%H:%M:%S.%f',
                '%Y-%m-%dT%H:%M:%S',
                '%Y-%m-%d %H:%M:%S.%f',
                '%Y-%m-%d %H:%M:%S'
            ]
            
            parsed_datetime = None
            for fmt in datetime_formats:
                try:
                    parsed_datetime = datetime.strptime(datetime_str.replace('Z', ''), fmt.replace('Z', ''))
                    break
                except ValueError:
                    continue
            
            if parsed_datetime is None:
                # Try ISO format parsing as fallback
                try:
                    parsed_datetime = datetime.fromisoformat(datetime_str.replace('Z', '+00:00'))
                except ValueError:
                    errors.append(f"Invalid datetime format: {datetime_str}")
                    return {'errors': errors, 'warnings': warnings, 'sanitized_value': sanitized_value}
            
            # Validate datetime range
            min_datetime = datetime(2000, 1, 1)
            max_datetime = datetime(2100, 1, 1)
            
            if parsed_datetime < min_datetime:
                errors.append(f"Datetime too old: {datetime_str}")
            elif parsed_datetime > max_datetime:
                errors.append(f"Datetime too far in future: {datetime_str}")
            else:
                # Convert to ISO format with timezone
                sanitized_value = parsed_datetime.isoformat() + 'Z'
                
                # Check if datetime is in the future
                current_datetime = datetime.now()
                if parsed_datetime > current_datetime:
                    time_diff = (parsed_datetime - current_datetime).total_seconds()
                    if time_diff > 300:  # 5 minutes tolerance
                        warnings.append(f"Datetime is in the future: {datetime_str}")
        
        except Exception as e:
            errors.append(f"Error parsing datetime: {e}")
        
        return {'errors': errors, 'warnings': warnings, 'sanitized_value': sanitized_value}
    
    def _validate_device_id(self, device_id: Any) -> Dict[str, Any]:
        """Validate device_id field"""
        errors = []
        warnings = []
        sanitized_value = None
        
        try:
            if not isinstance(device_id, str):
                errors.append(f"Device ID must be string, got {type(device_id)}")
                return {'errors': errors, 'warnings': warnings, 'sanitized_value': sanitized_value}
            
            # Trim whitespace first
            trimmed_device_id = device_id.strip()
            
            # Check length
            if len(trimmed_device_id) == 0:
                errors.append("Device ID cannot be empty")
            elif len(trimmed_device_id) > 255:
                errors.append(f"Device ID too long: {len(trimmed_device_id)} characters")
            else:
                # Validate format on trimmed value
                if not self.device_id_pattern.match(trimmed_device_id):
                    errors.append(f"Invalid device ID format: {device_id}")
                else:
                    sanitized_value = trimmed_device_id
        
        except Exception as e:
            errors.append(f"Error validating device ID: {e}")
        
        return {'errors': errors, 'warnings': warnings, 'sanitized_value': sanitized_value}
    
    def _validate_measurements(self, measurements: Any) -> Dict[str, Any]:
        """Validate measurements field"""
        errors = []
        warnings = []
        sanitized_value = {}
        
        try:
            if not isinstance(measurements, dict):
                errors.append(f"Measurements must be dictionary, got {type(measurements)}")
                return {'errors': errors, 'warnings': warnings, 'sanitized_value': None}
            
            if len(measurements) == 0:
                errors.append("Measurements cannot be empty")
                return {'errors': errors, 'warnings': warnings, 'sanitized_value': None}
            
            # Validate each measurement
            for measurement_name, value in measurements.items():
                measurement_result = self._validate_single_measurement(measurement_name, value)
                
                if measurement_result['errors']:
                    errors.extend(measurement_result['errors'])
                if measurement_result['warnings']:
                    warnings.extend(measurement_result['warnings'])
                if measurement_result['sanitized_value'] is not None:
                    sanitized_value[measurement_name] = measurement_result['sanitized_value']
            
            # Check if we have at least one valid measurement
            if len(sanitized_value) == 0:
                errors.append("No valid measurements found")
                sanitized_value = None
        
        except Exception as e:
            errors.append(f"Error validating measurements: {e}")
            sanitized_value = None
        
        return {'errors': errors, 'warnings': warnings, 'sanitized_value': sanitized_value}
    
    def _validate_single_measurement(self, name: str, value: Any) -> Dict[str, Any]:
        """Validate a single measurement"""
        errors = []
        warnings = []
        sanitized_value = None
        
        try:
            # Validate measurement name
            if not isinstance(name, str) or len(name) == 0:
                errors.append(f"Invalid measurement name: {name}")
                return {'errors': errors, 'warnings': warnings, 'sanitized_value': sanitized_value}
            
            # Convert value to float
            try:
                if isinstance(value, str):
                    if not self.numeric_pattern.match(value):
                        errors.append(f"Invalid numeric format for {name}: {value}")
                        return {'errors': errors, 'warnings': warnings, 'sanitized_value': sanitized_value}
                    float_value = float(value)
                elif isinstance(value, (int, float)):
                    float_value = float(value)
                else:
                    errors.append(f"Invalid value type for {name}: {type(value)}")
                    return {'errors': errors, 'warnings': warnings, 'sanitized_value': sanitized_value}
            except (ValueError, TypeError) as e:
                errors.append(f"Cannot convert {name} value to number: {e}")
                return {'errors': errors, 'warnings': warnings, 'sanitized_value': sanitized_value}
            
            # Check for special float values
            if not self._is_valid_float(float_value):
                errors.append(f"Invalid float value for {name}: {float_value}")
                return {'errors': errors, 'warnings': warnings, 'sanitized_value': sanitized_value}
            
            # Validate against expected ranges
            if name.lower() in self.expected_measurements:
                range_info = self.expected_measurements[name.lower()]
                if float_value < range_info['min']:
                    warnings.append(f"{name} value below expected range: {float_value} < {range_info['min']}")
                elif float_value > range_info['max']:
                    warnings.append(f"{name} value above expected range: {float_value} > {range_info['max']}")
            else:
                warnings.append(f"Unknown measurement type: {name}")
            
            sanitized_value = float_value
        
        except Exception as e:
            errors.append(f"Error validating measurement {name}: {e}")
        
        return {'errors': errors, 'warnings': warnings, 'sanitized_value': sanitized_value}
    
    def _validate_timestamp_consistency(self, timestamp: int, datetime_str: str) -> Dict[str, Any]:
        """Validate consistency between timestamp and datetime"""
        warnings = []
        
        try:
            # Parse datetime
            parsed_datetime = datetime.fromisoformat(datetime_str.replace('Z', '+00:00'))
            datetime_timestamp = int(parsed_datetime.timestamp())
            
            # Check consistency (allow 1 second tolerance)
            time_diff = abs(timestamp - datetime_timestamp)
            if time_diff > 1:
                warnings.append(f"Timestamp and datetime inconsistent: {time_diff} seconds difference")
        
        except Exception as e:
            warnings.append(f"Could not validate timestamp consistency: {e}")
        
        return {'warnings': warnings}
    
    def _is_valid_float(self, value: float) -> bool:
        """Check if float value is valid (not NaN or infinite)"""
        import math
        return not (math.isnan(value) or math.isinf(value))
    
    def _determine_severity(self, errors: List[str], warnings: List[str]) -> ValidationSeverity:
        """Determine overall severity based on errors and warnings"""
        if errors:
            # Check for critical errors
            critical_keywords = ['critical', 'fatal', 'corrupt']
            for error in errors:
                if any(keyword in error.lower() for keyword in critical_keywords):
                    return ValidationSeverity.CRITICAL
            return ValidationSeverity.ERROR
        elif warnings:
            return ValidationSeverity.WARNING
        else:
            return ValidationSeverity.INFO
    
    def sanitize_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Sanitize data by removing/fixing common issues
        
        Args:
            data: Raw data dictionary
            
        Returns:
            Sanitized data dictionary
        """
        try:
            validation_result = self.validate_sensor_data(data)
            
            if validation_result.sanitized_data:
                return validation_result.sanitized_data
            else:
                logger.warning("Data sanitization failed, returning original data")
                return data
        
        except Exception as e:
            logger.error(f"Error during data sanitization: {e}")
            return data
    
    def check_data_integrity(self, data: Dict[str, Any]) -> bool:
        """
        Quick integrity check for data
        
        Args:
            data: Data dictionary to check
            
        Returns:
            True if data passes basic integrity checks
        """
        try:
            # Basic structure check
            required_fields = ['timestamp', 'datetime', 'device_id', 'measurements']
            if not all(field in data for field in required_fields):
                return False
            
            # Check measurements is not empty
            measurements = data.get('measurements', {})
            if not isinstance(measurements, dict) or len(measurements) == 0:
                return False
            
            # Check at least one measurement is numeric
            for value in measurements.values():
                try:
                    float(value)
                    return True  # At least one valid numeric value found
                except (ValueError, TypeError):
                    continue
            
            return False  # No valid numeric measurements found
        
        except Exception as e:
            logger.error(f"Error during integrity check: {e}")
            return False
    
    def get_validation_stats(self) -> Dict[str, Any]:
        """Get validation statistics (placeholder for future implementation)"""
        return {
            'strict_mode': self.strict_mode,
            'max_value_length': self.max_value_length,
            'supported_measurements': list(self.expected_measurements.keys())
        }


def create_data_validator(strict_mode: bool = True, **kwargs) -> DataValidator:
    """
    Factory function to create data validator
    
    Args:
        strict_mode: Enable strict validation mode
        **kwargs: Additional validator parameters
        
    Returns:
        DataValidator instance
    """
    return DataValidator(strict_mode=strict_mode, **kwargs)