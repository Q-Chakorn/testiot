"""
Data models for IAQ Sensor Agent
"""
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Any
import time


@dataclass
class SensorReading:
    """
    Model for sensor reading data from IAQ sensors
    
    Attributes:
        timestamp: Unix timestamp
        datetime: Datetime object
        device_id: Device identifier
        measurements: Dictionary containing sensor measurements (temperature, humidity, co2)
    """
    timestamp: int
    datetime: datetime
    device_id: str
    measurements: Dict[str, float]
    
    def __post_init__(self):
        """Validate data after initialization"""
        self._validate_data()
    
    def _validate_data(self):
        """Validate sensor reading data"""
        # Check required measurement fields
        required_measurements = ['temperature', 'humidity', 'co2']
        
        if not isinstance(self.measurements, dict):
            raise ValueError("measurements must be a dictionary")
        
        for field in required_measurements:
            if field not in self.measurements:
                raise ValueError(f"Missing required measurement field: {field}")
            
            # Validate data types and ranges
            value = self.measurements[field]
            if not isinstance(value, (int, float)):
                raise ValueError(f"Measurement {field} must be a number, got {type(value)}")
            
            # Basic range validation
            if field == 'temperature' and not (-50 <= value <= 100):
                raise ValueError(f"Temperature {value}°C is out of valid range (-50 to 100°C)")
            elif field == 'humidity' and not (0 <= value <= 100):
                raise ValueError(f"Humidity {value}% is out of valid range (0 to 100%)")
            elif field == 'co2' and not (0 <= value <= 10000):
                raise ValueError(f"CO2 {value}ppm is out of valid range (0 to 10000ppm)")
        
        # Validate timestamp and datetime consistency
        if not isinstance(self.timestamp, int):
            raise ValueError("timestamp must be an integer")
        
        if not isinstance(self.datetime, datetime):
            raise ValueError("datetime must be a datetime object")
        
        if not isinstance(self.device_id, str) or not self.device_id.strip():
            raise ValueError("device_id must be a non-empty string")
    
    def to_raw_data_records(self) -> List[Dict[str, Any]]:
        """
        Convert sensor reading to raw_data table format
        
        Returns:
            List of dictionaries, one for each measurement
        """
        records = []
        for datapoint, value in self.measurements.items():
            records.append({
                'timestamp': self.timestamp,
                'datetime': self.datetime,
                'device_id': self.device_id,
                'datapoint': datapoint,
                'value': str(value)
            })
        return records
    
    def to_json_message(self) -> Dict[str, Any]:
        """
        Convert sensor reading to JSON message format for RabbitMQ
        
        Returns:
            Dictionary in the format expected by the message queue
        """
        return {
            'timestamp': self.timestamp,
            'datetime': self.datetime.isoformat() + 'Z',
            'device_id': self.device_id,
            'measurements': self.measurements
        }
    
    @classmethod
    def from_csv_row(cls, row: Dict[str, str], device_id: str) -> 'SensorReading':
        """
        Create SensorReading from CSV row data
        
        Args:
            row: Dictionary containing CSV row data
            device_id: Device identifier
            
        Returns:
            SensorReading instance
        """
        # Parse datetime from CSV format
        datetime_str = row['datetime']
        dt = datetime.strptime(datetime_str, '%Y-%m-%d %H:%M:%S.%f')
        
        # Convert to timestamp
        timestamp = int(dt.timestamp())
        
        # Extract measurements
        measurements = {
            'temperature': float(row['temperature']),
            'humidity': float(row['humidity']),
            'co2': float(row['co2'])
        }
        
        return cls(
            timestamp=timestamp,
            datetime=dt,
            device_id=device_id,
            measurements=measurements
        )


def validate_sensor_data(data: Dict) -> bool:
    """
    Validate sensor data dictionary format
    
    Args:
        data: Dictionary containing sensor data
        
    Returns:
        True if valid, False otherwise
    """
    try:
        required_fields = ['timestamp', 'datetime', 'device_id', 'measurements']
        measurement_fields = ['temperature', 'humidity', 'co2']
        
        # Check required fields
        if not all(field in data for field in required_fields):
            return False
        
        # Check measurement fields
        measurements = data.get('measurements', {})
        if not all(field in measurements for field in measurement_fields):
            return False
        
        # Check data types
        for field in measurement_fields:
            try:
                float(measurements[field])
            except (ValueError, TypeError):
                return False
        
        return True
    except Exception:
        return False