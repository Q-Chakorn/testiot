#!/usr/bin/env python3
"""
Main entry point for IAQ Sensor Agent

This script starts the IAQ Sensor Agent that reads sensor data from CSV files
and publishes it to RabbitMQ at regular intervals.
"""
import sys
import os
import logging
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables from .env file (only if running locally)
# In Docker, environment variables are injected by docker-compose
# Don't override Docker environment variables with .env file
if not os.getenv('RABBITMQ_HOST') == 'rabbitmq':
    # We're running locally (not in Docker), load .env file
    try:
        load_dotenv()
    except Exception:
        pass

# Add the current directory to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from scheduler import create_scheduler_from_env, SchedulerError
from rabbitmq_client import RabbitMQClientError
from csv_reader import CSVReaderError


def setup_logging():
    """Setup logging configuration"""
    log_level = os.getenv('LOG_LEVEL', 'INFO').upper()
    log_format = os.getenv('LOG_FORMAT', '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    # Configure root logger
    logging.basicConfig(
        level=getattr(logging, log_level, logging.INFO),
        format=log_format,
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    # Set specific logger levels
    logging.getLogger('pika').setLevel(logging.WARNING)  # Reduce pika verbosity
    
    logger = logging.getLogger(__name__)
    logger.info(f"Logging configured at {log_level} level")
    
    # Debug: Print important environment variables
    logger.debug(f"DEBUG - CSV_DATA_PATH from env: {repr(os.getenv('CSV_DATA_PATH'))}")
    logger.debug(f"DEBUG - DEVICE_ID from env: {repr(os.getenv('DEVICE_ID'))}")
    logger.debug(f"DEBUG - RABBITMQ_HOST from env: {repr(os.getenv('RABBITMQ_HOST'))}")
    
    return logger


def validate_environment():
    """Validate required environment variables"""
    logger = logging.getLogger(__name__)
    
    # Check for multi-file mode or single-file mode
    csv_directory_path = os.getenv('CSV_DIRECTORY_PATH')
    csv_data_path = os.getenv('CSV_DATA_PATH')
    
    if csv_directory_path:
        # Multi-file mode validation
        logger.info("Multi-file mode detected")
        
        if not Path(csv_directory_path).exists():
            logger.error(f"CSV directory not found: {csv_directory_path}")
            return False
        
        if not Path(csv_directory_path).is_dir():
            logger.error(f"CSV_DIRECTORY_PATH is not a directory: {csv_directory_path}")
            return False
        
        # Check if directory contains CSV files
        csv_files = list(Path(csv_directory_path).glob("*.csv"))
        if not csv_files:
            logger.error(f"No CSV files found in directory: {csv_directory_path}")
            return False
        
        logger.info(f"Found {len(csv_files)} CSV files in directory")
        
    elif csv_data_path:
        # Single-file mode validation
        logger.info("Single-file mode detected")
        
        if not os.getenv('DEVICE_ID'):
            logger.error("DEVICE_ID is required for single-file mode")
            return False
        
        if not Path(csv_data_path).exists():
            logger.error(f"CSV file not found: {csv_data_path}")
            return False
        
    else:
        # Default to sample_data directory if available
        default_path = "./sample_data"
        if Path(default_path).exists() and Path(default_path).is_dir():
            logger.info(f"Using default CSV directory: {default_path}")
            os.environ['CSV_DIRECTORY_PATH'] = default_path
        else:
            logger.error("No CSV configuration found. Set CSV_DIRECTORY_PATH or CSV_DATA_PATH")
            return False
    
    # Validate RabbitMQ configuration
    required_rabbitmq_vars = {
        'RABBITMQ_HOST': 'RabbitMQ server host',
        'RABBITMQ_QUEUE': 'RabbitMQ queue name'
    }
    
    missing_vars = []
    for var, description in required_rabbitmq_vars.items():
        if not os.getenv(var):
            missing_vars.append(f"{var} ({description})")
    
    if missing_vars:
        logger.error("Missing required RabbitMQ environment variables:")
        for var in missing_vars:
            logger.error(f"  - {var}")
        return False
    
    logger.info("Environment validation passed")
    return True


def print_startup_banner():
    """Print startup banner with configuration"""
    logger = logging.getLogger(__name__)
    
    banner = """
    ╔══════════════════════════════════════════════════════════════╗
    ║                    IAQ Sensor Agent                         ║
    ║              Indoor Air Quality Data Publisher              ║
    ╚══════════════════════════════════════════════════════════════╝
    """
    
    print(banner)
    
    # Log configuration
    logger.info("IAQ Sensor Agent Configuration:")
    
    # CSV Configuration
    csv_directory_path = os.getenv('CSV_DIRECTORY_PATH')
    csv_data_path = os.getenv('CSV_DATA_PATH')
    
    if csv_directory_path:
        logger.info(f"  Mode: Multi-file")
        logger.info(f"  CSV Directory: {csv_directory_path}")
        logger.info(f"  File Pattern: {os.getenv('CSV_FILE_PATTERN', '*.csv')}")
        logger.info(f"  Reading Mode: {os.getenv('CSV_READING_MODE', 'chronological')}")
        
        # Count CSV files
        try:
            csv_files = list(Path(csv_directory_path).glob("*.csv"))
            logger.info(f"  CSV Files Found: {len(csv_files)}")
            for csv_file in csv_files[:3]:  # Show first 3 files
                logger.info(f"    - {csv_file.name}")
            if len(csv_files) > 3:
                logger.info(f"    ... and {len(csv_files) - 3} more")
        except:
            logger.info(f"  CSV Files: Unable to count")
    else:
        logger.info(f"  Mode: Single-file")
        logger.info(f"  CSV Data Path: {csv_data_path or 'Not set'}")
        logger.info(f"  Device ID: {os.getenv('DEVICE_ID', 'Not set')}")
    
    # Common configuration
    logger.info(f"  Send Interval: {os.getenv('SEND_INTERVAL', '5')}s")
    logger.info(f"  Batch Size: {os.getenv('BATCH_SIZE', '1')}")
    logger.info(f"  RabbitMQ Host: {os.getenv('RABBITMQ_HOST', 'Not set')}")
    logger.info(f"  RabbitMQ Queue: {os.getenv('RABBITMQ_QUEUE', 'Not set')}")
    logger.info(f"  Enable Replay: {os.getenv('ENABLE_REPLAY', 'true')}")
    logger.info(f"  Log Level: {os.getenv('LOG_LEVEL', 'INFO')}")
    logger.info(f"  Started at: {datetime.utcnow().isoformat()}Z")


def main():
    """Main function"""
    # Setup logging first
    logger = setup_logging()
    
    try:
        # Print startup banner
        print_startup_banner()
        
        # Validate environment
        if not validate_environment():
            logger.error("Environment validation failed, exiting")
            return 1
        
        # Create and start scheduler
        logger.info("Creating scheduler...")
        scheduler = create_scheduler_from_env()
        
        logger.info("Starting scheduler...")
        scheduler.start()
        
        # Wait for completion or interruption
        logger.info("IAQ Sensor Agent is running. Press Ctrl+C to stop.")
        scheduler.wait_for_completion()
        
        logger.info("IAQ Sensor Agent stopped successfully")
        return 0
        
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received, shutting down...")
        return 0
        
    except SchedulerError as e:
        logger.error(f"Scheduler error: {e}")
        return 1
        
    except RabbitMQClientError as e:
        logger.error(f"RabbitMQ error: {e}")
        return 1
        
    except CSVReaderError as e:
        logger.error(f"CSV reader error: {e}")
        return 1
        
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return 1


if __name__ == "__main__":
    sys.exit(main())