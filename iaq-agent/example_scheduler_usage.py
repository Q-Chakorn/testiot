#!/usr/bin/env python3
"""
Example usage of Data Scheduler for IAQ Sensor Agent

This script demonstrates how to use the scheduler to coordinate
CSV reading and RabbitMQ publishing.
"""
import os
import sys
import time
from datetime import datetime
import logging
import tempfile

# Add the current directory to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from scheduler import (
    create_scheduler,
    create_scheduler_from_env,
    SchedulerConfig,
    DataScheduler,
    SchedulerError
)
from rabbitmq_client import create_rabbitmq_client, RabbitMQConfig

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


def create_sample_csv_file():
    """Create a sample CSV file for demonstration"""
    csv_content = """datetime,temperature,humidity,co2
2024-12-27 00:00:00.00,24.5,55.0,488.8
2024-12-27 00:01:00.00,25.4,55.5,496.0
2024-12-27 00:02:00.00,25.8,54.8,482.7
2024-12-27 00:03:00.00,26.3,53.6,475.9
2024-12-27 00:04:00.00,27.1,53.5,473.2
2024-12-27 00:05:00.00,27.1,51.5,486.7
2024-12-27 00:06:00.00,27.1,51.6,506.0
2024-12-27 00:07:00.00,27.3,50.3,497.5
2024-12-27 00:08:00.00,27.7,50.6,500.6
2024-12-27 00:09:00.00,28.0,49.1,514.1
"""
    
    # Create temporary file
    temp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False)
    temp_file.write(csv_content)
    temp_file.close()
    
    return temp_file.name


def example_basic_scheduler():
    """Example: Basic scheduler usage"""
    print("=== Basic Scheduler Usage Example ===")
    
    # Create sample CSV file
    csv_file = create_sample_csv_file()
    
    try:
        # Create scheduler with basic configuration
        scheduler = create_scheduler(
            csv_file_path=csv_file,
            device_id='example_room_101',
            send_interval=2,  # Send every 2 seconds
            batch_size=1,
            enable_replay=True,
            log_level='INFO'
        )
        
        print("✓ Scheduler created successfully")
        
        # Get initial status
        status = scheduler.get_status()
        print(f"  Device ID: {status['config']['device_id']}")
        print(f"  Send interval: {status['config']['send_interval']}s")
        print(f"  CSV file: {status['config']['csv_file_path']}")
        print(f"  Replay enabled: {status['config']['enable_replay']}")
        
        # Note: In this example, we don't actually start the scheduler
        # because it would require a real RabbitMQ connection
        print("✓ Basic scheduler configuration completed")
        
        return True
        
    except Exception as e:
        print(f"✗ Error in basic scheduler example: {e}")
        return False
        
    finally:
        # Clean up temporary file
        os.unlink(csv_file)


def example_environment_configuration():
    """Example: Using environment variables for configuration"""
    print("\n=== Environment Configuration Example ===")
    
    csv_file = create_sample_csv_file()
    
    # Set environment variables for testing
    test_env = {
        'CSV_DATA_PATH': csv_file,
        'DEVICE_ID': 'env_example_room',
        'SEND_INTERVAL': '3',
        'BATCH_SIZE': '2',
        'MAX_RETRIES': '5',
        'RETRY_DELAY': '1',
        'ENABLE_REPLAY': 'true',
        'LOG_LEVEL': 'DEBUG',
        'STATS_INTERVAL': '30',
        'RABBITMQ_HOST': 'localhost',
        'RABBITMQ_PORT': '5672',
        'RABBITMQ_USERNAME': 'guest',
        'RABBITMQ_PASSWORD': 'guest',
        'RABBITMQ_QUEUE': 'env_test_queue'
    }
    
    # Temporarily set environment variables
    original_env = {}
    for key, value in test_env.items():
        original_env[key] = os.environ.get(key)
        os.environ[key] = value
    
    try:
        # Create scheduler from environment
        scheduler = create_scheduler_from_env()
        
        print("✓ Scheduler created from environment variables")
        
        # Verify configuration
        status = scheduler.get_status()
        config = status['config']
        
        print(f"  CSV file: {config['csv_file_path']}")
        print(f"  Device ID: {config['device_id']}")
        print(f"  Send interval: {config['send_interval']}s")
        print(f"  Batch size: {config['batch_size']}")
        print(f"  Replay enabled: {config['enable_replay']}")
        
        # Verify RabbitMQ configuration
        rabbitmq_info = status['rabbitmq_connection']
        print(f"  RabbitMQ host: {rabbitmq_info['host']}")
        print(f"  RabbitMQ port: {rabbitmq_info['port']}")
        print(f"  RabbitMQ queue: {rabbitmq_info['queue']}")
        
        print("✓ Environment configuration applied correctly")
        
        return True
        
    except Exception as e:
        print(f"✗ Error in environment configuration example: {e}")
        return False
        
    finally:
        # Restore original environment variables
        for key, value in original_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        
        # Clean up temporary file
        os.unlink(csv_file)


def example_scheduler_with_mock_rabbitmq():
    """Example: Running scheduler with mocked RabbitMQ for demonstration"""
    print("\n=== Scheduler with Mock RabbitMQ Example ===")
    
    csv_file = create_sample_csv_file()
    
    try:
        from unittest.mock import Mock
        
        # Create mock RabbitMQ client
        mock_rabbitmq = Mock()
        mock_rabbitmq.connect.return_value = True
        mock_rabbitmq.disconnect.return_value = None
        mock_rabbitmq.send_sensor_reading.return_value = True
        mock_rabbitmq.is_connection_healthy.return_value = True
        mock_rabbitmq.get_connection_info.return_value = {
            'host': 'mock_host',
            'port': 5672,
            'queue': 'mock_queue',
            'is_connected': True
        }
        
        # Create scheduler configuration
        config = SchedulerConfig(
            csv_file_path=csv_file,
            device_id='mock_example_room',
            send_interval=1,  # 1 second for quick demo
            batch_size=2,
            enable_replay=True,
            stats_interval=5,  # Log stats every 5 seconds
            log_level='INFO'
        )
        
        # Create scheduler with mock RabbitMQ
        scheduler = DataScheduler(config, mock_rabbitmq)
        
        print("✓ Scheduler created with mock RabbitMQ client")
        
        # Start scheduler
        print("✓ Starting scheduler for 10 seconds...")
        scheduler.start()
        
        # Let it run for a short demonstration
        for i in range(10):
            time.sleep(1)
            status = scheduler.get_status()
            print(f"  Running... Messages sent: {status['statistics']['messages_sent']}, "
                  f"CSV rows: {status['statistics']['csv_rows_processed']}")
        
        # Stop scheduler
        print("✓ Stopping scheduler...")
        scheduler.stop()
        
        # Show final statistics
        final_status = scheduler.get_status()
        stats = final_status['statistics']
        
        print("✓ Scheduler demonstration completed")
        print(f"  Final statistics:")
        print(f"    Messages sent: {stats['messages_sent']}")
        print(f"    Messages failed: {stats['messages_failed']}")
        print(f"    CSV rows processed: {stats['csv_rows_processed']}")
        print(f"    CSV replays: {stats['replay_count']}")
        print(f"    Uptime: {stats['uptime_seconds']:.1f}s")
        
        # Verify mock calls
        print(f"  Mock RabbitMQ calls:")
        print(f"    Connect called: {mock_rabbitmq.connect.call_count} times")
        print(f"    Send message called: {mock_rabbitmq.send_sensor_reading.call_count} times")
        print(f"    Disconnect called: {mock_rabbitmq.disconnect.call_count} times")
        
        return True
        
    except Exception as e:
        print(f"✗ Error in mock RabbitMQ example: {e}")
        return False
        
    finally:
        os.unlink(csv_file)


def example_scheduler_error_scenarios():
    """Example: Error handling scenarios"""
    print("\n=== Error Handling Scenarios Example ===")
    
    # Scenario 1: Non-existent CSV file
    print("Scenario 1: Non-existent CSV file")
    try:
        scheduler = create_scheduler(
            csv_file_path='non_existent_file.csv',
            device_id='error_test'
        )
        print("✗ Should have failed with non-existent file")
        return False
    except SchedulerError as e:
        print(f"✓ Correctly caught error: {e}")
    
    # Scenario 2: Invalid configuration
    print("\nScenario 2: Invalid send interval")
    csv_file = create_sample_csv_file()
    
    try:
        config = SchedulerConfig(
            csv_file_path=csv_file,
            device_id='config_test',
            send_interval=0,  # Invalid interval
            batch_size=-1     # Invalid batch size
        )
        
        # The scheduler should still create but may have issues during operation
        scheduler = DataScheduler(config)
        print("✓ Scheduler created with invalid config (will handle during runtime)")
        
        return True
        
    except Exception as e:
        print(f"✓ Configuration error handled: {e}")
        return True
        
    finally:
        os.unlink(csv_file)


def example_scheduler_status_monitoring():
    """Example: Status monitoring and statistics"""
    print("\n=== Status Monitoring Example ===")
    
    csv_file = create_sample_csv_file()
    
    try:
        from unittest.mock import Mock
        
        # Create mock RabbitMQ with some failures
        mock_rabbitmq = Mock()
        mock_rabbitmq.connect.return_value = True
        mock_rabbitmq.disconnect.return_value = None
        mock_rabbitmq.is_connection_healthy.return_value = True
        
        # Simulate some send failures
        call_count = 0
        def mock_send_with_failures(reading):
            nonlocal call_count
            call_count += 1
            return call_count % 4 != 0  # Fail every 4th message
        
        mock_rabbitmq.send_sensor_reading.side_effect = mock_send_with_failures
        mock_rabbitmq.get_connection_info.return_value = {
            'host': 'monitor_host',
            'port': 5672,
            'queue': 'monitor_queue',
            'is_connected': True
        }
        
        config = SchedulerConfig(
            csv_file_path=csv_file,
            device_id='monitor_example',
            send_interval=0.5,  # Fast for demonstration
            batch_size=1,
            stats_interval=2,   # Log stats every 2 seconds
            max_retries=1,      # Quick retries
            retry_delay=0.1
        )
        
        scheduler = DataScheduler(config, mock_rabbitmq)
        
        print("✓ Starting scheduler with monitoring...")
        scheduler.start()
        
        # Monitor for several intervals
        for i in range(6):
            time.sleep(1)
            status = scheduler.get_status()
            stats = status['statistics']
            
            success_rate = (stats['messages_sent'] / 
                          (stats['messages_sent'] + stats['messages_failed']) * 100
                          if (stats['messages_sent'] + stats['messages_failed']) > 0 else 0)
            
            print(f"  Monitor {i+1}: Sent: {stats['messages_sent']}, "
                  f"Failed: {stats['messages_failed']}, "
                  f"Success rate: {success_rate:.1f}%")
        
        scheduler.stop()
        
        # Final status report
        final_status = scheduler.get_status()
        final_stats = final_status['statistics']
        
        print("✓ Monitoring demonstration completed")
        print(f"  Final metrics:")
        print(f"    Total messages sent: {final_stats['messages_sent']}")
        print(f"    Total messages failed: {final_stats['messages_failed']}")
        print(f"    CSV rows processed: {final_stats['csv_rows_processed']}")
        print(f"    Total uptime: {final_stats['uptime_seconds']:.1f}s")
        
        return True
        
    except Exception as e:
        print(f"✗ Error in monitoring example: {e}")
        return False
        
    finally:
        os.unlink(csv_file)


def example_production_like_setup():
    """Example: Production-like setup with proper configuration"""
    print("\n=== Production-like Setup Example ===")
    
    csv_file = create_sample_csv_file()
    
    # Production-like environment variables
    production_env = {
        'CSV_DATA_PATH': csv_file,
        'DEVICE_ID': 'production_room_sensor_001',
        'SEND_INTERVAL': '5',        # Standard 5-second interval
        'BATCH_SIZE': '1',           # One reading at a time
        'MAX_RETRIES': '3',          # Reasonable retry count
        'RETRY_DELAY': '2',          # 2-second retry delay
        'ENABLE_REPLAY': 'true',     # Enable continuous operation
        'LOG_LEVEL': 'INFO',         # Production logging level
        'STATS_INTERVAL': '300',     # Log stats every 5 minutes
        'RABBITMQ_HOST': 'rabbitmq.production.com',
        'RABBITMQ_PORT': '5672',
        'RABBITMQ_USERNAME': 'iaq_agent',
        'RABBITMQ_PASSWORD': 'secure_password',
        'RABBITMQ_QUEUE': 'sensor_data_production',
        'RABBITMQ_EXCHANGE': 'sensor_exchange',
        'RABBITMQ_DURABLE': 'true',
        'RABBITMQ_HEARTBEAT': '600',
        'RABBITMQ_CONNECTION_TIMEOUT': '30'
    }
    
    # Temporarily set environment variables
    original_env = {}
    for key, value in production_env.items():
        original_env[key] = os.environ.get(key)
        os.environ[key] = value
    
    try:
        # Create production-like scheduler
        scheduler = create_scheduler_from_env()
        
        print("✓ Production-like scheduler created")
        
        # Show configuration
        status = scheduler.get_status()
        config = status['config']
        rabbitmq_info = status['rabbitmq_connection']
        
        print(f"  Production Configuration:")
        print(f"    Device ID: {config['device_id']}")
        print(f"    Send interval: {config['send_interval']}s")
        print(f"    Batch size: {config['batch_size']}")
        print(f"    Replay enabled: {config['enable_replay']}")
        print(f"    RabbitMQ host: {rabbitmq_info['host']}")
        print(f"    RabbitMQ queue: {rabbitmq_info['queue']}")
        print(f"    RabbitMQ exchange: {rabbitmq_info['exchange']}")
        
        print("✓ Production configuration validated")
        print("  Note: This would connect to real RabbitMQ in production")
        
        return True
        
    except Exception as e:
        print(f"✗ Error in production setup example: {e}")
        return False
        
    finally:
        # Restore original environment
        for key, value in original_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        
        os.unlink(csv_file)


def main():
    """Run all examples"""
    print("Data Scheduler Usage Examples")
    print("=" * 50)
    
    examples = [
        ("Basic Scheduler Usage", example_basic_scheduler),
        ("Environment Configuration", example_environment_configuration),
        ("Scheduler with Mock RabbitMQ", example_scheduler_with_mock_rabbitmq),
        ("Error Handling Scenarios", example_scheduler_error_scenarios),
        ("Status Monitoring", example_scheduler_status_monitoring),
        ("Production-like Setup", example_production_like_setup)
    ]
    
    for example_name, example_func in examples:
        try:
            print(f"\nRunning: {example_name}")
            result = example_func()
            if not result:
                print(f"❌ {example_name} failed")
        except Exception as e:
            print(f"❌ {example_name} failed with exception: {e}")
            import traceback
            traceback.print_exc()
    
    print("\n" + "=" * 50)
    print("Examples completed!")
    print("\nTo run the actual IAQ Agent:")
    print("1. Set up environment variables (see .env.example)")
    print("2. Start RabbitMQ server:")
    print("   docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management")
    print("3. Run the agent:")
    print("   python main.py")


if __name__ == "__main__":
    main()