#!/usr/bin/env python3
"""
Health Check Example Usage

This script demonstrates how to use the database health check functionality
"""

import os
import sys
import time
import requests
import json
from datetime import datetime

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db_health_check import DatabaseHealthChecker


def example_standalone_health_check():
    """Example of running standalone database health check"""
    print("=== Standalone Database Health Check ===")
    
    # Use environment variable or default connection string
    connection_string = os.getenv(
        "DATABASE_URL", 
        "postgresql://postgres:password@localhost:5432/hotel_iot"
    )
    
    print(f"Connecting to database...")
    checker = DatabaseHealthChecker(connection_string)
    
    # Run health check
    result = checker.run_health_check()
    
    print(f"Overall Status: {result['overall_status'].upper()}")
    print(f"Timestamp: {result['timestamp']}")
    
    if 'summary' in result:
        summary = result['summary']
        print(f"\nSummary:")
        print(f"  Total Checks: {summary['total_checks']}")
        print(f"  Healthy: {summary['healthy']}")
        print(f"  Warnings: {summary['warnings']}")
        print(f"  Errors: {summary['errors']}")
    
    print(f"\nDetailed Results:")
    for check in result['checks']:
        status_symbol = {
            'healthy': '✓',
            'warning': '⚠',
            'error': '✗'
        }.get(check['status'], '?')
        
        print(f"  {status_symbol} {check['component']}: {check['details']}")


def example_http_health_check():
    """Example of accessing health check via HTTP endpoints"""
    print("\n=== HTTP Health Check Endpoints ===")
    
    base_url = "http://localhost:8080"
    
    endpoints = [
        ("/health", "Basic Health Check"),
        ("/health/detailed", "Detailed Health Check"),
        ("/health/database", "Database Health Check")
    ]
    
    for endpoint, description in endpoints:
        print(f"\n{description}:")
        print(f"GET {base_url}{endpoint}")
        
        try:
            response = requests.get(f"{base_url}{endpoint}", timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                print(f"✓ Status: {response.status_code}")
                print(f"  Overall Status: {data.get('overall_status', data.get('status', 'unknown'))}")
                
                if 'timestamp' in data:
                    print(f"  Timestamp: {data['timestamp']}")
                
                if 'summary' in data:
                    summary = data['summary']
                    print(f"  Summary: {summary['healthy']} healthy, {summary['warnings']} warnings, {summary['errors']} errors")
                
            else:
                print(f"✗ Status: {response.status_code}")
                print(f"  Error: {response.text}")
                
        except requests.ConnectionError:
            print(f"✗ Connection failed - make sure DataLogger Agent is running")
        except requests.Timeout:
            print(f"✗ Request timeout")
        except Exception as e:
            print(f"✗ Error: {e}")


def example_continuous_monitoring():
    """Example of continuous health monitoring"""
    print("\n=== Continuous Health Monitoring ===")
    print("Monitoring health status every 30 seconds...")
    print("Press Ctrl+C to stop")
    
    base_url = "http://localhost:8080"
    
    try:
        while True:
            try:
                response = requests.get(f"{base_url}/health", timeout=5)
                
                if response.status_code == 200:
                    data = response.json()
                    status = data.get('status', 'unknown')
                    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    
                    status_symbol = {
                        'healthy': '✓',
                        'warning': '⚠',
                        'stopped': '■',
                        'error': '✗'
                    }.get(status, '?')
                    
                    print(f"[{timestamp}] {status_symbol} Status: {status.upper()}")
                    
                    # Show component status
                    if 'components' in data:
                        components = data['components']
                        db_status = components.get('database', {}).get('is_connected', False)
                        rabbitmq_status = components.get('rabbitmq', {}).get('status', 'unknown')
                        
                        print(f"    Database: {'✓' if db_status else '✗'}")
                        print(f"    RabbitMQ: {'✓' if rabbitmq_status == 'connected' else '✗'}")
                    
                else:
                    print(f"[{datetime.now().strftime('%H:%M:%S')}] ✗ HTTP {response.status_code}")
                
            except requests.ConnectionError:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] ✗ Connection failed")
            except Exception as e:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] ✗ Error: {e}")
            
            time.sleep(30)
            
    except KeyboardInterrupt:
        print("\nMonitoring stopped")


def example_health_check_with_alerting():
    """Example of health check with simple alerting"""
    print("\n=== Health Check with Alerting ===")
    
    base_url = "http://localhost:8080"
    
    try:
        response = requests.get(f"{base_url}/health/detailed", timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            overall_status = data.get('overall_status', data.get('status', 'unknown'))
            
            # Check for issues
            alerts = []
            
            if overall_status == 'error':
                alerts.append("🚨 CRITICAL: System has errors")
            elif overall_status == 'warning':
                alerts.append("⚠️  WARNING: System has warnings")
            
            # Check database health
            if 'detailed_database_health' in data:
                db_health = data['detailed_database_health']
                if db_health.get('overall_status') == 'error':
                    alerts.append("🚨 CRITICAL: Database health check failed")
            
            # Check message processing
            if 'message_processing' in data:
                msg_proc = data['message_processing']
                if not msg_proc.get('is_receiving_messages', False):
                    alerts.append("⚠️  WARNING: No recent messages received")
            
            # Send alerts (in real implementation, this would send to monitoring system)
            if alerts:
                print("ALERTS DETECTED:")
                for alert in alerts:
                    print(f"  {alert}")
                
                # Example: Send to webhook, email, Slack, etc.
                print("\nWould send alerts to monitoring system...")
                
            else:
                print("✓ All systems healthy - no alerts")
                
        else:
            print(f"Failed to get health status: HTTP {response.status_code}")
            
    except Exception as e:
        print(f"Health check failed: {e}")


if __name__ == "__main__":
    print("Database Health Check Examples")
    print("=" * 50)
    
    # Run examples
    example_standalone_health_check()
    
    print("\n" + "=" * 50)
    print("HTTP Health Check Examples")
    print("Make sure to start the DataLogger Agent first:")
    print("  python main.py")
    print("=" * 50)
    
    example_http_health_check()
    
    # Uncomment to run continuous monitoring
    # example_continuous_monitoring()
    
    example_health_check_with_alerting()
