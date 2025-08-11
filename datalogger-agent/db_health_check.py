#!/usr/bin/env python3
"""
Database Health Check Script for Smart Hotel IoT System
This script checks the health and status of the TimescaleDB database
"""

import os
import sys
import psycopg2
from psycopg2.extras import RealDictCursor
import json
from datetime import datetime, timedelta
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DatabaseHealthChecker:
    def __init__(self, connection_string: str):
        """
        Initialize the database health checker
        
        Args:
            connection_string: PostgreSQL connection string
        """
        self.connection_string = connection_string
        self.connection = None
        
    def connect(self) -> bool:
        """Establish database connection"""
        try:
            self.connection = psycopg2.connect(
                self.connection_string,
                cursor_factory=RealDictCursor
            )
            return True
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            return False
    
    def disconnect(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
    
    def check_extension(self) -> dict:
        """Check if TimescaleDB extension is installed"""
        try:
            with self.connection.cursor() as cursor:
                cursor.execute("""
                    SELECT EXISTS (
                        SELECT 1 FROM pg_extension WHERE extname = 'timescaledb'
                    ) as installed
                """)
                result = cursor.fetchone()
                
                return {
                    'component': 'timescaledb_extension',
                    'status': 'healthy' if result['installed'] else 'error',
                    'details': 'TimescaleDB extension is installed' if result['installed'] else 'TimescaleDB extension is missing'
                }
        except Exception as e:
            return {
                'component': 'timescaledb_extension',
                'status': 'error',
                'details': f'Error checking extension: {str(e)}'
            }
    
    def check_tables(self) -> list:
        """Check if required tables exist"""
        required_tables = ['raw_data', 'deployment_logs']
        results = []
        
        for table_name in required_tables:
            try:
                with self.connection.cursor() as cursor:
                    cursor.execute("""
                        SELECT EXISTS (
                            SELECT 1 FROM information_schema.tables 
                            WHERE table_name = %s
                        ) as exists
                    """, (table_name,))
                    result = cursor.fetchone()
                    
                    results.append({
                        'component': f'{table_name}_table',
                        'status': 'healthy' if result['exists'] else 'error',
                        'details': f'Table {table_name} exists' if result['exists'] else f'Table {table_name} is missing'
                    })
            except Exception as e:
                results.append({
                    'component': f'{table_name}_table',
                    'status': 'error',
                    'details': f'Error checking table {table_name}: {str(e)}'
                })
        
        return results
    
    def check_hypertable(self) -> dict:
        """Check if raw_data is configured as a hypertable"""
        try:
            with self.connection.cursor() as cursor:
                cursor.execute("""
                    SELECT EXISTS (
                        SELECT 1 FROM timescaledb_information.hypertables 
                        WHERE hypertable_name = 'raw_data'
                    ) as is_hypertable
                """)
                result = cursor.fetchone()
                
                return {
                    'component': 'raw_data_hypertable',
                    'status': 'healthy' if result['is_hypertable'] else 'error',
                    'details': 'Raw data is configured as hypertable' if result['is_hypertable'] else 'Raw data is not a hypertable'
                }
        except Exception as e:
            return {
                'component': 'raw_data_hypertable',
                'status': 'error',
                'details': f'Error checking hypertable: {str(e)}'
            }
    
    def check_indexes(self) -> list:
        """Check if required indexes exist"""
        required_indexes = [
            'idx_raw_data_device_id',
            'idx_raw_data_datapoint',
            'idx_raw_data_timestamp',
            'idx_raw_data_device_datapoint_datetime'
        ]
        results = []
        
        for index_name in required_indexes:
            try:
                with self.connection.cursor() as cursor:
                    cursor.execute("""
                        SELECT EXISTS (
                            SELECT 1 FROM pg_indexes 
                            WHERE indexname = %s
                        ) as exists
                    """, (index_name,))
                    result = cursor.fetchone()
                    
                    results.append({
                        'component': f'{index_name}_index',
                        'status': 'healthy' if result['exists'] else 'warning',
                        'details': f'Index {index_name} exists' if result['exists'] else f'Index {index_name} is missing'
                    })
            except Exception as e:
                results.append({
                    'component': f'{index_name}_index',
                    'status': 'error',
                    'details': f'Error checking index {index_name}: {str(e)}'
                })
        
        return results
    
    def check_recent_data(self) -> dict:
        """Check if there's recent data in the database"""
        try:
            with self.connection.cursor() as cursor:
                cursor.execute("""
                    SELECT 
                        COUNT(*) as total_records,
                        COUNT(CASE WHEN datetime > NOW() - INTERVAL '1 hour' THEN 1 END) as recent_records,
                        MAX(datetime) as latest_record
                    FROM raw_data
                """)
                result = cursor.fetchone()
                
                total_records = result['total_records']
                recent_records = result['recent_records']
                latest_record = result['latest_record']
                
                if total_records == 0:
                    status = 'warning'
                    details = 'No data found in database'
                elif recent_records > 0:
                    status = 'healthy'
                    details = f'Recent data available: {recent_records} records in last hour'
                else:
                    status = 'warning'
                    details = f'No recent data (latest: {latest_record})'
                
                return {
                    'component': 'recent_data',
                    'status': status,
                    'details': details,
                    'metrics': {
                        'total_records': total_records,
                        'recent_records': recent_records,
                        'latest_record': str(latest_record) if latest_record else None
                    }
                }
        except Exception as e:
            return {
                'component': 'recent_data',
                'status': 'error',
                'details': f'Error checking recent data: {str(e)}'
            }
    
    def get_database_stats(self) -> dict:
        """Get database statistics"""
        try:
            with self.connection.cursor() as cursor:
                # Get basic statistics
                cursor.execute("""
                    SELECT 
                        COUNT(*) as total_records,
                        COUNT(DISTINCT device_id) as unique_devices,
                        COUNT(DISTINCT datapoint) as datapoint_types,
                        MIN(datetime) as earliest_record,
                        MAX(datetime) as latest_record
                    FROM raw_data
                """)
                basic_stats = cursor.fetchone()
                
                # Get database size
                cursor.execute("""
                    SELECT pg_size_pretty(pg_database_size(current_database())) as database_size
                """)
                size_result = cursor.fetchone()
                
                # Get table sizes
                cursor.execute("""
                    SELECT 
                        schemaname,
                        tablename,
                        pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size
                    FROM pg_tables 
                    WHERE tablename IN ('raw_data', 'deployment_logs')
                    ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC
                """)
                table_sizes = cursor.fetchall()
                
                return {
                    'component': 'database_statistics',
                    'status': 'healthy',
                    'details': 'Database statistics collected successfully',
                    'metrics': {
                        'total_records': basic_stats['total_records'],
                        'unique_devices': basic_stats['unique_devices'],
                        'datapoint_types': basic_stats['datapoint_types'],
                        'earliest_record': str(basic_stats['earliest_record']) if basic_stats['earliest_record'] else None,
                        'latest_record': str(basic_stats['latest_record']) if basic_stats['latest_record'] else None,
                        'database_size': size_result['database_size'],
                        'table_sizes': [dict(row) for row in table_sizes]
                    }
                }
        except Exception as e:
            return {
                'component': 'database_statistics',
                'status': 'error',
                'details': f'Error collecting statistics: {str(e)}'
            }
    
    def run_health_check(self) -> dict:
        """Run complete health check"""
        if not self.connect():
            return {
                'overall_status': 'error',
                'timestamp': datetime.now().isoformat(),
                'checks': [{
                    'component': 'database_connection',
                    'status': 'error',
                    'details': 'Failed to connect to database'
                }]
            }
        
        try:
            checks = []
            
            # Check extension
            checks.append(self.check_extension())
            
            # Check tables
            checks.extend(self.check_tables())
            
            # Check hypertable
            checks.append(self.check_hypertable())
            
            # Check indexes
            checks.extend(self.check_indexes())
            
            # Check recent data
            checks.append(self.check_recent_data())
            
            # Get statistics
            checks.append(self.get_database_stats())
            
            # Determine overall status
            error_count = sum(1 for check in checks if check['status'] == 'error')
            warning_count = sum(1 for check in checks if check['status'] == 'warning')
            
            if error_count > 0:
                overall_status = 'error'
            elif warning_count > 0:
                overall_status = 'warning'
            else:
                overall_status = 'healthy'
            
            return {
                'overall_status': overall_status,
                'timestamp': datetime.now().isoformat(),
                'summary': {
                    'total_checks': len(checks),
                    'healthy': sum(1 for check in checks if check['status'] == 'healthy'),
                    'warnings': warning_count,
                    'errors': error_count
                },
                'checks': checks
            }
            
        finally:
            self.disconnect()

def main():
    """Main entry point for the health check script"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Database Health Check Tool")
    parser.add_argument(
        "--connection-string", 
        default=os.getenv("DATABASE_URL", "postgresql://postgres:password@localhost:5432/hotel_iot"),
        help="PostgreSQL connection string"
    )
    parser.add_argument(
        "--format",
        choices=["json", "text"],
        default="text",
        help="Output format"
    )
    parser.add_argument(
        "--output-file",
        help="Output file path (optional)"
    )
    
    args = parser.parse_args()
    
    # Run health check
    checker = DatabaseHealthChecker(args.connection_string)
    result = checker.run_health_check()
    
    # Format output
    if args.format == "json":
        output = json.dumps(result, indent=2)
    else:
        # Text format
        lines = []
        lines.append(f"=== Database Health Check ===")
        lines.append(f"Timestamp: {result['timestamp']}")
        lines.append(f"Overall Status: {result['overall_status'].upper()}")
        lines.append("")
        
        if 'summary' in result:
            summary = result['summary']
            lines.append(f"Summary:")
            lines.append(f"  Total Checks: {summary['total_checks']}")
            lines.append(f"  Healthy: {summary['healthy']}")
            lines.append(f"  Warnings: {summary['warnings']}")
            lines.append(f"  Errors: {summary['errors']}")
            lines.append("")
        
        lines.append("Detailed Results:")
        for check in result['checks']:
            status_symbol = {
                'healthy': '✓',
                'warning': '⚠',
                'error': '✗'
            }.get(check['status'], '?')
            
            lines.append(f"  {status_symbol} {check['component']}: {check['details']}")
            
            if 'metrics' in check:
                for key, value in check['metrics'].items():
                    if isinstance(value, list):
                        lines.append(f"    {key}:")
                        for item in value:
                            lines.append(f"      - {item}")
                    else:
                        lines.append(f"    {key}: {value}")
        
        output = "\n".join(lines)
    
    # Output result
    if args.output_file:
        with open(args.output_file, 'w') as f:
            f.write(output)
        print(f"Health check results written to {args.output_file}")
    else:
        print(output)
    
    # Exit with appropriate code
    sys.exit(0 if result['overall_status'] in ['healthy', 'warning'] else 1)

if __name__ == "__main__":
    main()