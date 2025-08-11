"""
Database module for DataLogger Agent

This module provides functionality to connect to TimescaleDB/PostgreSQL
and manage database operations with connection pooling and error handling.
"""
import psycopg2
import psycopg2.pool
import psycopg2.extras
from psycopg2 import sql
import logging
import time
from typing import Optional, List, Dict, Any, Tuple
from contextlib import contextmanager
from pathlib import Path
import os

logger = logging.getLogger(__name__)


class DatabaseError(Exception):
    """Custom exception for database errors"""
    pass


class DatabaseConnection:
    """
    Database connection manager for TimescaleDB/PostgreSQL
    
    Features:
    - Connection pooling for better performance
    - Automatic reconnection on connection loss
    - Transaction management
    - Health checks and monitoring
    - Schema initialization
    """
    
    def __init__(self, connection_string: str, pool_size: int = 5, max_connections: int = 20):
        """
        Initialize database connection manager
        
        Args:
            connection_string: PostgreSQL connection string
            pool_size: Minimum number of connections in pool
            max_connections: Maximum number of connections in pool
        """
        self.connection_string = connection_string
        self.pool_size = pool_size
        self.max_connections = max_connections
        self.connection_pool: Optional[psycopg2.pool.ThreadedConnectionPool] = None
        self.is_connected = False
        
        # Parse connection parameters for logging (without password)
        self._log_connection_info()
    
    def _log_connection_info(self):
        """Log connection information (without sensitive data)"""
        try:
            # Parse connection string to extract host and database
            params = {}
            for param in self.connection_string.split():
                if '=' in param:
                    key, value = param.split('=', 1)
                    params[key] = value
            
            host = params.get('host', 'localhost')
            port = params.get('port', '5432')
            dbname = params.get('dbname', 'unknown')
            
            logger.info(f"Database connection configured: {host}:{port}/{dbname}")
            
        except Exception as e:
            logger.warning(f"Could not parse connection string for logging: {e}")
    
    def connect(self) -> bool:
        """
        Establish connection pool to database
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            logger.info("Establishing database connection pool...")
            
            # Create connection pool
            self.connection_pool = psycopg2.pool.ThreadedConnectionPool(
                minconn=self.pool_size,
                maxconn=self.max_connections,
                dsn=self.connection_string,
                cursor_factory=psycopg2.extras.RealDictCursor
            )
            
            # Test connection
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT version();")
                    version = cursor.fetchone()
                    logger.info(f"Connected to database: {version['version']}")
            
            self.is_connected = True
            logger.info("Database connection pool established successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            self.is_connected = False
            return False
    
    def disconnect(self):
        """Close all connections in the pool"""
        if self.connection_pool:
            try:
                self.connection_pool.closeall()
                logger.info("Database connection pool closed")
            except Exception as e:
                logger.error(f"Error closing connection pool: {e}")
            finally:
                self.connection_pool = None
                self.is_connected = False
    
    @contextmanager
    def get_connection(self):
        """
        Context manager for getting a connection from the pool
        
        Yields:
            Database connection
        """
        if not self.connection_pool:
            raise DatabaseError("Connection pool not initialized")
        
        connection = None
        try:
            connection = self.connection_pool.getconn()
            if connection.closed:
                # Connection is closed, try to reconnect
                logger.warning("Connection was closed, attempting to reconnect...")
                self.connection_pool.putconn(connection, close=True)
                connection = self.connection_pool.getconn()
            
            yield connection
            
        except Exception as e:
            if connection:
                connection.rollback()
            raise DatabaseError(f"Database operation failed: {e}") from e
        finally:
            if connection:
                self.connection_pool.putconn(connection)
    
    def execute_query(self, query: str, params: Optional[Tuple] = None) -> int:
        """
        Execute a query that doesn't return results (INSERT, UPDATE, DELETE)
        
        Args:
            query: SQL query string
            params: Query parameters
            
        Returns:
            Number of affected rows
        """
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, params)
                conn.commit()
                return cursor.rowcount
    
    def fetch_one(self, query: str, params: Optional[Tuple] = None) -> Optional[Dict[str, Any]]:
        """
        Execute a query and fetch one result
        
        Args:
            query: SQL query string
            params: Query parameters
            
        Returns:
            Single row as dictionary or None
        """
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, params)
                result = cursor.fetchone()
                return dict(result) if result else None
    
    def fetch_all(self, query: str, params: Optional[Tuple] = None) -> List[Dict[str, Any]]:
        """
        Execute a query and fetch all results
        
        Args:
            query: SQL query string
            params: Query parameters
            
        Returns:
            List of rows as dictionaries
        """
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, params)
                results = cursor.fetchall()
                return [dict(row) for row in results]
    
    def execute_many(self, query: str, params_list: List[Tuple]) -> int:
        """
        Execute a query multiple times with different parameters (batch operation)
        
        Args:
            query: SQL query string
            params_list: List of parameter tuples
            
        Returns:
            Total number of affected rows
        """
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.executemany(query, params_list)
                conn.commit()
                return cursor.rowcount
    
    def initialize_schema(self, schema_file_path: str = "schema.sql") -> bool:
        """
        Initialize database schema from SQL file
        
        Args:
            schema_file_path: Path to schema SQL file
            
        Returns:
            True if schema initialization successful
        """
        try:
            schema_path = Path(schema_file_path)
            if not schema_path.exists():
                logger.error(f"Schema file not found: {schema_file_path}")
                return False
            
            logger.info(f"Initializing database schema from {schema_file_path}")
            
            with open(schema_path, 'r', encoding='utf-8') as f:
                schema_sql = f.read()
            
            # Split SQL file into individual statements
            statements = [stmt.strip() for stmt in schema_sql.split(';') if stmt.strip()]
            
            # Separate transaction-sensitive statements
            transaction_statements = []
            non_transaction_statements = []
            
            for statement in statements:
                statement_upper = statement.upper()
                if ('CREATE MATERIALIZED VIEW' in statement_upper or 
                    'ADD_CONTINUOUS_AGGREGATE_POLICY' in statement_upper or
                    'ADD_COMPRESSION_POLICY' in statement_upper or
                    'ADD_RETENTION_POLICY' in statement_upper):
                    non_transaction_statements.append(statement)
                else:
                    transaction_statements.append(statement)
            
            # Execute regular statements in transaction
            if transaction_statements:
                with self.get_connection() as conn:
                    with conn.cursor() as cursor:
                        for statement in transaction_statements:
                            if statement:
                                try:
                                    cursor.execute(statement)
                                    logger.debug(f"Executed: {statement[:50]}...")
                                except Exception as e:
                                    logger.warning(f"Statement failed (continuing): {e}")
                                    # Continue with other statements
                                    continue
                        
                        conn.commit()
            
            # Execute non-transaction statements individually
            if non_transaction_statements:
                with self.get_connection() as conn:
                    conn.autocommit = True  # Enable autocommit for these statements
                    with conn.cursor() as cursor:
                        for statement in non_transaction_statements:
                            if statement:
                                try:
                                    cursor.execute(statement)
                                    logger.debug(f"Executed (autocommit): {statement[:50]}...")
                                except Exception as e:
                                    logger.warning(f"Statement failed (continuing): {e}")
                                    # Continue with other statements
                                    continue
                    conn.autocommit = False  # Restore normal transaction mode
            
            logger.info("Database schema initialized successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to initialize schema: {e}")
            return False
    
    def health_check(self) -> Dict[str, Any]:
        """
        Perform database health check
        
        Returns:
            Dictionary containing health check results
        """
        health_info = {
            'is_connected': False,
            'connection_pool_status': 'unknown',
            'database_version': None,
            'schema_valid': False,
            'response_time_ms': None,
            'error': None
        }
        
        try:
            start_time = time.time()
            
            # Check connection pool
            if not self.connection_pool:
                health_info['error'] = 'Connection pool not initialized'
                return health_info
            
            health_info['connection_pool_status'] = 'active'
            
            # Test database connection and get version
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT version();")
                    version_result = cursor.fetchone()
                    health_info['database_version'] = version_result['version']
                    
                    # Check if raw_data table exists
                    cursor.execute("""
                        SELECT EXISTS (
                            SELECT FROM information_schema.tables 
                            WHERE table_name = 'raw_data'
                        );
                    """)
                    table_exists = cursor.fetchone()['exists']
                    health_info['schema_valid'] = table_exists
            
            # Calculate response time
            response_time = (time.time() - start_time) * 1000
            health_info['response_time_ms'] = round(response_time, 2)
            health_info['is_connected'] = True
            
        except Exception as e:
            health_info['error'] = str(e)
            logger.error(f"Database health check failed: {e}")
        
        return health_info
    
    def get_connection_stats(self) -> Dict[str, Any]:
        """
        Get connection pool statistics
        
        Returns:
            Dictionary containing connection pool stats
        """
        if not self.connection_pool:
            return {'error': 'Connection pool not initialized'}
        
        try:
            # Note: psycopg2 doesn't provide direct access to pool stats
            # This is a basic implementation
            return {
                'pool_size': self.pool_size,
                'max_connections': self.max_connections,
                'is_connected': self.is_connected
            }
        except Exception as e:
            return {'error': str(e)}
    
    def __enter__(self):
        """Context manager entry"""
        if not self.is_connected:
            self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.disconnect()


def create_database_connection(connection_string: str, **kwargs) -> DatabaseConnection:
    """
    Factory function to create database connection
    
    Args:
        connection_string: PostgreSQL connection string
        **kwargs: Additional connection parameters
        
    Returns:
        DatabaseConnection instance
    """
    return DatabaseConnection(connection_string, **kwargs)


def create_database_connection_from_env() -> DatabaseConnection:
    """
    Create database connection from environment variables
    
    Expected environment variables:
    - DATABASE_URL: Full PostgreSQL connection string
    OR individual components:
    - DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
    
    Returns:
        DatabaseConnection instance
    """
    database_url = os.getenv('DATABASE_URL')
    
    if database_url:
        return DatabaseConnection(database_url)
    
    # Build connection string from individual components
    host = os.getenv('DB_HOST', 'localhost')
    port = os.getenv('DB_PORT', '5432')
    dbname = os.getenv('DB_NAME', 'sensor_data')
    user = os.getenv('DB_USER', 'postgres')
    password = os.getenv('DB_PASSWORD', 'password')
    
    connection_string = f"host={host} port={port} dbname={dbname} user={user} password={password}"
    
    return DatabaseConnection(connection_string)