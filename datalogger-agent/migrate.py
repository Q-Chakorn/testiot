#!/usr/bin/env python3
"""
Database Migration Script for Smart Hotel IoT System
This script manages database schema migrations for TimescaleDB
"""

import os
import sys
import hashlib
import psycopg2
from psycopg2.extras import RealDictCursor
from pathlib import Path
import logging
from typing import List, Dict, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DatabaseMigrator:
    def __init__(self, connection_string: str, migrations_dir: str = "migrations"):
        """
        Initialize the database migrator
        
        Args:
            connection_string: PostgreSQL connection string
            migrations_dir: Directory containing migration files
        """
        self.connection_string = connection_string
        self.migrations_dir = Path(migrations_dir)
        self.connection = None
        
    def connect(self) -> bool:
        """Establish database connection"""
        try:
            self.connection = psycopg2.connect(
                self.connection_string,
                cursor_factory=RealDictCursor
            )
            self.connection.autocommit = False
            logger.info("Connected to database successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            return False
    
    def disconnect(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            logger.info("Disconnected from database")
    
    def get_file_checksum(self, file_path: Path) -> str:
        """Calculate MD5 checksum of a file"""
        hash_md5 = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
    
    def get_applied_migrations(self) -> List[Dict]:
        """Get list of applied migrations from database"""
        try:
            with self.connection.cursor() as cursor:
                cursor.execute("""
                    SELECT version, description, applied_at, checksum 
                    FROM schema_migrations 
                    ORDER BY version
                """)
                return cursor.fetchall()
        except psycopg2.Error as e:
            logger.error(f"Failed to get applied migrations: {e}")
            return []
    
    def get_pending_migrations(self) -> List[Path]:
        """Get list of migration files that haven't been applied"""
        if not self.migrations_dir.exists():
            logger.error(f"Migrations directory {self.migrations_dir} does not exist")
            return []
        
        # Get all SQL files in migrations directory
        migration_files = sorted(self.migrations_dir.glob("*.sql"))
        
        # Get applied migrations
        applied_migrations = self.get_applied_migrations()
        applied_versions = {m['version'] for m in applied_migrations}
        
        # Filter out already applied migrations
        pending_migrations = []
        for file_path in migration_files:
            version = file_path.stem.split('_')[0]
            if version not in applied_versions:
                pending_migrations.append(file_path)
        
        return pending_migrations
    
    def apply_migration(self, migration_file: Path) -> bool:
        """Apply a single migration file"""
        version = migration_file.stem.split('_')[0]
        description = ' '.join(migration_file.stem.split('_')[1:]).replace('_', ' ')
        
        logger.info(f"Applying migration {version}: {description}")
        
        try:
            # Read migration file
            with open(migration_file, 'r', encoding='utf-8') as f:
                migration_sql = f.read()
            
            # Calculate checksum
            checksum = self.get_file_checksum(migration_file)
            
            # Execute migration in a transaction
            with self.connection.cursor() as cursor:
                # Execute the migration SQL
                cursor.execute(migration_sql)
                
                # Record the migration (if not already recorded by the migration itself)
                cursor.execute("""
                    INSERT INTO schema_migrations (version, description, applied_at, checksum) 
                    VALUES (%s, %s, NOW(), %s)
                    ON CONFLICT (version) DO UPDATE SET
                        description = EXCLUDED.description,
                        checksum = EXCLUDED.checksum
                """, (version, description, checksum))
                
                # Commit the transaction
                self.connection.commit()
                
            logger.info(f"Successfully applied migration {version}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to apply migration {version}: {e}")
            self.connection.rollback()
            return False
    
    def migrate(self) -> bool:
        """Apply all pending migrations"""
        if not self.connect():
            return False
        
        try:
            # Ensure migration table exists
            self.ensure_migration_table()
            
            # Get pending migrations
            pending_migrations = self.get_pending_migrations()
            
            if not pending_migrations:
                logger.info("No pending migrations found")
                return True
            
            logger.info(f"Found {len(pending_migrations)} pending migrations")
            
            # Apply each migration
            for migration_file in pending_migrations:
                if not self.apply_migration(migration_file):
                    logger.error("Migration failed, stopping")
                    return False
            
            logger.info("All migrations applied successfully")
            return True
            
        finally:
            self.disconnect()
    
    def ensure_migration_table(self):
        """Ensure the schema_migrations table exists"""
        try:
            with self.connection.cursor() as cursor:
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS schema_migrations (
                        version VARCHAR(10) PRIMARY KEY,
                        description TEXT NOT NULL,
                        applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        checksum VARCHAR(64)
                    )
                """)
                self.connection.commit()
        except Exception as e:
            logger.error(f"Failed to create migration table: {e}")
            self.connection.rollback()
            raise
    
    def status(self) -> bool:
        """Show migration status"""
        if not self.connect():
            return False
        
        try:
            self.ensure_migration_table()
            
            applied_migrations = self.get_applied_migrations()
            pending_migrations = self.get_pending_migrations()
            
            print("\n=== Migration Status ===")
            print(f"Applied migrations: {len(applied_migrations)}")
            print(f"Pending migrations: {len(pending_migrations)}")
            
            if applied_migrations:
                print("\nApplied Migrations:")
                for migration in applied_migrations:
                    print(f"  ✓ {migration['version']}: {migration['description']} ({migration['applied_at']})")
            
            if pending_migrations:
                print("\nPending Migrations:")
                for migration_file in pending_migrations:
                    version = migration_file.stem.split('_')[0]
                    description = ' '.join(migration_file.stem.split('_')[1:]).replace('_', ' ')
                    print(f"  ○ {version}: {description}")
            
            print()
            return True
            
        finally:
            self.disconnect()
    
    def rollback(self, target_version: str) -> bool:
        """Rollback to a specific migration version (basic implementation)"""
        logger.warning("Rollback functionality is not implemented yet")
        logger.warning("Manual rollback may be required for complex schema changes")
        return False

def main():
    """Main entry point for the migration script"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Database Migration Tool")
    parser.add_argument(
        "--connection-string", 
        default=os.getenv("DATABASE_URL", "postgresql://postgres:password@localhost:5432/hotel_iot"),
        help="PostgreSQL connection string"
    )
    parser.add_argument(
        "--migrations-dir",
        default="migrations",
        help="Directory containing migration files"
    )
    parser.add_argument(
        "command",
        choices=["migrate", "status", "rollback"],
        help="Command to execute"
    )
    parser.add_argument(
        "--target-version",
        help="Target version for rollback"
    )
    
    args = parser.parse_args()
    
    migrator = DatabaseMigrator(args.connection_string, args.migrations_dir)
    
    if args.command == "migrate":
        success = migrator.migrate()
        sys.exit(0 if success else 1)
    elif args.command == "status":
        success = migrator.status()
        sys.exit(0 if success else 1)
    elif args.command == "rollback":
        if not args.target_version:
            logger.error("Target version required for rollback")
            sys.exit(1)
        success = migrator.rollback(args.target_version)
        sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()