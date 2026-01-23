from models import Base, get_db_engine
from config import Config
import pymysql

def create_database_if_not_exists():
    """Create the database if it doesn't exist"""
    try:
        # Connect without database name
        connection = pymysql.connect(
            host=Config.DB_HOST,
            port=int(Config.DB_PORT),
            user=Config.DB_USER,
            password=Config.DB_PASSWORD
        )
        cursor = connection.cursor()
        
        # Create database
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {Config.DB_NAME} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
        print(f"Database '{Config.DB_NAME}' created or already exists")
        
        cursor.close()
        connection.close()
        return True
    except Exception as e:
        print(f"Error creating database: {e}")
        return False

def init_tables():
    """Initialize all database tables"""
    try:
        # First create database
        if not create_database_if_not_exists():
            print("Failed to create database")
            return False
        
        # Create engine and tables
        engine = get_db_engine(Config.DATABASE_URL)
        Base.metadata.create_all(engine)
        print("All database tables created successfully!")
        
        # Print created tables
        from sqlalchemy import inspect
        inspector = inspect(engine)
        tables = inspector.get_table_names()
        print(f"Created tables: {', '.join(tables)}")
        
        return True
    except Exception as e:
        print(f"Error initializing tables: {e}")
        return False

if __name__ == "__main__":
    init_tables()
