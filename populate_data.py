#!/usr/bin/env python3
"""
Standalone Data Platform Population Script
===========================================

This script is completely self-contained and populates source tables with realistic
e-commerce data using a 4-stage order lifecycle. No external modules required
except standard Python libraries and packages in requirements.txt.

Tables populated in order:
1. source_customers
2. source_products  
3. source_orders (with new 4-stage lifecycle)
4. source_order_items

Features:
- 4-stage order lifecycle: pending → items added → completed/cancelled → refunded
- Realistic data generation with temporal consistency
- Referential integrity maintenance
- Parameterized control over data volumes

Author: Bengo
Date: November 2025
"""

import os
import sys
import random
import psycopg2
import argparse
from datetime import datetime, timedelta
from decimal import Decimal
from typing import List, Tuple, Optional, Dict, Any
from faker import Faker

# Initialize Faker with multiple locales for diversity
fake = Faker(['en_US','en_GB','de_DE','fr_FR','es_ES','it_IT'])
Faker.seed(42)

# Product categories for realistic data generation
PRODUCT_CATEGORIES = [
    "Dresses", "Tops & Blouses", "Sweaters & Knitwear", "Jackets & Coats", 
    "Pants & Trousers", "Skirts", "Jeans & Denim", "Activewear & Athleisure",
    "Lingerie & Intimates", "Sleepwear & Loungewear", "Swimwear & Beachwear",
    "Handbags & Purses", "Jewelry & Watches", "Scarves & Wraps", 
    "Belts & Accessories", "Shoes & Footwear", "Sunglasses & Eyewear",
    "Hats & Hair Accessories", "Tech Accessories", "Gift Cards & Sets"
]

# Keep legacy environment loading
try:
    from dotenv import load_dotenv
    load_dotenv(override=True)
except Exception:
    pass


# Standalone helper functions (replacing populate package)
def random_timestamp(start_dt: datetime, end_dt: datetime) -> datetime:
    """Generate a random timestamp between start and end datetime"""
    time_between = end_dt - start_dt
    random_seconds = random.randint(0, int(time_between.total_seconds()))
    return start_dt + timedelta(seconds=random_seconds)


def generate_random_timestamp(start_date: datetime, end_date: datetime) -> datetime:
    """Generate a random timestamp between start and end dates (compatible with legacy code)"""
    time_between = end_date - start_date
    days_between = time_between.days
    random_days = random.randint(0, days_between)
    random_hours = random.randint(0, 23)
    random_minutes = random.randint(0, 59)
    random_seconds = random.randint(0, 59)
    return start_date + timedelta(
        days=random_days,
        hours=random_hours,
        minutes=random_minutes,
        seconds=random_seconds
    )


def standalone_calculate_date_range(cursor, table_name: str, days_from_max: int, start_date: Optional[str] = None) -> Tuple[datetime, datetime]:
    """Calculate date range for data generation"""
    if start_date:
        start_datetime = datetime.strptime(start_date, '%Y-%m-%d')
        end_datetime = start_datetime + timedelta(days=days_from_max)
        return start_datetime, end_datetime
    
    # Get the latest date from the table
    cursor.execute(f"SELECT MAX(created_at) FROM data.{table_name}")
    result = cursor.fetchone()
    max_date = result[0] if result and result[0] else datetime.now()
    
    if isinstance(max_date, str):
        max_date = datetime.fromisoformat(max_date.replace('Z', '+00:00'))
    
    # Calculate range
    start_datetime = max_date
    end_datetime = start_datetime + timedelta(days=days_from_max)
    
    return start_datetime, end_datetime


def get_context():
    """Create database connection context"""
    connection = psycopg2.connect(
        host=os.getenv('DB_HOST', 'localhost'),
        port=os.getenv('DB_PORT', '5432'),
        database=os.getenv('DB_NAME', 'edikted'),
        user=os.getenv('DB_USER', 'postgres'),
        password=os.getenv('DB_PASSWORD', 'postgres123')
    )
    connection.autocommit = True
    cursor = connection.cursor()
    
    # Simple context object
    class Context:
        def __init__(self, connection, cursor):
            self.connection = connection
            self.cursor = cursor
            
        def close(self):
            """Close database connection"""
            if self.cursor:
                self.cursor.close()
            if self.connection:
                self.connection.close()
                
        def connect(self, database_name):
            """Connect to specific database - for compatibility"""
            # Already connected to the database, just return True
            return True
    
    return Context(connection, cursor)


def initialize_database(ctx):
    """Initialize the database schema"""
    try:
        # Create data schema if it doesn't exist
        ctx.cursor.execute("CREATE SCHEMA IF NOT EXISTS data")
        print("✅ Schema 'data' created/verified")
        return True
    except Exception as e:
        print(f"❌ Error initializing database: {e}")
        return False


def drop_and_recreate(ctx):
    """Drop and recreate all source tables"""
    print("🗑️ Dropping and recreating tables...")
    
    # SQL for creating tables
    create_sql = """
    -- Drop tables if they exist
    DROP TABLE IF EXISTS data.source_order_items CASCADE;
    DROP TABLE IF EXISTS data.source_orders CASCADE;
    DROP TABLE IF EXISTS data.source_products CASCADE;
    DROP TABLE IF EXISTS data.source_customers CASCADE;
    
    -- Create customers table
    CREATE TABLE data.source_customers (
        id SERIAL PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        email VARCHAR(255) UNIQUE NOT NULL,
        country VARCHAR(100) NOT NULL,
        signup_date DATE NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    
    -- Create products table  
    CREATE TABLE data.source_products (
        id SERIAL PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        category VARCHAR(100) NOT NULL,
        price DECIMAL(10,2) NOT NULL CHECK (price >= 0),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    
    -- Create orders table
    CREATE TABLE data.source_orders (
        record_id SERIAL PRIMARY KEY,
        id VARCHAR(50) NOT NULL,
        customer_id INTEGER NOT NULL REFERENCES data.source_customers(id),
        order_date TIMESTAMP NOT NULL,
        status VARCHAR(50) NOT NULL CHECK (status IN ('pending', 'completed', 'cancelled', 'refunded')),
        total_amount DECIMAL(10,2) NOT NULL CHECK (total_amount >= 0),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    
    -- Create order_items table
    CREATE TABLE data.source_order_items (
        id SERIAL PRIMARY KEY,
        order_id VARCHAR(50) NOT NULL,
        product_id INTEGER NOT NULL REFERENCES data.source_products(id),
        quantity INTEGER NOT NULL CHECK (quantity >= 1),
        unit_price DECIMAL(10,2) NOT NULL CHECK (unit_price >= 0),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    
    -- Create indexes
    CREATE INDEX idx_orders_customer_id ON data.source_orders(customer_id);
    CREATE INDEX idx_orders_id ON data.source_orders(id);
    CREATE INDEX idx_orders_status ON data.source_orders(status);
    CREATE INDEX idx_order_items_order_id ON data.source_order_items(order_id);
    CREATE INDEX idx_order_items_product_id ON data.source_order_items(product_id);
    """
    
    try:
        ctx.cursor.execute(create_sql)
        print("✅ Tables created successfully")
        return True
    except Exception as e:
        print(f"❌ Error creating tables: {e}")
        return False


def truncate_all(ctx):
    """Truncate all source tables"""
    print("🧹 Truncating all tables...")
    try:
        ctx.cursor.execute("""
            TRUNCATE TABLE data.source_order_items CASCADE;
            TRUNCATE TABLE data.source_orders CASCADE;
            TRUNCATE TABLE data.source_products CASCADE;
            TRUNCATE TABLE data.source_customers CASCADE;
        """)
        print("✅ All tables truncated")
    except Exception as e:
        print(f"❌ Error truncating tables: {e}")


def show_table_stats(ctx):
    """Show statistics for all source tables"""
    tables = ['source_customers', 'source_products', 'source_orders', 'source_order_items']
    
    print("\n" + "="*50)
    print("📊 TABLE STATISTICS")
    print("="*50)
    
    for table in tables:
        try:
            ctx.cursor.execute(f"SELECT COUNT(*) FROM data.{table}")
            count = ctx.cursor.fetchone()[0]
            print(f"{table:20}: {count:,} records")
        except Exception as e:
            print(f"{table:20}: Error - {e}")


def populate_customers(ctx, num_records: int, days_from_max: int, start_date: Optional[str] = None):
    """Populate customers table with realistic data"""
    if num_records <= 0:
        return
    print(f"\n🏃‍♂️ Customers: creating {num_records} records")
    
    start_dt, end_dt = standalone_calculate_date_range(ctx.cursor, 'source_customers', days_from_max, start_date)
    
    # Get existing emails to avoid duplicates
    existing_emails = set()
    ctx.cursor.execute("SELECT email FROM data.source_customers")
    existing_emails.update([r[0] for r in ctx.cursor.fetchall()])

    rows = []
    for _ in range(num_records):
        name = fake.name_female()
        email = fake.email()
        tries = 0
        while email in existing_emails and tries < 50:
            email = fake.email()
            tries += 1
        existing_emails.add(email)
        
        country = fake.country()
        signup_date = random_timestamp(start_dt, end_dt).date()
        created_at = random_timestamp(start_dt, end_dt)
        
        rows.append((name, email, country, signup_date, created_at))
    
    # Bulk insert
    insert_sql = """
        INSERT INTO data.source_customers (name, email, country, signup_date, created_at)
        VALUES (%s, %s, %s, %s, %s)
    """
    ctx.cursor.executemany(insert_sql, rows)
    print(f"✅ {len(rows)} customers created")


def populate_products(ctx, num_records: int, days_from_max: int, start_date: Optional[str] = None):
    """Populate products table with realistic data"""
    if num_records <= 0:
        return
    print(f"\n📦 Products: creating {num_records} records")
    
    start_dt, end_dt = standalone_calculate_date_range(ctx.cursor, 'source_products', days_from_max, start_date)
    
    rows = []
    for _ in range(num_records):
        # Generate product name
        base_names = [
            "Classic", "Essential", "Premium", "Luxury", "Comfort", "Style", "Trend", "Modern",
            "Vintage", "Elegant", "Casual", "Formal", "Active", "Cozy", "Chic", "Bold"
        ]
        base_name = random.choice(base_names)
        category = random.choice(PRODUCT_CATEGORIES)
        name = f"{base_name} {category.split(' ')[0]}"
        
        # Generate realistic price based on category
        if "Luxury" in category or "Premium" in base_name:
            price = Decimal(str(round(random.uniform(200, 800), 2)))
        elif "Accessories" in category or "Jewelry" in category:
            price = Decimal(str(round(random.uniform(25, 200), 2)))
        else:
            price = Decimal(str(round(random.uniform(30, 150), 2)))
        
        created_at = random_timestamp(start_dt, end_dt)
        
        rows.append((name, category, price, created_at))
    
    # Bulk insert
    insert_sql = """
        INSERT INTO data.source_products (name, category, price, created_at)
        VALUES (%s, %s, %s, %s)
    """
    ctx.cursor.executemany(insert_sql, rows)
    print(f"✅ {len(rows)} products created")
    
    def database_exists(self, db_name: str) -> bool:
        """
        Check if database exists
        """
        admin_conn = self.get_admin_connection()
        if admin_conn is None:
            return False
        
        try:
            cursor = admin_conn.cursor()
            cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s;", (db_name,))
            exists = cursor.fetchone() is not None
            cursor.close()
            admin_conn.close()
            return exists
        except psycopg2.Error as e:
            print(f"❌ Error checking database existence: {e}")
            admin_conn.close()
            return False
    
    def create_database(self, db_name: str) -> bool:
        """
        Create database if it doesn't exist
        """
        if self.database_exists(db_name):
            print(f"✅ Database '{db_name}' already exists")
            return True
        
        print(f"🔄 Creating database '{db_name}'...")
        admin_conn = self.get_admin_connection()
        if admin_conn is None:
            return False
        
        try:
            cursor = admin_conn.cursor()
            cursor.execute(f"CREATE DATABASE {db_name};")
            cursor.close()
            admin_conn.close()
            print(f"✅ Database '{db_name}' created successfully")
            return True
        except psycopg2.Error as e:
            print(f"❌ Error creating database: {e}")
            admin_conn.close()
            return False
    
    def schema_exists(self, schema_name: str) -> bool:
        """
        Check if schema exists in the connected database
        """
        if not self.connection or not self.cursor:
            return False
        
        try:
            self.cursor.execute("""
                SELECT 1 FROM information_schema.schemata 
                WHERE schema_name = %s;
            """, (schema_name,))
            return self.cursor.fetchone() is not None
        except psycopg2.Error as e:
            print(f"❌ Error checking schema existence: {e}")
            self.connection.rollback()
            return False
    
    def table_exists(self, table_name: str, schema_name: str = 'data') -> bool:
        """
        Check if table exists in the specified schema
        """
        if not self.connection or not self.cursor:
            return False
        
        try:
            self.cursor.execute("""
                SELECT 1 FROM information_schema.tables 
                WHERE table_schema = %s AND table_name = %s;
            """, (schema_name, table_name))
            return self.cursor.fetchone() is not None
        except psycopg2.Error as e:
            print(f"❌ Error checking table existence: {e}")
            self.connection.rollback()
            return False
    
    def create_schema_and_tables(self) -> bool:
        """
        Create the data schema and all required tables
        """
        print("🔄 Creating schema and tables...")
        
        if not self.connection or not self.cursor:
            print("❌ No database connection")
            return False
        
        try:
            # Create data schema
            if not self.schema_exists('data'):
                print("  📁 Creating 'data' schema...")
                self.cursor.execute("CREATE SCHEMA data;")
                print("  ✅ Schema 'data' created")
            else:
                print("  ✅ Schema 'data' already exists")
            
            # Create tables in order (respecting foreign key dependencies)
            
            # 1. SOURCE_CUSTOMERS TABLE
            if not self.table_exists('source_customers'):
                print("  📋 Creating 'source_customers' table...")
                customers_sql = """
                CREATE TABLE data.source_customers (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(255) NOT NULL,
                    email VARCHAR(255) NOT NULL UNIQUE,
                    signup_date DATE NOT NULL,
                    country VARCHAR(100) NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                """
                self.cursor.execute(customers_sql)
                print("  ✅ Table 'source_customers' created")
            else:
                print("  ✅ Table 'source_customers' already exists")
            
            # 2. SOURCE_PRODUCTS TABLE
            if not self.table_exists('source_products'):
                print("  📋 Creating 'source_products' table...")
                products_sql = """
                CREATE TABLE data.source_products (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(255) NOT NULL,
                    category VARCHAR(100) NOT NULL,
                    price DECIMAL(10, 2) NOT NULL CHECK (price >= 0),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                """
                self.cursor.execute(products_sql)
                print("  ✅ Table 'source_products' created")
            else:
                print("  ✅ Table 'source_products' already exists")
            
            # 3. SOURCE_ORDERS TABLE
            if not self.table_exists('source_orders'):
                print("  📋 Creating 'source_orders' table...")
                orders_sql = """
                CREATE TABLE data.source_orders (
                    record_id SERIAL PRIMARY KEY,
                    id VARCHAR(50) NOT NULL,
                    customer_id INTEGER NOT NULL REFERENCES data.source_customers(id),
                    order_date TIMESTAMP NOT NULL,
                    status VARCHAR(50) NOT NULL CHECK (status IN ('pending', 'completed', 'cancelled', 'refunded')),
                    total_amount DECIMAL(10, 2) NOT NULL CHECK (total_amount >= 0),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                """
                self.cursor.execute(orders_sql)
                print("  ✅ Table 'source_orders' created")
            else:
                print("  ✅ Table 'source_orders' already exists")
            
            # 4. SOURCE_ORDER_ITEMS TABLE
            if not self.table_exists('source_order_items'):
                print("  📋 Creating 'source_order_items' table...")
                order_items_sql = """
                CREATE TABLE data.source_order_items (
                    id SERIAL PRIMARY KEY,
                    order_id VARCHAR(50) NOT NULL,
                    product_id INTEGER NOT NULL REFERENCES data.source_products(id),
                    quantity INTEGER NOT NULL CHECK (quantity > 0),
                    unit_price DECIMAL(10, 2) NOT NULL CHECK (unit_price >= 0),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                """
                self.cursor.execute(order_items_sql)
                print("  ✅ Table 'source_order_items' created")
            else:
                print("  ✅ Table 'source_order_items' already exists")
            
            # Commit all changes
            self.connection.commit()
            print("✅ All tables created successfully!")
            return True
            
        except psycopg2.Error as e:
            print(f"❌ Error creating schema and tables: {e}")
            self.connection.rollback()
            return False
    
    def drop_and_recreate_tables(self) -> bool:
        """
        Drop all source tables and recreate them with the correct structure
        """
        print("🔄 Dropping and recreating all source tables...")
        
        if not self.connection or not self.cursor:
            print("❌ No database connection")
            return False
        
        try:
            # Drop tables in reverse dependency order to avoid foreign key constraints
            tables_to_drop = [
                'source_order_items',
                'source_orders', 
                'source_products',
                'source_customers'
            ]
            
            for table in tables_to_drop:
                print(f"  🗑️  Dropping {table}...")
                self.cursor.execute(f"DROP TABLE IF EXISTS data.{table} CASCADE;")
            
            print("  ✅ All tables dropped successfully!")
            
            # Now recreate all tables with correct structure
            return self.create_schema_and_tables()
            
        except psycopg2.Error as e:
            print(f"❌ Error dropping and recreating tables: {e}")
            self.connection.rollback()
            return False
        """
        Truncate all source tables (in correct order to handle foreign keys)
        """
        print("🔄 Truncating all source tables...")
        
        if not self.connection:
            print("❌ No database connection")
            return False
        
        try:
            # Truncate in reverse dependency order to avoid foreign key constraints
            tables_to_truncate = [
                'source_order_items',
                'source_orders', 
                'source_products',
                'source_customers'
            ]
            
            for table in tables_to_truncate:
                if self.table_exists(table):
                    print(f"  🗑️  Truncating {table}...")
                    self.cursor.execute(f"TRUNCATE TABLE data.{table} RESTART IDENTITY CASCADE;")
                else:
                    print(f"  ⚠️  Table {table} does not exist, skipping...")
            
            self.connection.commit()
            print("✅ All tables truncated successfully!")
            return True
            
        except psycopg2.Error as e:
            print(f"❌ Error truncating tables: {e}")
            self.connection.rollback()
            return False
    
    def initialize_database(self) -> bool:
        """
        Complete database initialization: create database, schema, and tables
        """
        print("\n" + "="*60)
        print("🗃️  DATABASE INITIALIZATION")
        print("="*60)
        
        # Step 1: Create database
        if not self.create_database('edikted'):
            return False
        
        # Step 2: Connect to edikted database
        if not self.connect_to_database():
            return False
        
        # Step 3: Create schema and tables
        if not self.create_schema_and_tables():
            return False
        
        print("🎉 Database initialization completed!")
        return True
        
    def connect_to_database(self) -> bool:
        """
        Establish connection to the database
        """
        # Force correct database name 
        db_name = 'edikted'
        db_host = os.getenv('DB_HOST', 'localhost')
        db_port = os.getenv('DB_PORT', '5432')
        db_user = os.getenv('DB_USER', 'postgres')
        db_password = os.getenv('DB_PASSWORD', 'postgres123')
        
        print(f"  🔗 Connecting to: {db_host}:{db_port}/{db_name} as {db_user}")
        
        try:
            self.connection = psycopg2.connect(
                host=db_host,
                port=db_port,
                database=db_name,
                user=db_user,
                password=db_password
            )
            self.connection.autocommit = False  # Ensure we control transactions
            self.cursor = self.connection.cursor()
            print("✅ Connected to database successfully")
            
            # Test connection by querying the database name
            self.cursor.execute("SELECT current_database()")
            result = self.cursor.fetchone()
            if result:
                actual_db_name = result[0]
                print(f"  📊 Connected to database: {actual_db_name}")
            
            return True
        except psycopg2.Error as e:
            print(f"❌ Error connecting to database: {e}")
            return False
    
    def get_table_max_created_at(self, table_name: str) -> Optional[datetime]:
        """
        Get the maximum created_at timestamp from a table
        """
        if not self.connection or not self.cursor:
            return None
            
        try:
            self.cursor.execute(f"""
                SELECT MAX(created_at) 
                FROM data.{table_name}
            """)
            result = self.cursor.fetchone()
            return result[0] if result and result[0] else None
        except psycopg2.Error as e:
            print(f"❌ Error getting max timestamp from {table_name}: {e}")
            self.connection.rollback()  # Reset transaction state
            return None
    
    def get_table_count(self, table_name: str) -> int:
        """
        Get the current row count of a table
        """
        if not self.connection or not self.cursor:
            return 0
            
        try:
            self.cursor.execute(f"SELECT COUNT(*) FROM data.{table_name}")
            result = self.cursor.fetchone()
            return result[0] if result else 0
        except psycopg2.Error as e:
            print(f"❌ Error counting rows in {table_name}: {e}")
            self.connection.rollback()  # Reset transaction state
            return 0
    
    def calculate_date_range(self, table_name: str, days_from_max: int, start_date: Optional[str] = None) -> Tuple[datetime, datetime]:
        """
        Calculate the date range for new records based on existing data or start date
        """
        max_created_at = self.get_table_max_created_at(table_name)
        current_time = datetime.now()
        
        if max_created_at is None:
            # Table is empty, use provided start date or default to 2025-06-01
            if start_date:
                start_datetime = datetime.strptime(start_date, "%Y-%m-%d")
            else:
                # Default to June 1, 2025 if no start date provided and table is empty
                start_datetime = datetime.strptime("2025-06-01", "%Y-%m-%d")
                print(f"  📅 Table '{table_name}' is empty. Using default start date: 2025-06-01")
        else:
            # Use max created_at + 1 minute as start to avoid conflicts
            start_datetime = max_created_at + timedelta(minutes=1)
        
        # Calculate end date: start + days_from_max, but never greater than current time
        end_datetime = start_datetime + timedelta(days=days_from_max)
        
        # Ensure end_datetime is not before start_datetime and not in the future
        if end_datetime > current_time:
            end_datetime = current_time
        
        # If the calculated range is invalid (end before start), use a minimal range
        if end_datetime <= start_datetime:
            end_datetime = start_datetime + timedelta(hours=1)
            if end_datetime > current_time:
                end_datetime = current_time
                start_datetime = current_time - timedelta(hours=1)
        
        print(f"  📅 Date range for {table_name}: {start_datetime.strftime('%Y-%m-%d %H:%M')} to {end_datetime.strftime('%Y-%m-%d %H:%M')}")
        
        return start_datetime, end_datetime
    
    def generate_random_timestamp(self, start_date: datetime, end_date: datetime) -> datetime:
        """
        Generate a random timestamp between start and end dates
        """
        time_between = end_date - start_date
        days_between = time_between.days
        hours_between = time_between.seconds // 3600
        
        random_days = random.randint(0, days_between)
        random_hours = random.randint(0, 23)
        random_minutes = random.randint(0, 59)
        random_seconds = random.randint(0, 59)
        
        return start_date + timedelta(
            days=random_days,
            hours=random_hours,
            minutes=random_minutes,
            seconds=random_seconds
        )
    
    def populate_customers(self, num_records: int, days_from_max: int, start_date: Optional[str] = None):
        """
        Populate source_customers table with fake data using Faker
        """
        print(f"\n🏃‍♂️ Populating {num_records} customers...")
        
        if not self.connection or not self.cursor:
            print("❌ No database connection")
            return
        
        start_datetime, end_datetime = self.calculate_date_range('source_customers', days_from_max, start_date)
        
        customers_data = []
        existing_emails = set()
        
        # Get existing emails to avoid duplicates
        self.cursor.execute("SELECT email FROM data.source_customers")
        existing_emails.update([row[0] for row in self.cursor.fetchall()])
        
        for i in range(num_records):
            # Generate realistic female name using Faker for women's fashion brand
            name = self.fake.name_female()
            
            # Generate unique email based on name
            email = self.fake.email()
            counter = 1
            while email in existing_emails:
                email = self.fake.email()
                counter += 1
                # Fallback to ensure uniqueness after many attempts
                if counter > 100:
                    email = f"{self.fake.user_name()}{counter}@{self.fake.domain_name()}"
            existing_emails.add(email)
            
            # Generate realistic country
            country = self.fake.country()
            
            # Generate realistic dates
            created_at = self.generate_random_timestamp(start_datetime, end_datetime)
            signup_date = created_at.date()
            
            customers_data.append((name, email, signup_date, country, created_at))
        
        # Insert customers
        if not self.connection or not self.cursor:
            print("❌ No database connection for inserting customers")
            return
            
        try:
            self.cursor.executemany("""
                INSERT INTO data.source_customers (name, email, signup_date, country, created_at)
                VALUES (%s, %s, %s, %s, %s)
            """, customers_data)
            self.connection.commit()
            print(f"✅ Successfully inserted {num_records} customers")
        except psycopg2.Error as e:
            print(f"❌ Error inserting customers: {e}")
            self.connection.rollback()
    
    def populate_products(self, num_records: int, days_from_max: int, start_date: Optional[str] = None):
        """
        Populate source_products table with fake data using Faker for women's fashion
        """
        print(f"\n📦 Populating {num_records} products...")
        
        start_datetime, end_datetime = self.calculate_date_range('source_products', days_from_max, start_date)
        
        # Fashion-specific product name components
        fashion_adjectives = [
            "Elegant", "Chic", "Stylish", "Trendy", "Classic", "Modern", "Vintage", 
            "Sophisticated", "Casual", "Formal", "Bohemian", "Minimalist", "Luxe",
            "Cozy", "Sleek", "Feminine", "Edgy", "Romantic", "Bold", "Timeless"
        ]
        
        fashion_styles = [
            "Wrap", "A-Line", "Bodycon", "Maxi", "Mini", "Midi", "High-Waisted",
            "Off-Shoulder", "Cropped", "Oversized", "Fitted", "Flowy", "Structured",
            "Layered", "Pleated", "Ruched", "Embellished", "Cut-Out", "Asymmetric"
        ]
        
        fashion_materials = [
            "Cotton", "Silk", "Denim", "Chiffon", "Velvet", "Lace", "Satin",
            "Cashmere", "Wool", "Leather", "Suede", "Jersey", "Tweed", "Linen"
        ]
        
        products_data = []
        
        for i in range(num_records):
            category = random.choice(self.product_categories)
            
            # Generate fashion-appropriate product names based on category
            if "Dresses" in category:
                product_name = f"{random.choice(fashion_adjectives)} {random.choice(fashion_styles)} {random.choice(fashion_materials)} Dress"
            elif "Tops" in category or "Blouses" in category:
                product_name = f"{random.choice(fashion_adjectives)} {random.choice(fashion_materials)} Blouse"
            elif "Sweaters" in category or "Knitwear" in category:
                product_name = f"{random.choice(fashion_adjectives)} {random.choice(fashion_materials)} Sweater"
            elif "Jackets" in category or "Coats" in category:
                product_name = f"{random.choice(fashion_adjectives)} {random.choice(fashion_materials)} Jacket"
            elif "Pants" in category or "Trousers" in category:
                product_name = f"{random.choice(fashion_adjectives)} {random.choice(fashion_styles)} Trousers"
            elif "Skirts" in category:
                product_name = f"{random.choice(fashion_adjectives)} {random.choice(fashion_styles)} Skirt"
            elif "Jeans" in category:
                product_name = f"{random.choice(fashion_adjectives)} {random.choice(fashion_styles)} Jeans"
            elif "Handbags" in category or "Purses" in category:
                product_name = f"{random.choice(fashion_adjectives)} {random.choice(fashion_materials)} Handbag"
            elif "Jewelry" in category:
                jewelry_types = ["Necklace", "Bracelet", "Earrings", "Ring", "Pendant"]
                product_name = f"{random.choice(fashion_adjectives)} {random.choice(jewelry_types)}"
            elif "Shoes" in category:
                shoe_types = ["Heels", "Flats", "Boots", "Sandals", "Sneakers", "Pumps"]
                product_name = f"{random.choice(fashion_adjectives)} {random.choice(shoe_types)}"
            else:
                # For other accessories
                product_name = f"{random.choice(fashion_adjectives)} {category.split(' &')[0]}"
            
            # Sometimes add seasonal or collection identifiers
            if random.choice([True, False]):
                seasons = ["Spring", "Summer", "Fall", "Winter", "Holiday"]
                collections = ["Classic", "Premium", "Signature", "Limited Edition"]
                identifier = random.choice(seasons + collections)
                product_name += f" - {identifier} Collection"
            
            # Generate realistic fashion pricing
            # Fashion has different price ranges by category
            if "Jewelry" in category:
                if random.random() < 0.4:  # 40% affordable jewelry
                    price = round(random.uniform(19.99, 99.99), 2)
                elif random.random() < 0.8:  # 40% mid-range
                    price = round(random.uniform(100.00, 399.99), 2)
                else:  # 20% luxury jewelry
                    price = round(random.uniform(400.00, 1999.99), 2)
            elif "Handbags" in category:
                if random.random() < 0.3:  # 30% affordable bags
                    price = round(random.uniform(39.99, 149.99), 2)
                elif random.random() < 0.7:  # 40% designer bags
                    price = round(random.uniform(150.00, 599.99), 2)
                else:  # 30% luxury bags
                    price = round(random.uniform(600.00, 2999.99), 2)
            elif "Shoes" in category:
                if random.random() < 0.5:  # 50% regular shoes
                    price = round(random.uniform(49.99, 199.99), 2)
                elif random.random() < 0.8:  # 30% designer shoes
                    price = round(random.uniform(200.00, 499.99), 2)
                else:  # 20% luxury shoes
                    price = round(random.uniform(500.00, 1499.99), 2)
            else:  # Clothing items
                if random.random() < 0.6:  # 60% regular fashion
                    price = round(random.uniform(29.99, 149.99), 2)
                elif random.random() < 0.85:  # 25% designer fashion
                    price = round(random.uniform(150.00, 399.99), 2)
                else:  # 15% luxury fashion
                    price = round(random.uniform(400.00, 1299.99), 2)
            
            created_at = self.generate_random_timestamp(start_datetime, end_datetime)
            
            products_data.append((product_name, category, Decimal(str(price)), created_at))
        
        # Insert products
        if not self.connection or not self.cursor:
            print("❌ No database connection for inserting products")
            return
            
        try:
            self.cursor.executemany("""
                INSERT INTO data.source_products (name, category, price, created_at)
                VALUES (%s, %s, %s, %s)
            """, products_data)
            self.connection.commit()
            print(f"✅ Successfully inserted {num_records} products")
        except psycopg2.Error as e:
            print(f"❌ Error inserting products: {e}")
            self.connection.rollback()
    
    def get_existing_customer_ids(self) -> List[int]:
        """
        Get all existing customer IDs for order creation
        """
        if not self.connection or not self.cursor:
            return []
            
        self.cursor.execute("SELECT id FROM data.source_customers")
        return [row[0] for row in self.cursor.fetchall()]
    
    def populate_orders(self, num_orders: int, days_from_max: int, start_date: Optional[str] = None):
        """
        Populate source_orders table using 4-stage approach:
        Stage 1: Generate X pending orders (100%)
        Stage 2: Select 80-100% → mark completed  
        Stage 3: Remaining from Stage 1 → mark cancelled (1-4 days)
        Stage 4: Select 2-10% from Stage 2 → mark refunded
        """
        print(f"\n🛒 Populating {num_orders} orders with 4-stage progression...")
        
        customer_ids = self.get_existing_customer_ids()
        if not customer_ids:
            print("❌ No customers found. Please populate customers first.")
            return
        
        print(f"  📊 Found {len(customer_ids)} existing customers")
        
        start_datetime, end_datetime = self.calculate_date_range('source_orders', days_from_max, start_date)
        
        # Get the minimum product creation time to ensure orders don't precede products
        if self.connection and self.cursor:
            try:
                self.cursor.execute("SELECT MIN(created_at) FROM data.source_products")
                result = self.cursor.fetchone()
                min_product_time = result[0] if result and result[0] else None
                if min_product_time and min_product_time > start_datetime:
                    start_datetime = min_product_time + timedelta(minutes=1)
                    print(f"  📅 Adjusted order start time to {start_datetime.strftime('%Y-%m-%d %H:%M')} (after products)")
            except psycopg2.Error:
                pass
        
        orders_data = []
        order_counters = {}
        
        # STAGE 1: Generate all pending orders
        print(f"  📝 Stage 1: Creating {num_orders} pending orders...")
        pending_orders = []
        
        for order_num in range(num_orders):
            customer_id = random.choice(customer_ids)
            total_amount = 0.00  # Will be calculated after order items
            
            # Generate original order timestamp
            original_order_date = self.generate_random_timestamp(start_datetime, end_datetime)
            
            # Generate unique order ID
            timestamp_str = original_order_date.strftime('%Y%m%d-%H%M%S')
            if timestamp_str not in order_counters:
                order_counters[timestamp_str] = 1
            else:
                order_counters[timestamp_str] += 1
            
            order_id = f"{timestamp_str}-{order_counters[timestamp_str]:06d}"
            
            # Store order info for later stages
            order_info = {
                'id': order_id,
                'customer_id': customer_id,
                'order_date': original_order_date,
                'total_amount': total_amount
            }
            pending_orders.append(order_info)
            
            # Add pending record
            orders_data.append((order_id, customer_id, original_order_date, "pending", Decimal(str(total_amount)), original_order_date))
        
        # STAGE 2: Select 80-100% for completion
        completion_rate = random.uniform(0.80, 1.00)
        num_to_complete = int(num_orders * completion_rate)
        completed_orders = random.sample(pending_orders, num_to_complete)
        
        print(f"  ✅ Stage 2: Completing {num_to_complete} orders ({completion_rate:.1%})")
        
        for order in completed_orders:
            # Add completion record 1-7 days later
            completed_at = order['order_date'] + timedelta(days=random.randint(1, 7))
            if completed_at > end_datetime:
                completed_at = end_datetime - timedelta(hours=random.randint(1, 12))
            
            orders_data.append((order['id'], order['customer_id'], order['order_date'], "completed", 
                             Decimal(str(order['total_amount'])), completed_at))
        
        # STAGE 3: Remaining orders get cancelled (1-4 days)
        remaining_orders = [order for order in pending_orders if order not in completed_orders]
        
        print(f"  ❌ Stage 3: Cancelling {len(remaining_orders)} orders (1-4 days)")
        
        for order in remaining_orders:
            # Add cancellation record 1-4 days later
            cancelled_at = order['order_date'] + timedelta(days=random.randint(1, 4))
            if cancelled_at > end_datetime:
                cancelled_at = end_datetime - timedelta(hours=random.randint(1, 12))
            
            orders_data.append((order['id'], order['customer_id'], order['order_date'], "cancelled", 
                             Decimal(str(order['total_amount'])), cancelled_at))
        
        # STAGE 4: Select 2-10% of completed for refunds
        refund_rate = random.uniform(0.02, 0.10)
        num_to_refund = int(len(completed_orders) * refund_rate)
        if num_to_refund > 0:
            refunded_orders = random.sample(completed_orders, num_to_refund)
            
            print(f"  💸 Stage 4: Refunding {num_to_refund} orders ({refund_rate:.1%} of completed)")
            
            for order in refunded_orders:
                # Find completion time for this order
                completion_time = None
                for record in orders_data:
                    if record[0] == order['id'] and record[3] == "completed":
                        completion_time = record[5]
                        break
                
                if completion_time:
                    # Add refund record 3-14 days after completion
                    refunded_at = completion_time + timedelta(days=random.randint(3, 14))
                    if refunded_at > end_datetime:
                        refunded_at = end_datetime - timedelta(hours=random.randint(1, 12))
                    
                    orders_data.append((order['id'], order['customer_id'], order['order_date'], "refunded", 
                                     Decimal(str(order['total_amount'])), refunded_at))
        else:
            print(f"  💸 Stage 4: No refunds (rate too low: {refund_rate:.1%})")
        
        # Insert all order records
        if not self.connection or not self.cursor:
            print("❌ No database connection for inserting orders")
            return
            
        try:
            self.cursor.executemany("""
                INSERT INTO data.source_orders (id, customer_id, order_date, status, total_amount, created_at)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, orders_data)
            self.connection.commit()
            
            # Calculate and display stats
            total_records = len(orders_data)
            pending_count = num_orders
            completed_count = len(completed_orders)
            cancelled_count = len(remaining_orders)
            refunded_count = num_to_refund if num_to_refund > 0 else 0
            
            print(f"✅ Successfully inserted {total_records} order records for {num_orders} orders")
            print(f"  📊 Order IDs: YYYYMMDD-HHMMSS-XXXXXX format")
            print(f"  � Status distribution: {pending_count} pending → {completed_count} completed, {cancelled_count} cancelled → {refunded_count} refunded")
            print(f"  � Average {total_records/num_orders:.1f} records per order")
            
        except psycopg2.Error as e:
            print(f"❌ Error inserting orders: {e}")
            self.connection.rollback()
    
    def get_existing_order_ids_and_dates(self) -> Dict[str, datetime]:
        """
        Get unique order IDs with their original order dates
        Since we now have multiple records per order (status progression),
        we get the earliest record for each order ID to get the original order_date
        """
        if not self.connection or not self.cursor:
            return {}
            
        self.cursor.execute("""
            SELECT id, order_date 
            FROM data.source_orders
            WHERE (id, created_at) IN (
                SELECT id, MIN(created_at)
                FROM data.source_orders 
                GROUP BY id
            )
        """)
        return {row[0]: row[1] for row in self.cursor.fetchall()}
    
    def get_existing_product_ids_and_details(self) -> Dict[int, Dict]:
        """
        Get all existing product IDs with their created_at dates and prices
        """
        if not self.connection or not self.cursor:
            return {}
            
        self.cursor.execute("SELECT id, created_at, price FROM data.source_products")
        return {
            row[0]: {
                'created_at': row[1], 
                'price': row[2]
            } for row in self.cursor.fetchall()
        }
    
    def populate_order_items(self, max_items_per_order: int = 5, days_from_max: int = 30, start_date: Optional[str] = None):
        """
        Populate source_order_items table with 1-x items per order, ensuring temporal consistency and realistic pricing
        Each order gets a random number of items between 1 and max_items_per_order
        """
        print(f"\n📋 Populating order items (1-{max_items_per_order} items per order)...")
        
        if not self.connection or not self.cursor:
            print("❌ No database connection")
            return
        
        order_data = self.get_existing_order_ids_and_dates()
        product_data = self.get_existing_product_ids_and_details()
        
        if not order_data:
            print("❌ No orders found. Please populate orders first.")
            return
        
        if not product_data:
            print("❌ No products found. Please populate products first.")
            return
        
        print(f"  📊 Found {len(order_data)} existing orders and {len(product_data)} existing products")
        
        start_datetime, end_datetime = self.calculate_date_range('source_order_items', days_from_max, start_date)
        
        order_items_data = []
        order_totals = {}
        total_items_created = 0
        
        # Process each order and give it 1-x items
        for order_id, order_date in order_data.items():
            # Find products that exist on or before the order date
            eligible_products = [
                pid for pid, product_info in product_data.items()
                if product_info['created_at'] <= order_date
            ]
            
            if not eligible_products:
                print(f"  ⚠️  No products available for order {order_id} (order date: {order_date})")
                continue
            
            # Random number of items for this order (1 to max_items_per_order)
            num_items_for_order = random.randint(1, max_items_per_order)
            
            # Track products already in this order to avoid duplicates
            products_in_order = set()
            
            for item_num in range(num_items_for_order):
                # Try to get a unique product for this order item
                product_id = None
                attempts = 0
                while attempts < 10:  # Prevent infinite loop
                    temp_product_id = random.choice(eligible_products)
                    if temp_product_id not in products_in_order or len(eligible_products) == 1:
                        product_id = temp_product_id
                        products_in_order.add(product_id)
                        break
                    attempts += 1
                
                # If we couldn't find a unique product, use any available one
                if product_id is None:
                    product_id = random.choice(eligible_products)
                    products_in_order.add(product_id)
                
                # Random quantity for this line item (1-3 pieces of the same product)
                quantity = random.randint(1, 3)
                
                # Use actual product price with optional discount
                catalog_price = float(product_data[product_id]['price'])
                
                # Sometimes apply discount (10% chance for 10-30% off)
                if random.random() < 0.1:
                    discount = random.uniform(0.1, 0.3)
                    unit_price = catalog_price * (1 - discount)
                else:
                    unit_price = catalog_price
                
                unit_price = round(unit_price, 2)
                
                # Order items created shortly after order (within same day typically)
                item_created_at = order_date + timedelta(
                    minutes=random.randint(1, 60),
                    seconds=random.randint(0, 59)
                )
                
                # Ensure item created_at is within our date range
                if item_created_at < start_datetime:
                    item_created_at = start_datetime + timedelta(minutes=random.randint(1, 60))
                elif item_created_at > end_datetime:
                    item_created_at = end_datetime - timedelta(minutes=random.randint(1, 60))
                
                order_items_data.append((order_id, product_id, quantity, Decimal(str(unit_price)), item_created_at))
                
                # Track running total for this order
                line_total = quantity * unit_price
                if order_id not in order_totals:
                    order_totals[order_id] = 0
                order_totals[order_id] += line_total
                
                total_items_created += 1
        
        # Insert order items
        try:
            if order_items_data:
                self.cursor.executemany("""
                    INSERT INTO data.source_order_items (order_id, product_id, quantity, unit_price, created_at)
                    VALUES (%s, %s, %s, %s, %s)
                """, order_items_data)
                self.connection.commit()
                
                print(f"✅ Successfully inserted {total_items_created} order items across {len(order_totals)} orders")
                print(f"  📊 Average {total_items_created/len(order_totals):.1f} items per order")
                
                # Update order totals
                if order_totals:
                    self.update_order_totals(order_totals)
            else:
                print("❌ No order items could be created")
                
        except psycopg2.Error as e:
            print(f"❌ Error inserting order items: {e}")
            if self.connection:
                self.connection.rollback()
    
    def update_order_totals(self, order_totals: Dict[str, float]):
        """
        Update order total_amount for each order
        Since we now have multiple records per order (status progression),
        we update ALL records with the same order ID
        """
        print(f"  🔄 Updating order totals for {len(order_totals)} orders...")
        
        if not self.connection or not self.cursor:
            print("❌ No database connection")
            return
        
        try:
            for order_id, total in order_totals.items():
                # Update ALL records for this order ID with the calculated total
                self.cursor.execute("""
                    UPDATE data.source_orders 
                    SET total_amount = %s 
                    WHERE id = %s
                """, (Decimal(str(round(total, 2))), order_id))
            
            self.connection.commit()
            print(f"  ✅ Updated order totals for {len(order_totals)} orders (all status records)")
            
        except psycopg2.Error as e:
            print(f"  ❌ Error updating order totals: {e}")
            self.connection.rollback()
    
    def show_table_stats(self):
        """
        Display current table statistics
        """
        print("\n" + "="*60)
        print("📊 CURRENT TABLE STATISTICS")
        print("="*60)
        
        tables = ['source_customers', 'source_products', 'source_orders', 'source_order_items']
        
        for table in tables:
            count = self.get_table_count(table)
            max_date = self.get_table_max_created_at(table)
            max_date_str = max_date.strftime("%Y-%m-%d %H:%M:%S") if max_date else "No data"
            print(f"  {table:20}: {count:6} rows | Latest: {max_date_str}")
        
        print("="*60)
    
    def close_connection(self):
        """
        Close database connection
        """
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        print("✅ Database connection closed")


def populate_orders_and_items_new_lifecycle(ctx, num_orders: int, max_items_per_order: int, days: int, start_date=None):
    """
    NEW ORDER LIFECYCLE:
    1. Create orders as 'pending' (in memory)
    2. Create order_items for 85-100% of pending orders (in memory) 
    3. Determine final statuses:
       - Orders WITH items: 90-100% → 'completed', rest → 'cancelled'
       - Orders WITHOUT items: all → 'cancelled'
       - 5-10% of completed → 'refunded' (+1-2 days)
    4. Insert everything with proper timestamps
    """
    import random
    from datetime import datetime, timedelta
    from decimal import Decimal
    from faker import Faker
    
    fake = Faker()
    
    print(f"\n🛒 NEW LIFECYCLE: Populating {num_orders} orders with items-based progression...")
    
    # Get existing data
    if not ctx.connection or not ctx.cursor:
        print("❌ No database connection")
        return False
    
    # Get customers
    ctx.cursor.execute("SELECT id FROM data.source_customers")
    customer_ids = [row[0] for row in ctx.cursor.fetchall()]
    if not customer_ids:
        print("❌ No customers found. Please populate customers first.")
        return False
    
    # Get products with details
    ctx.cursor.execute("SELECT id, created_at, price FROM data.source_products")
    product_data = {
        row[0]: {'created_at': row[1], 'price': row[2]} 
        for row in ctx.cursor.fetchall()
    }
    if not product_data:
        print("❌ No products found. Please populate products first.")
        return False
    
    print(f"  📊 Found {len(customer_ids)} customers and {len(product_data)} products")
    
    # Calculate date range
    start_datetime, end_datetime = standalone_calculate_date_range(ctx.cursor, 'source_orders', days, start_date)
    
    # Ensure orders don't precede products
    min_product_time = min(p['created_at'] for p in product_data.values())
    if min_product_time > start_datetime:
        start_datetime = min_product_time + timedelta(minutes=1)
        print(f"  📅 Adjusted order start time to {start_datetime.strftime('%Y-%m-%d %H:%M')} (after products)")
    
    # STAGE 1: Create pending orders (in memory)
    print(f"  📝 Stage 1: Creating {num_orders} pending orders...")
    pending_orders = []
    order_counters = {}
    
    for order_num in range(num_orders):
        customer_id = random.choice(customer_ids)
        original_order_date = generate_random_timestamp(start_datetime, end_datetime)
        
        # Generate unique order ID
        timestamp_str = original_order_date.strftime('%Y%m%d-%H%M%S')
        if timestamp_str not in order_counters:
            order_counters[timestamp_str] = 1
        else:
            order_counters[timestamp_str] += 1
        
        order_id = f"{timestamp_str}-{order_counters[timestamp_str]:06d}"
        
        pending_orders.append({
            'id': order_id,
            'customer_id': customer_id,
            'order_date': original_order_date,
            'total_amount': 0.00,
            'has_items': False,
            'items': []
        })
    
    # STAGE 2: Create order_items for 85-100% of orders
    item_rate = random.uniform(0.85, 1.00)
    num_orders_with_items = int(num_orders * item_rate)
    orders_to_get_items = random.sample(pending_orders, num_orders_with_items)
    
    print(f"  📋 Stage 2: Creating items for {num_orders_with_items} orders ({item_rate:.1%})")
    
    total_items_created = 0
    for order in orders_to_get_items:
        order_date = order['order_date']
        
        # Find products available at order date
        eligible_products = [
            pid for pid, pdata in product_data.items()
            if pdata['created_at'] <= order_date
        ]
        
        if not eligible_products:
            continue
        
        # Random number of items (1 to max_items_per_order)
        num_items = random.randint(1, max_items_per_order)
        products_in_order = set()
        order_total = 0.0
        
        for _ in range(num_items):
            # Try to get unique product
            product_id = None
            attempts = 0
            while attempts < 10:
                temp_product_id = random.choice(eligible_products)
                if temp_product_id not in products_in_order or len(eligible_products) == 1:
                    product_id = temp_product_id
                    products_in_order.add(product_id)
                    break
                attempts += 1
            
            if product_id is None:
                product_id = random.choice(eligible_products)
                products_in_order.add(product_id)
            
            quantity = random.randint(1, 3)
            catalog_price = float(product_data[product_id]['price'])
            
            # Optional discount
            if random.random() < 0.1:
                discount = random.uniform(0.1, 0.3)
                unit_price = catalog_price * (1 - discount)
            else:
                unit_price = catalog_price
            
            unit_price = round(unit_price, 2)
            line_total = quantity * unit_price
            order_total += line_total
            
            # Item created shortly after order
            item_created_at = order_date + timedelta(
                minutes=random.randint(1, 60),
                seconds=random.randint(0, 59)
            )
            
            order['items'].append({
                'product_id': product_id,
                'quantity': quantity,
                'unit_price': unit_price,
                'created_at': item_created_at
            })
            
            total_items_created += 1
        
        order['has_items'] = True
        order['total_amount'] = round(order_total, 2)
    
    print(f"  ✅ Created {total_items_created} items across {len(orders_to_get_items)} orders")
    
    # STAGE 3: Determine final statuses
    orders_with_items = [o for o in pending_orders if o['has_items']]
    orders_without_items = [o for o in pending_orders if not o['has_items']]
    
    # 90-100% of orders with items get completed
    completion_rate = random.uniform(0.90, 1.00)
    num_to_complete = int(len(orders_with_items) * completion_rate)
    completed_orders = random.sample(orders_with_items, num_to_complete) if orders_with_items else []
    cancelled_with_items = [o for o in orders_with_items if o not in completed_orders]
    
    print(f"  ✅ Stage 3a: Completing {num_to_complete} orders with items ({completion_rate:.1%})")
    print(f"  ❌ Stage 3b: Cancelling {len(cancelled_with_items)} orders with items + {len(orders_without_items)} without items")
    
    # STAGE 4: 5-10% of completed get refunded
    refund_rate = random.uniform(0.05, 0.10)
    num_to_refund = int(len(completed_orders) * refund_rate)
    refunded_orders = random.sample(completed_orders, num_to_refund) if completed_orders else []
    
    print(f"  💸 Stage 4: Refunding {num_to_refund} orders ({refund_rate:.1%} of completed)")
    
    # Prepare all order records for insertion
    all_order_records = []
    all_item_records = []
    
    for order in pending_orders:
        order_id = order['id']
        customer_id = order['customer_id']
        order_date = order['order_date']
        total_amount = Decimal(str(order['total_amount']))
        
        # Add pending record
        all_order_records.append((order_id, customer_id, order_date, "pending", total_amount, order_date))
        
        # Add items if they exist
        for item in order['items']:
            all_item_records.append((
                order_id,
                item['product_id'], 
                item['quantity'],
                Decimal(str(item['unit_price'])),
                item['created_at']
            ))
        
        # Add status progression records
        if order in completed_orders:
            # Add completed record (1-4 days later)
            completed_at = order_date + timedelta(days=random.randint(1, 4))
            if completed_at > end_datetime:
                completed_at = end_datetime - timedelta(hours=random.randint(1, 12))
            all_order_records.append((order_id, customer_id, order_date, "completed", total_amount, completed_at))
            
            # Add refunded record if applicable (1-2 days after completion)
            if order in refunded_orders:
                refunded_at = completed_at + timedelta(days=random.randint(1, 2))
                if refunded_at > end_datetime:
                    refunded_at = end_datetime - timedelta(hours=random.randint(1, 12))
                all_order_records.append((order_id, customer_id, order_date, "refunded", total_amount, refunded_at))
        
        else:
            # Add cancelled record (1-4 days later)
            cancelled_at = order_date + timedelta(days=random.randint(1, 4))
            if cancelled_at > end_datetime:
                cancelled_at = end_datetime - timedelta(hours=random.randint(1, 12))
            all_order_records.append((order_id, customer_id, order_date, "cancelled", total_amount, cancelled_at))
    
    # Insert everything into database
    try:
        if all_order_records:
            print(f"  💾 Inserting {len(all_order_records)} order records...")
            ctx.cursor.executemany("""
                INSERT INTO data.source_orders (id, customer_id, order_date, status, total_amount, created_at)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, all_order_records)
        
        if all_item_records:
            print(f"  💾 Inserting {len(all_item_records)} order item records...")
            ctx.cursor.executemany("""
                INSERT INTO data.source_order_items (order_id, product_id, quantity, unit_price, created_at)
                VALUES (%s, %s, %s, %s, %s)
            """, all_item_records)
        
        ctx.connection.commit()
        
        # Display final stats
        print(f"✅ NEW LIFECYCLE COMPLETE!")
        print(f"  📊 {num_orders} unique orders created")
        print(f"  📊 {len(orders_with_items)} orders got items, {len(orders_without_items)} without items")
        print(f"  📊 {len(completed_orders)} completed, {len(cancelled_with_items) + len(orders_without_items)} cancelled, {len(refunded_orders)} refunded")
        print(f"  📊 {len(all_order_records)} total order records (avg {len(all_order_records)/num_orders:.1f} per order)")
        print(f"  📊 {len(all_item_records)} total item records")
        
        return True
        
    except Exception as e:
        print(f"❌ Error inserting records: {e}")
        ctx.connection.rollback()
        return False


def populate_all_new_lifecycle(ctx, customers=50, products=100, orders=200, order_items=5, days=30, start_date=None):
    """
    Populate all tables using new order lifecycle
    """
    print("🏃‍♂️ Populating customers...")
    populate_customers(ctx, customers, days, start_date)
    
    print("📦 Populating products...")
    populate_products(ctx, products, days, start_date)
    
    print("🛒 Populating orders and items (NEW LIFECYCLE)...")
    return populate_orders_and_items_new_lifecycle(ctx, orders, order_items, days, start_date)


def main():
    parser = argparse.ArgumentParser(description='Populate source tables (modular version)')
    parser.add_argument('--customers', type=int, default=300)
    parser.add_argument('--products', type=int, default=500)
    parser.add_argument('--orders', type=int, default=2500)
    parser.add_argument('--order-items', type=int, default=7)
    parser.add_argument('--days', type=int, default=30)
    parser.add_argument('--start-date', type=str)
    parser.add_argument('--stats-only', action='store_true')
    parser.add_argument('--init-db', action='store_true')
    parser.add_argument('--truncate', action='store_true')
    parser.add_argument('--drop-recreate', action='store_true')
    args = parser.parse_args()

    print("="*60)
    print("🚀 DATA POPULATION SCRIPT (modular)")
    print("="*60)

    ctx = get_context()

    # Init or connect
    if args.init_db:
        if not initialize_database(ctx):
            ctx.close(); return 1
    else:
        if not ctx.connect('edikted'):
            print("❌ Failed to connect. Try --init-db")
            ctx.close(); return 1

    # Optional destructive operations
    if args.truncate and args.drop_recreate:
        print("❌ Cannot use both --truncate and --drop-recreate")
        ctx.close(); return 1
    if args.drop_recreate:
        if not drop_and_recreate(ctx):
            ctx.close(); return 1
    elif args.truncate:
        truncate_all(ctx)

    if args.stats_only:
        show_table_stats(ctx)
        ctx.close(); return 0

    print("📋 Parameters:")
    print(f"  Customers: {args.customers}")
    print(f"  Products: {args.products}")
    print(f"  Orders: {args.orders}")
    print(f"  Max Items / Order: {args.order_items}")
    print(f"  Days window: {args.days}")
    if args.start_date:
        print(f"  Start date: {args.start_date}")
    if args.truncate:
        print("  Truncated: Yes")
    if args.drop_recreate:
        print("  Dropped/Recreated: Yes")

    try:
        populate_all_new_lifecycle(ctx,
                     customers=args.customers,
                     products=args.products,
                     orders=args.orders,
                     order_items=args.order_items,
                     days=args.days,
                     start_date=args.start_date)
        print("\n🎉 Data population completed successfully!")
        print("\n📋 Usage examples:")
        print("  python3 populate_data.py --init-db")
        print("  python3 populate_data.py --customers 100 --products 200 --orders 300 --order-items 5")
        print("  python3 populate_data.py --drop-recreate --customers 50 --products 100")
        print("  python3 populate_data.py --days 7 --start-date 2025-01-01")
        print("  python3 populate_data.py --stats-only")
    except KeyboardInterrupt:
        print("\n❌ Interrupted by user")
        ctx.close(); return 1
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        ctx.close(); return 1
    finally:
        ctx.close()
    return 0


if __name__ == "__main__":
    exit(main())