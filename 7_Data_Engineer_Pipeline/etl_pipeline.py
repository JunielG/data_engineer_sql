#!/usr/bin/env python3
"""
Complete ETL Pipeline Example
============================

This example demonstrates a production-ready ETL pipeline that:
- Extracts data from multiple sources (CSV, API, Database)
- Transforms data with validation and cleaning
- Loads data to a target database
- Includes error handling, logging, and data quality checks
"""

import pandas as pd
import sqlite3
import requests
import logging
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from pathlib import Path
import hashlib
import time

# Configuration
@dataclass
class ETLConfig:
    """Configuration class for ETL pipeline"""
    source_csv_path: str = "data/sales_data.csv"
    source_api_url: str = "https://jsonplaceholder.typicode.com/users"
    source_db_path: str = "data/source.db"
    target_db_path: str = "data/warehouse.db"
    log_file: str = "logs/etl_pipeline.log"
    batch_size: int = 1000
    max_retries: int = 3
    timeout: int = 30

class ETLLogger:
    """Custom logging setup for ETL pipeline"""
    
    def __init__(self, log_file: str, level: str = "INFO"):
        self.logger = logging.getLogger("ETL_Pipeline")
        self.logger.setLevel(getattr(logging, level))
        
        # Create logs directory if it doesn't exist
        Path(log_file).parent.mkdir(parents=True, exist_ok=True)
        
        # File handler
        file_handler = logging.FileHandler(log_file)
        file_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        file_handler.setFormatter(file_formatter)
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_formatter = logging.Formatter(
            '%(levelname)s - %(message)s'
        )
        console_handler.setFormatter(console_formatter)
        
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)
    
    def get_logger(self):
        return self.logger

class DataQualityChecker:
    """Data quality validation and checks"""
    
    def __init__(self, logger):
        self.logger = logger
        self.quality_report = {}
    
    def check_null_values(self, df: pd.DataFrame, columns: List[str]) -> Dict:
        """Check for null values in specified columns"""
        null_report = {}
        for col in columns:
            if col in df.columns:
                null_count = df[col].isnull().sum()
                null_percentage = (null_count / len(df)) * 100
                null_report[col] = {
                    'null_count': null_count,
                    'null_percentage': round(null_percentage, 2)
                }
        return null_report
    
    def check_duplicates(self, df: pd.DataFrame, key_columns: List[str]) -> int:
        """Check for duplicate records based on key columns"""
        return df.duplicated(subset=key_columns).sum()
    
    def check_data_types(self, df: pd.DataFrame, expected_types: Dict) -> Dict:
        """Validate data types match expectations"""
        type_issues = {}
        for col, expected_type in expected_types.items():
            if col in df.columns:
                actual_type = str(df[col].dtype)
                if expected_type not in actual_type:
                    type_issues[col] = {
                        'expected': expected_type,
                        'actual': actual_type
                    }
        return type_issues
    
    def validate_data(self, df: pd.DataFrame, validation_rules: Dict) -> bool:
        """Run comprehensive data validation"""
        self.logger.info(f"Running data quality checks on {len(df)} records")
        
        # Check required columns exist
        required_cols = validation_rules.get('required_columns', [])
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            self.logger.error(f"Missing required columns: {missing_cols}")
            return False
        
        # Check null values
        null_report = self.check_null_values(df, required_cols)
        self.quality_report['null_values'] = null_report
        
        # Check duplicates
        key_cols = validation_rules.get('key_columns', [])
        if key_cols:
            duplicate_count = self.check_duplicates(df, key_cols)
            self.quality_report['duplicates'] = duplicate_count
            if duplicate_count > 0:
                self.logger.warning(f"Found {duplicate_count} duplicate records")
        
        # Check data types
        expected_types = validation_rules.get('data_types', {})
        type_issues = self.check_data_types(df, expected_types)
        self.quality_report['type_issues'] = type_issues
        
        # Log quality report
        self.logger.info(f"Data quality report: {json.dumps(self.quality_report, indent=2)}")
        
        return len(type_issues) == 0

class DataExtractor:
    """Handle data extraction from various sources"""
    
    def __init__(self, config: ETLConfig, logger):
        self.config = config
        self.logger = logger
    
    def extract_from_csv(self, file_path: str) -> pd.DataFrame:
        """Extract data from CSV file"""
        try:
            self.logger.info(f"Extracting data from CSV: {file_path}")
            df = pd.read_csv(file_path)
            self.logger.info(f"Successfully extracted {len(df)} records from CSV")
            return df
        except FileNotFoundError:
            self.logger.error(f"CSV file not found: {file_path}")
            # Create sample data for demo
            return self._create_sample_sales_data()
        except Exception as e:
            self.logger.error(f"Error extracting from CSV: {str(e)}")
            raise
    
    def extract_from_api(self, url: str, retries: int = 3) -> pd.DataFrame:
        """Extract data from REST API with retry logic"""
        for attempt in range(retries):
            try:
                self.logger.info(f"Extracting data from API: {url} (attempt {attempt + 1})")
                response = requests.get(url, timeout=self.config.timeout)
                response.raise_for_status()
                
                data = response.json()
                df = pd.json_normalize(data)
                self.logger.info(f"Successfully extracted {len(df)} records from API")
                return df
                
            except requests.exceptions.RequestException as e:
                self.logger.warning(f"API request failed (attempt {attempt + 1}): {str(e)}")
                if attempt == retries - 1:
                    self.logger.error("All API retry attempts failed")
                    return pd.DataFrame()  # Return empty DataFrame on failure
                time.sleep(2 ** attempt)  # Exponential backoff
    
    def extract_from_database(self, db_path: str, query: str) -> pd.DataFrame:
        """Extract data from SQLite database"""
        try:
            self.logger.info(f"Extracting data from database: {db_path}")
            with sqlite3.connect(db_path) as conn:
                df = pd.read_sql_query(query, conn)
            self.logger.info(f"Successfully extracted {len(df)} records from database")
            return df
        except sqlite3.Error as e:
            self.logger.error(f"Database extraction error: {str(e)}")
            return pd.DataFrame()
        except Exception as e:
            self.logger.error(f"Unexpected error in database extraction: {str(e)}")
            raise
    
    def _create_sample_sales_data(self) -> pd.DataFrame:
        """Create sample sales data for demonstration"""
        self.logger.info("Creating sample sales data for demonstration")
        sample_data = {
            'transaction_id': range(1, 101),
            'customer_id': [f"CUST_{i:03d}" for i in range(1, 101)],
            'product_id': [f"PROD_{i % 10 + 1:03d}" for i in range(100)],
            'quantity': [i % 5 + 1 for i in range(100)],
            'unit_price': [10.0 + (i % 50) for i in range(100)],
            'transaction_date': [
                (datetime.now() - timedelta(days=i % 30)).strftime('%Y-%m-%d')
                for i in range(100)
            ]
        }
        return pd.DataFrame(sample_data)

class DataTransformer:
    """Handle data transformation and cleaning"""
    
    def __init__(self, logger):
        self.logger = logger
    
    def clean_sales_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and transform sales data"""
        self.logger.info("Starting sales data transformation")
        
        # Create a copy to avoid modifying original
        df_clean = df.copy()
        
        # Remove duplicates
        initial_count = len(df_clean)
        df_clean = df_clean.drop_duplicates()
        duplicates_removed = initial_count - len(df_clean)
        if duplicates_removed > 0:
            self.logger.info(f"Removed {duplicates_removed} duplicate records")
        
        # Handle missing values
        df_clean = df_clean.dropna(subset=['transaction_id', 'customer_id'])
        
        # Data type conversions
        if 'transaction_date' in df_clean.columns:
            df_clean['transaction_date'] = pd.to_datetime(df_clean['transaction_date'])
        
        # Calculate derived fields
        if 'quantity' in df_clean.columns and 'unit_price' in df_clean.columns:
            df_clean['total_amount'] = df_clean['quantity'] * df_clean['unit_price']
        
        # Add audit fields
        df_clean['processed_at'] = datetime.now()
        df_clean['record_hash'] = df_clean.apply(
            lambda row: hashlib.md5(str(row.to_dict()).encode()).hexdigest(), axis=1
        )
        
        self.logger.info(f"Transformation complete: {len(df_clean)} records processed")
        return df_clean
    
    def transform_user_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform user data from API"""
        if df.empty:
            return df
        
        self.logger.info("Starting user data transformation")
        
        df_transformed = df.copy()
        
        # Flatten nested structures
        if 'address.geo.lat' in df_transformed.columns:
            df_transformed['latitude'] = pd.to_numeric(df_transformed['address.geo.lat'], errors='coerce')
            df_transformed['longitude'] = pd.to_numeric(df_transformed['address.geo.lng'], errors='coerce')
        
        # Create full address
        address_cols = ['address.street', 'address.city', 'address.zipcode']
        available_cols = [col for col in address_cols if col in df_transformed.columns]
        if available_cols:
            df_transformed['full_address'] = df_transformed[available_cols].fillna('').agg(' '.join, axis=1)
        
        # Clean and standardize data
        if 'email' in df_transformed.columns:
            df_transformed['email'] = df_transformed['email'].str.lower().str.strip()
        
        # Add audit fields
        df_transformed['extracted_at'] = datetime.now()
        
        self.logger.info(f"User data transformation complete: {len(df_transformed)} records")
        return df_transformed
    
    def aggregate_sales_summary(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create sales summary aggregations"""
        if df.empty or 'total_amount' not in df.columns:
            return pd.DataFrame()
        
        self.logger.info("Creating sales summary aggregations")
        
        # Daily sales summary
        daily_summary = df.groupby('transaction_date').agg({
            'total_amount': ['sum', 'count', 'mean'],
            'quantity': 'sum'
        }).round(2)
        
        # Flatten column names
        daily_summary.columns = ['_'.join(col).strip() for col in daily_summary.columns]
        daily_summary = daily_summary.reset_index()
        daily_summary['summary_type'] = 'daily'
        daily_summary['created_at'] = datetime.now()
        
        self.logger.info(f"Created daily summary with {len(daily_summary)} records")
        return daily_summary

class DataLoader:
    """Handle data loading to target systems"""
    
    def __init__(self, config: ETLConfig, logger):
        self.config = config
        self.logger = logger
    
    def setup_target_database(self):
        """Create target database tables"""
        self.logger.info("Setting up target database schema")
        
        # Create data directory if it doesn't exist
        Path(self.config.target_db_path).parent.mkdir(parents=True, exist_ok=True)
        
        with sqlite3.connect(self.config.target_db_path) as conn:
            cursor = conn.cursor()
            
            # Sales data table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS sales_data (
                    transaction_id INTEGER PRIMARY KEY,
                    customer_id TEXT NOT NULL,
                    product_id TEXT,
                    quantity INTEGER,
                    unit_price REAL,
                    total_amount REAL,
                    transaction_date DATE,
                    processed_at TIMESTAMP,
                    record_hash TEXT
                )
            """)
            
            # User data table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS user_data (
                    id INTEGER PRIMARY KEY,
                    name TEXT,
                    username TEXT,
                    email TEXT,
                    phone TEXT,
                    website TEXT,
                    full_address TEXT,
                    latitude REAL,
                    longitude REAL,
                    extracted_at TIMESTAMP
                )
            """)
            
            # Sales summary table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS sales_summary (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    transaction_date DATE,
                    total_amount_sum REAL,
                    total_amount_count INTEGER,
                    total_amount_mean REAL,
                    quantity_sum INTEGER,
                    summary_type TEXT,
                    created_at TIMESTAMP
                )
            """)
            
            conn.commit()
            self.logger.info("Target database schema created successfully")
    
    def load_data(self, df: pd.DataFrame, table_name: str, if_exists: str = 'append') -> bool:
        """Load data to target database"""
        if df.empty:
            self.logger.warning(f"No data to load for table {table_name}")
            return True
        
        try:
            self.logger.info(f"Loading {len(df)} records to {table_name}")
            
            with sqlite3.connect(self.config.target_db_path) as conn:
                df.to_sql(table_name, conn, if_exists=if_exists, index=False)
            
            self.logger.info(f"Successfully loaded data to {table_name}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error loading data to {table_name}: {str(e)}")
            return False
    
    def get_load_statistics(self) -> Dict:
        """Get statistics about loaded data"""
        try:
            with sqlite3.connect(self.config.target_db_path) as conn:
                cursor = conn.cursor()
                
                stats = {}
                tables = ['sales_data', 'user_data', 'sales_summary']
                
                for table in tables:
                    cursor.execute(f"SELECT COUNT(*) FROM {table}")
                    count = cursor.fetchone()[0]
                    stats[table] = count
                
                return stats
                
        except Exception as e:
            self.logger.error(f"Error getting load statistics: {str(e)}")
            return {}

class ETLPipeline:
    """Main ETL Pipeline orchestrator"""
    
    def __init__(self, config: ETLConfig = None):
        self.config = config or ETLConfig()
        self.logger_setup = ETLLogger(self.config.log_file)
        self.logger = self.logger_setup.get_logger()
        
        # Initialize components
        self.extractor = DataExtractor(self.config, self.logger)
        self.transformer = DataTransformer(self.logger)
        self.loader = DataLoader(self.config, self.logger)
        self.quality_checker = DataQualityChecker(self.logger)
        
        self.pipeline_stats = {
            'start_time': None,
            'end_time': None,
            'records_processed': 0,
            'errors': []
        }
    
    def run_pipeline(self) -> bool:
        """Execute the complete ETL pipeline"""
        self.pipeline_stats['start_time'] = datetime.now()
        self.logger.info("="*50)
        self.logger.info("Starting ETL Pipeline")
        self.logger.info("="*50)
        
        try:
            # Setup target database
            self.loader.setup_target_database()
            
            # Extract data from multiple sources
            sales_data = self.extractor.extract_from_csv(self.config.source_csv_path)
            user_data = self.extractor.extract_from_api(self.config.source_api_url)
            
            # Data quality validation
            sales_validation_rules = {
                'required_columns': ['transaction_id', 'customer_id'],
                'key_columns': ['transaction_id'],
                'data_types': {'transaction_id': 'int', 'customer_id': 'object'}
            }
            
            if not self.quality_checker.validate_data(sales_data, sales_validation_rules):
                self.logger.error("Sales data quality validation failed")
                return False
            
            # Transform data
            sales_clean = self.transformer.clean_sales_data(sales_data)
            user_clean = self.transformer.transform_user_data(user_data)
            sales_summary = self.transformer.aggregate_sales_summary(sales_clean)
            
            # Load data
            success = True
            success &= self.loader.load_data(sales_clean, 'sales_data', 'replace')
            success &= self.loader.load_data(user_clean, 'user_data', 'replace')
            success &= self.loader.load_data(sales_summary, 'sales_summary', 'replace')
            
            if not success:
                self.logger.error("Data loading failed")
                return False
            
            # Get final statistics
            load_stats = self.loader.get_load_statistics()
            self.pipeline_stats['records_processed'] = sum(load_stats.values())
            
            self.pipeline_stats['end_time'] = datetime.now()
            duration = self.pipeline_stats['end_time'] - self.pipeline_stats['start_time']
            
            self.logger.info("="*50)
            self.logger.info("ETL Pipeline Completed Successfully")
            self.logger.info(f"Duration: {duration}")
            self.logger.info(f"Records processed: {self.pipeline_stats['records_processed']}")
            self.logger.info(f"Load statistics: {load_stats}")
            self.logger.info("="*50)
            
            return True
            
        except Exception as e:
            self.logger.error(f"Pipeline execution failed: {str(e)}")
            self.pipeline_stats['errors'].append(str(e))
            return False
    
    def get_pipeline_stats(self) -> Dict:
        """Get pipeline execution statistics"""
        return self.pipeline_stats

# Example usage and testing
if __name__ == "__main__":
    # Create ETL pipeline instance
    pipeline = ETLPipeline()
    
    # Run the pipeline
    success = pipeline.run_pipeline()
    
    if success:
        print("\n✅ ETL Pipeline executed successfully!")
        stats = pipeline.get_pipeline_stats()
        print(f"Records processed: {stats['records_processed']}")
        print(f"Execution time: {stats['end_time'] - stats['start_time']}")
    else:
        print("\n❌ ETL Pipeline failed!")
        print("Check logs for details.")
    
    # Example of how to query the results
    import sqlite3
    
    print("\n📊 Sample Results:")
    print("-" * 30)
    
    config = ETLConfig()
    try:
        with sqlite3.connect(config.target_db_path) as conn:
            # Show sales summary
            df_summary = pd.read_sql_query(
                "SELECT * FROM sales_summary ORDER BY transaction_date DESC LIMIT 5", 
                conn
            )
            print("Sales Summary (Last 5 days):")
            print(df_summary.to_string(index=False))
            
            # Show top customers by total amount
            df_customers = pd.read_sql_query("""
                SELECT customer_id, 
                       COUNT(*) as transaction_count,
                       SUM(total_amount) as total_spent
                FROM sales_data 
                GROUP BY customer_id 
                ORDER BY total_spent DESC 
                LIMIT 5
            """, conn)
            print("\n\nTop 5 Customers by Total Spent:")
            print(df_customers.to_string(index=False))
            
    except Exception as e:
        print(f"Error querying results: {e}")