#!/usr/bin/env python3
"""
Complete Data Engineering Pipeline
A comprehensive pipeline demonstrating data ingestion, transformation, storage, and monitoring
"""

import pandas as pd
import sqlite3
import json
import logging
import asyncio
import aiohttp
import schedule
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
from abc import ABC, abstractmethod
import hashlib
import concurrent.futures
from contextlib import contextmanager

# Configuration
@dataclass
class PipelineConfig:
    """Configuration for the data pipeline"""
    raw_data_path: str = "data/raw"
    processed_data_path: str = "data/processed"
    database_path: str = "data/pipeline.db"
    log_path: str = "logs/pipeline.log"
    batch_size: int = 1000
    max_workers: int = 4
    api_rate_limit: int = 100  # requests per minute

# Setup logging
def setup_logging(log_path: str):
    """Configure logging for the pipeline"""
    Path(log_path).parent.mkdir(parents=True, exist_ok=True)
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_path),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

# Data Sources
class DataSource(ABC):
    """Abstract base class for data sources"""
    
    @abstractmethod
    async def extract(self) -> List[Dict[str, Any]]:
        pass

class APIDataSource(DataSource):
    """Extract data from REST API"""
    
    def __init__(self, base_url: str, endpoints: List[str], headers: Dict[str, str] = None):
        self.base_url = base_url
        self.endpoints = endpoints
        self.headers = headers or {}
        self.logger = logging.getLogger(self.__class__.__name__)
    
    async def extract(self) -> List[Dict[str, Any]]:
        """Extract data from API endpoints"""
        all_data = []
        
        async with aiohttp.ClientSession(headers=self.headers) as session:
            for endpoint in self.endpoints:
                url = f"{self.base_url}/{endpoint}"
                try:
                    async with session.get(url) as response:
                        if response.status == 200:
                            data = await response.json()
                            if isinstance(data, list):
                                all_data.extend(data)
                            else:
                                all_data.append(data)
                            self.logger.info(f"Extracted {len(data)} records from {endpoint}")
                        else:
                            self.logger.error(f"Failed to fetch {url}: {response.status}")
                except Exception as e:
                    self.logger.error(f"Error fetching {url}: {str(e)}")
        
        return all_data

class FileDataSource(DataSource):
    """Extract data from files (CSV, JSON, etc.)"""
    
    def __init__(self, file_paths: List[str]):
        self.file_paths = file_paths
        self.logger = logging.getLogger(self.__class__.__name__)
    
    async def extract(self) -> List[Dict[str, Any]]:
        """Extract data from files"""
        all_data = []
        
        for file_path in self.file_paths:
            try:
                path = Path(file_path)
                if path.suffix.lower() == '.csv':
                    df = pd.read_csv(file_path)
                    all_data.extend(df.to_dict('records'))
                elif path.suffix.lower() == '.json':
                    with open(file_path, 'r') as f:
                        data = json.load(f)
                        if isinstance(data, list):
                            all_data.extend(data)
                        else:
                            all_data.append(data)
                
                self.logger.info(f"Extracted data from {file_path}")
            except Exception as e:
                self.logger.error(f"Error reading {file_path}: {str(e)}")
        
        return all_data

# Data Transformers
class DataTransformer:
    """Handle data transformations and cleaning"""
    
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def clean_data(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Clean and standardize data"""
        cleaned_data = []
        
        for record in data:
            try:
                # Remove null/empty values
                cleaned_record = {k: v for k, v in record.items() if v is not None and v != ""}
                
                # Standardize date formats
                for key, value in cleaned_record.items():
                    if 'date' in key.lower() or 'time' in key.lower():
                        cleaned_record[key] = self._standardize_date(value)
                
                # Add metadata
                cleaned_record['_processed_at'] = datetime.now().isoformat()
                cleaned_record['_record_hash'] = self._generate_hash(cleaned_record)
                
                cleaned_data.append(cleaned_record)
                
            except Exception as e:
                self.logger.warning(f"Error cleaning record: {str(e)}")
                continue
        
        self.logger.info(f"Cleaned {len(cleaned_data)} records")
        return cleaned_data
    
    def _standardize_date(self, date_value: Any) -> str:
        """Standardize date formats"""
        if isinstance(date_value, str):
            try:
                # Try common date formats
                for fmt in ['%Y-%m-%d', '%m/%d/%Y', '%d-%m-%Y', '%Y-%m-%d %H:%M:%S']:
                    try:
                        dt = datetime.strptime(date_value, fmt)
                        return dt.isoformat()
                    except ValueError:
                        continue
            except:
                pass
        return str(date_value)
    
    def _generate_hash(self, record: Dict[str, Any]) -> str:
        """Generate hash for record deduplication"""
        # Create a copy without metadata fields
        clean_record = {k: v for k, v in record.items() if not k.startswith('_')}
        record_str = json.dumps(clean_record, sort_keys=True)
        return hashlib.md5(record_str.encode()).hexdigest()
    
    def aggregate_data(self, data: List[Dict[str, Any]], group_by: str, metrics: List[str]) -> List[Dict[str, Any]]:
        """Aggregate data by specified columns"""
        df = pd.DataFrame(data)
        
        if group_by not in df.columns:
            self.logger.warning(f"Group by column '{group_by}' not found")
            return data
        
        agg_dict = {}
        for metric in metrics:
            if metric in df.columns and pd.api.types.is_numeric_dtype(df[metric]):
                agg_dict[metric] = ['sum', 'mean', 'count']
        
        if not agg_dict:
            self.logger.warning("No numeric columns found for aggregation")
            return data
        
        aggregated = df.groupby(group_by).agg(agg_dict).reset_index()
        aggregated.columns = ['_'.join(col).strip() for col in aggregated.columns.values]
        
        result = aggregated.to_dict('records')
        self.logger.info(f"Aggregated data into {len(result)} groups")
        return result

# Data Storage
class DataStorage:
    """Handle data storage operations"""
    
    def __init__(self, config: PipelineConfig):
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        self._init_database()
    
    def _init_database(self):
        """Initialize database tables"""
        Path(self.config.database_path).parent.mkdir(parents=True, exist_ok=True)
        
        with sqlite3.connect(self.config.database_path) as conn:
            # Create raw data table
            conn.execute('''
                CREATE TABLE IF NOT EXISTS raw_data (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    data_source TEXT NOT NULL,
                    record_hash TEXT UNIQUE,
                    data JSON NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Create processed data table
            conn.execute('''
                CREATE TABLE IF NOT EXISTS processed_data (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    batch_id TEXT NOT NULL,
                    record_hash TEXT UNIQUE,
                    data JSON NOT NULL,
                    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Create pipeline runs table
            conn.execute('''
                CREATE TABLE IF NOT EXISTS pipeline_runs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT UNIQUE NOT NULL,
                    status TEXT NOT NULL,
                    records_processed INTEGER,
                    errors INTEGER,
                    started_at TIMESTAMP,
                    completed_at TIMESTAMP,
                    details JSON
                )
            ''')
            
            conn.commit()
    
    @contextmanager
    def get_connection(self):
        """Get database connection with context manager"""
        conn = sqlite3.connect(self.config.database_path)
        try:
            yield conn
        finally:
            conn.close()
    
    def store_raw_data(self, data: List[Dict[str, Any]], source: str) -> int:
        """Store raw data"""
        stored_count = 0
        
        with self.get_connection() as conn:
            for record in data:
                record_hash = hashlib.md5(json.dumps(record, sort_keys=True).encode()).hexdigest()
                try:
                    conn.execute(
                        'INSERT OR IGNORE INTO raw_data (data_source, record_hash, data) VALUES (?, ?, ?)',
                        (source, record_hash, json.dumps(record))
                    )
                    stored_count += conn.rowcount
                except Exception as e:
                    self.logger.error(f"Error storing raw data: {str(e)}")
            
            conn.commit()
        
        self.logger.info(f"Stored {stored_count} raw records from {source}")
        return stored_count
    
    def store_processed_data(self, data: List[Dict[str, Any]], batch_id: str) -> int:
        """Store processed data"""
        stored_count = 0
        
        with self.get_connection() as conn:
            for record in data:
                record_hash = record.get('_record_hash', 'unknown')
                try:
                    conn.execute(
                        'INSERT OR REPLACE INTO processed_data (batch_id, record_hash, data) VALUES (?, ?, ?)',
                        (batch_id, record_hash, json.dumps(record))
                    )
                    stored_count += conn.rowcount
                except Exception as e:
                    self.logger.error(f"Error storing processed data: {str(e)}")
            
            conn.commit()
        
        self.logger.info(f"Stored {stored_count} processed records for batch {batch_id}")
        return stored_count
    
    def get_latest_data(self, table: str, limit: int = 100) -> List[Dict[str, Any]]:
        """Get latest data from specified table"""
        with self.get_connection() as conn:
            cursor = conn.execute(
                f'SELECT data FROM {table} ORDER BY id DESC LIMIT ?',
                (limit,)
            )
            results = [json.loads(row[0]) for row in cursor.fetchall()]
        
        return results

# Pipeline Orchestrator
class DataPipeline:
    """Main pipeline orchestrator"""
    
    def __init__(self, config: PipelineConfig):
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        self.storage = DataStorage(config)
        self.transformer = DataTransformer()
        self.sources = []
        
        # Create directories
        Path(config.raw_data_path).mkdir(parents=True, exist_ok=True)
        Path(config.processed_data_path).mkdir(parents=True, exist_ok=True)
    
    def add_source(self, source: DataSource, name: str):
        """Add a data source to the pipeline"""
        self.sources.append((source, name))
        self.logger.info(f"Added data source: {name}")
    
    async def run_pipeline(self) -> Dict[str, Any]:
        """Run the complete pipeline"""
        run_id = f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        start_time = datetime.now()
        
        self.logger.info(f"Starting pipeline run: {run_id}")
        
        # Initialize run tracking
        run_stats = {
            'run_id': run_id,
            'started_at': start_time.isoformat(),
            'sources_processed': 0,
            'total_records': 0,
            'errors': 0,
            'status': 'running'
        }
        
        try:
            # Extract phase
            all_raw_data = []
            for source, source_name in self.sources:
                try:
                    self.logger.info(f"Extracting from {source_name}")
                    data = await source.extract()
                    
                    # Store raw data
                    stored_count = self.storage.store_raw_data(data, source_name)
                    all_raw_data.extend(data)
                    
                    run_stats['sources_processed'] += 1
                    run_stats['total_records'] += len(data)
                    
                except Exception as e:
                    self.logger.error(f"Error processing source {source_name}: {str(e)}")
                    run_stats['errors'] += 1
            
            # Transform phase
            self.logger.info("Starting transformation phase")
            cleaned_data = self.transformer.clean_data(all_raw_data)
            
            # Store processed data in batches
            batch_size = self.config.batch_size
            for i in range(0, len(cleaned_data), batch_size):
                batch = cleaned_data[i:i + batch_size]
                batch_id = f"{run_id}_batch_{i // batch_size + 1}"
                self.storage.store_processed_data(batch, batch_id)
            
            # Export processed data
            await self._export_processed_data(cleaned_data, run_id)
            
            # Update run stats
            run_stats['status'] = 'completed'
            run_stats['completed_at'] = datetime.now().isoformat()
            run_stats['duration_seconds'] = (datetime.now() - start_time).total_seconds()
            
            self.logger.info(f"Pipeline run {run_id} completed successfully")
            
        except Exception as e:
            run_stats['status'] = 'failed'
            run_stats['error'] = str(e)
            self.logger.error(f"Pipeline run {run_id} failed: {str(e)}")
        
        # Store run metadata
        self._store_run_metadata(run_stats)
        
        return run_stats
    
    async def _export_processed_data(self, data: List[Dict[str, Any]], run_id: str):
        """Export processed data to files"""
        if not data:
            return
        
        # Export as CSV
        df = pd.json_normalize(data)
        csv_path = Path(self.config.processed_data_path) / f"{run_id}_processed.csv"
        df.to_csv(csv_path, index=False)
        
        # Export as JSON
        json_path = Path(self.config.processed_data_path) / f"{run_id}_processed.json"
        with open(json_path, 'w') as f:
            json.dump(data, f, indent=2)
        
        self.logger.info(f"Exported processed data to {csv_path} and {json_path}")
    
    def _store_run_metadata(self, run_stats: Dict[str, Any]):
        """Store pipeline run metadata"""
        with self.storage.get_connection() as conn:
            conn.execute('''
                INSERT OR REPLACE INTO pipeline_runs 
                (run_id, status, records_processed, errors, started_at, completed_at, details)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                run_stats['run_id'],
                run_stats['status'],
                run_stats.get('total_records', 0),
                run_stats.get('errors', 0),
                run_stats.get('started_at'),
                run_stats.get('completed_at'),
                json.dumps(run_stats)
            ))
            conn.commit()
    
    def get_pipeline_status(self) -> Dict[str, Any]:
        """Get pipeline status and statistics"""
        with self.storage.get_connection() as conn:
            # Get recent runs
            cursor = conn.execute('''
                SELECT * FROM pipeline_runs 
                ORDER BY started_at DESC 
                LIMIT 10
            ''')
            
            runs = []
            for row in cursor.fetchall():
                runs.append({
                    'run_id': row[1],
                    'status': row[2],
                    'records_processed': row[3],
                    'errors': row[4],
                    'started_at': row[5],
                    'completed_at': row[6]
                })
            
            # Get data statistics
            raw_count = conn.execute('SELECT COUNT(*) FROM raw_data').fetchone()[0]
            processed_count = conn.execute('SELECT COUNT(*) FROM processed_data').fetchone()[0]
        
        return {
            'recent_runs': runs,
            'total_raw_records': raw_count,
            'total_processed_records': processed_count,
            'last_run': runs[0] if runs else None
        }

# Scheduler and Monitoring
class PipelineScheduler:
    """Handle pipeline scheduling and monitoring"""
    
    def __init__(self, pipeline: DataPipeline):
        self.pipeline = pipeline
        self.logger = logging.getLogger(self.__class__.__name__)
        self.is_running = False
    
    def schedule_daily(self, time_str: str = "02:00"):
        """Schedule pipeline to run daily"""
        schedule.every().day.at(time_str).do(self._run_scheduled_pipeline)
        self.logger.info(f"Scheduled daily pipeline run at {time_str}")
    
    def schedule_hourly(self):
        """Schedule pipeline to run hourly"""
        schedule.every().hour.do(self._run_scheduled_pipeline)
        self.logger.info("Scheduled hourly pipeline runs")
    
    def _run_scheduled_pipeline(self):
        """Run pipeline in scheduled mode"""
        if self.is_running:
            self.logger.warning("Pipeline already running, skipping scheduled run")
            return
        
        self.is_running = True
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            result = loop.run_until_complete(self.pipeline.run_pipeline())
            self.logger.info(f"Scheduled pipeline run completed: {result['status']}")
        except Exception as e:
            self.logger.error(f"Scheduled pipeline run failed: {str(e)}")
        finally:
            self.is_running = False
    
    def start_scheduler(self):
        """Start the scheduler"""
        self.logger.info("Starting pipeline scheduler")
        while True:
            schedule.run_pending()
            time.sleep(60)  # Check every minute

# Example usage and demo
async def demo_pipeline():
    """Demonstrate the pipeline with sample data"""
    
    # Setup
    config = PipelineConfig()
    logger = setup_logging(config.log_path)
    
    # Create pipeline
    pipeline = DataPipeline(config)
    
    # Add sample data sources
    # Note: These are examples - replace with your actual data sources
    
    # Example API source (using a mock data API)
    api_source = APIDataSource(
        base_url="https://jsonplaceholder.typicode.com",
        endpoints=["users", "posts"]
    )
    pipeline.add_source(api_source, "sample_api")
    
    # Example file source (create sample data if needed)
    sample_data = [
        {"id": 1, "name": "John Doe", "email": "john@example.com", "signup_date": "2024-01-15"},
        {"id": 2, "name": "Jane Smith", "email": "jane@example.com", "signup_date": "2024-02-20"},
        {"id": 3, "name": "Bob Johnson", "email": "bob@example.com", "signup_date": "2024-03-10"}
    ]
    
    # Create sample file
    sample_file_path = Path(config.raw_data_path) / "sample_users.json"
    with open(sample_file_path, 'w') as f:
        json.dump(sample_data, f)
    
    file_source = FileDataSource([str(sample_file_path)])
    pipeline.add_source(file_source, "sample_file")
    
    # Run pipeline
    logger.info("Running demo pipeline")
    result = await pipeline.run_pipeline()
    
    # Display results
    logger.info("Pipeline Results:")
    logger.info(f"Status: {result['status']}")
    logger.info(f"Total Records: {result['total_records']}")
    logger.info(f"Sources Processed: {result['sources_processed']}")
    logger.info(f"Errors: {result['errors']}")
    
    # Show pipeline status
    status = pipeline.get_pipeline_status()
    logger.info("Pipeline Status:")
    logger.info(f"Total Raw Records: {status['total_raw_records']}")
    logger.info(f"Total Processed Records: {status['total_processed_records']}")
    
    return result

if __name__ == "__main__":
    # Run the demo
    result = asyncio.run(demo_pipeline())
    
    # Example of setting up scheduler (commented out for demo)
    # config = PipelineConfig()
    # pipeline = DataPipeline(config)
    # scheduler = PipelineScheduler(pipeline)
    # scheduler.schedule_daily("02:00")
    # scheduler.start_scheduler()