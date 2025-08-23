# ====================
# 1. AIRFLOW DAG EXAMPLE
# ====================

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

def extract_data(**context):
    """Extract data from source system"""
    import pandas as pd
    # Simulate data extraction
    data = pd.DataFrame({
        'id': range(1, 101),
        'value': range(100, 200),
        'timestamp': pd.date_range('2024-01-01', periods=100, freq='H')
    })
    # Save to temporary location
    data.to_parquet('/tmp/extracted_data.parquet')
    return '/tmp/extracted_data.parquet'

def transform_data(**context):
    """Transform extracted data"""
    import pandas as pd
    file_path = context['task_instance'].xcom_pull(task_ids='extract')
    df = pd.read_parquet(file_path)
    
    # Apply transformations
    df['value_squared'] = df['value'] ** 2
    df['date'] = df['timestamp'].dt.date
    
    transformed_path = '/tmp/transformed_data.parquet'
    df.to_parquet(transformed_path)
    return transformed_path

# Define DAG
dag = DAG(
    'etl_pipeline',
    default_args=default_args,
    description='ETL pipeline example',
    schedule_interval='@daily',
    catchup=False,
    tags=['etl', 'example']
)

# Define tasks
extract_task = PythonOperator(
    task_id='extract',
    python_callable=extract_data,
    dag=dag
)

transform_task = PythonOperator(
    task_id='transform',
    python_callable=transform_data,
    dag=dag
)

load_task = PostgresOperator(
    task_id='load',
    postgres_conn_id='postgres_default',
    sql="""
    COPY target_table FROM '{{ ti.xcom_pull(task_ids='transform') }}'
    WITH (FORMAT parquet);
    """,
    dag=dag
)

quality_check = BashOperator(
    task_id='data_quality_check',
    bash_command='python /opt/airflow/scripts/quality_check.py',
    dag=dag
)

# Set dependencies
extract_task >> transform_task >> load_task >> quality_check

# ====================
# 2. DOCKER DEPLOYMENT
# ====================

# Dockerfile
"""
FROM python:3.9-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY src/ ./src/
COPY config/ ./config/

# Create non-root user
RUN useradd -m -u 1000 appuser && chown -R appuser:appuser /app
USER appuser

EXPOSE 8000

CMD ["python", "src/main.py"]
"""

# docker-compose.yml
"""
version: '3.8'

services:
  etl-app:
    build: .
    environment:
      - DATABASE_URL=${DATABASE_URL}
      - AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID}
      - AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY}
    volumes:
      - ./logs:/app/logs
      - ./data:/app/data
    depends_on:
      - postgres
      - redis
    networks:
      - etl-network

  postgres:
    image: postgres:13
    environment:
      POSTGRES_DB: etl_db
      POSTGRES_USER: etl_user
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
    volumes:
      - postgres_data:/var/lib/postgresql/data
    ports:
      - "5432:5432"
    networks:
      - etl-network

  redis:
    image: redis:alpine
    ports:
      - "6379:6379"
    networks:
      - etl-network

volumes:
  postgres_data:

networks:
  etl-network:
    driver: bridge
"""

# ====================
# 3. CLOUD STORAGE (S3/GCS)
# ====================

import boto3
import pandas as pd
from google.cloud import storage
from io import BytesIO
import os

class CloudStorageManager:
    def __init__(self):
        # AWS S3 setup
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            region_name=os.getenv('AWS_REGION', 'us-east-1')
        )
        
        # Google Cloud Storage setup
        self.gcs_client = storage.Client()
    
    def upload_to_s3(self, dataframe, bucket_name, key):
        """Upload DataFrame to S3 as parquet"""
        buffer = BytesIO()
        dataframe.to_parquet(buffer, index=False)
        buffer.seek(0)
        
        self.s3_client.upload_fileobj(
            buffer, 
            bucket_name, 
            key,
            ExtraArgs={'ContentType': 'application/octet-stream'}
        )
        print(f"Uploaded to s3://{bucket_name}/{key}")
    
    def download_from_s3(self, bucket_name, key):
        """Download parquet file from S3 to DataFrame"""
        response = self.s3_client.get_object(Bucket=bucket_name, Key=key)
        return pd.read_parquet(BytesIO(response['Body'].read()))
    
    def upload_to_gcs(self, dataframe, bucket_name, blob_name):
        """Upload DataFrame to Google Cloud Storage"""
        bucket = self.gcs_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        
        buffer = BytesIO()
        dataframe.to_parquet(buffer, index=False)
        buffer.seek(0)
        
        blob.upload_from_file(buffer, content_type='application/octet-stream')
        print(f"Uploaded to gs://{bucket_name}/{blob_name}")
    
    def download_from_gcs(self, bucket_name, blob_name):
        """Download parquet file from GCS to DataFrame"""
        bucket = self.gcs_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        
        buffer = BytesIO()
        blob.download_to_file(buffer)
        buffer.seek(0)
        return pd.read_parquet(buffer)

# Usage example
storage_manager = CloudStorageManager()

# Create sample data
df = pd.DataFrame({
    'date': pd.date_range('2024-01-01', periods=1000),
    'sales': range(1000, 2000),
    'region': ['US', 'EU', 'ASIA'] * 334
})

# Upload to both S3 and GCS
storage_manager.upload_to_s3(df, 'my-data-bucket', 'sales/2024/data.parquet')
storage_manager.upload_to_gcs(df, 'my-gcs-bucket', 'sales/2024/data.parquet')

# ====================
# 4. DATA WAREHOUSE CONNECTIONS
# ====================

import snowflake.connector
from google.cloud import bigquery
import pandas as pd
from sqlalchemy import create_engine

class DataWarehouseConnector:
    def __init__(self):
        self.snowflake_conn = None
        self.bigquery_client = None
        self.setup_connections()
    
    def setup_connections(self):
        # Snowflake connection
        self.snowflake_conn = snowflake.connector.connect(
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
            database=os.getenv('SNOWFLAKE_DATABASE'),
            schema=os.getenv('SNOWFLAKE_SCHEMA')
        )
        
        # BigQuery client
        self.bigquery_client = bigquery.Client(
            project=os.getenv('GCP_PROJECT_ID')
        )
    
    def execute_snowflake_query(self, query):
        """Execute query on Snowflake and return DataFrame"""
        cursor = self.snowflake_conn.cursor()
        try:
            cursor.execute(query)
            columns = [desc[0] for desc in cursor.description]
            data = cursor.fetchall()
            return pd.DataFrame(data, columns=columns)
        finally:
            cursor.close()
    
    def load_to_snowflake(self, dataframe, table_name, if_exists='append'):
        """Load DataFrame to Snowflake table"""
        engine = create_engine(
            f"snowflake://{os.getenv('SNOWFLAKE_USER')}:{os.getenv('SNOWFLAKE_PASSWORD')}"
            f"@{os.getenv('SNOWFLAKE_ACCOUNT')}/{os.getenv('SNOWFLAKE_DATABASE')}"
            f"/{os.getenv('SNOWFLAKE_SCHEMA')}?warehouse={os.getenv('SNOWFLAKE_WAREHOUSE')}"
        )
        
        dataframe.to_sql(
            table_name, 
            engine, 
            if_exists=if_exists, 
            index=False,
            method='multi'
        )
    
    def execute_bigquery_query(self, query):
        """Execute query on BigQuery and return DataFrame"""
        query_job = self.bigquery_client.query(query)
        return query_job.to_dataframe()
    
    def load_to_bigquery(self, dataframe, table_id, if_exists='append'):
        """Load DataFrame to BigQuery table"""
        job_config = bigquery.LoadJobConfig()
        
        if if_exists == 'replace':
            job_config.write_disposition = 'WRITE_TRUNCATE'
        else:
            job_config.write_disposition = 'WRITE_APPEND'
        
        job_config.autodetect = True
        
        job = self.bigquery_client.load_table_from_dataframe(
            dataframe, table_id, job_config=job_config
        )
        job.result()  # Wait for job to complete

# Usage example
dw = DataWarehouseConnector()

# Query from Snowflake
sf_data = dw.execute_snowflake_query("SELECT * FROM sales_data WHERE date >= '2024-01-01'")

# Query from BigQuery
bq_data = dw.execute_bigquery_query("""
    SELECT 
        product_id,
        SUM(revenue) as total_revenue
    FROM `project.dataset.sales`
    WHERE date >= '2024-01-01'
    GROUP BY product_id
""")

# ====================
# 5. SECRETS MANAGEMENT
# ====================

import os
import boto3
from google.cloud import secretmanager
import hvac  # HashiCorp Vault
from cryptography.fernet import Fernet

class SecretsManager:
    def __init__(self):
        self.aws_secrets = boto3.client('secretsmanager')
        self.gcp_secrets = secretmanager.SecretManagerServiceClient()
        self.vault_client = None
        self.setup_vault()
    
    def setup_vault(self):
        """Setup HashiCorp Vault client"""
        if os.getenv('VAULT_URL'):
            self.vault_client = hvac.Client(url=os.getenv('VAULT_URL'))
            self.vault_client.token = os.getenv('VAULT_TOKEN')
    
    def get_aws_secret(self, secret_name, region='us-east-1'):
        """Retrieve secret from AWS Secrets Manager"""
        try:
            response = self.aws_secrets.get_secret_value(
                SecretId=secret_name,
                VersionStage='AWSCURRENT'
            )
            return response['SecretString']
        except Exception as e:
            print(f"Error retrieving AWS secret: {e}")
            return None
    
    def get_gcp_secret(self, project_id, secret_id, version_id='latest'):
        """Retrieve secret from Google Secret Manager"""
        try:
            name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
            response = self.gcp_secrets.access_secret_version(request={"name": name})
            return response.payload.data.decode("UTF-8")
        except Exception as e:
            print(f"Error retrieving GCP secret: {e}")
            return None
    
    def get_vault_secret(self, path):
        """Retrieve secret from HashiCorp Vault"""
        if not self.vault_client:
            return None
        
        try:
            response = self.vault_client.secrets.kv.v2.read_secret_version(path=path)
            return response['data']['data']
        except Exception as e:
            print(f"Error retrieving Vault secret: {e}")
            return None
    
    def encrypt_local_secret(self, secret_value, key=None):
        """Encrypt secret locally using Fernet"""
        if not key:
            key = Fernet.generate_key()
        
        fernet = Fernet(key)
        encrypted_secret = fernet.encrypt(secret_value.encode())
        return encrypted_secret, key
    
    def decrypt_local_secret(self, encrypted_secret, key):
        """Decrypt locally encrypted secret"""
        fernet = Fernet(key)
        return fernet.decrypt(encrypted_secret).decode()

# Configuration class using secrets
class SecureConfig:
    def __init__(self):
        self.secrets_manager = SecretsManager()
        self.load_config()
    
    def load_config(self):
        """Load configuration with secrets from various sources"""
        # Try different secret sources in order of preference
        self.database_url = (
            self.secrets_manager.get_aws_secret('prod/database/url') or
            self.secrets_manager.get_gcp_secret('my-project', 'database-url') or
            os.getenv('DATABASE_URL')
        )
        
        self.api_keys = {
            'external_api': self.secrets_manager.get_vault_secret('api-keys/external'),
            'analytics': self.secrets_manager.get_aws_secret('prod/analytics/key')
        }

# ====================
# 6. MONITORING DASHBOARDS
# ====================

import psutil
import time
import logging
from prometheus_client import Counter, Histogram, Gauge, start_http_server
from datadog import initialize, statsd
import json

# Prometheus metrics
JOBS_TOTAL = Counter('etl_jobs_total', 'Total ETL jobs', ['status', 'job_type'])
JOB_DURATION = Histogram('etl_job_duration_seconds', 'ETL job duration', ['job_type'])
RECORDS_PROCESSED = Counter('etl_records_processed_total', 'Total records processed')
SYSTEM_MEMORY = Gauge('system_memory_usage_bytes', 'System memory usage')

class ETLMonitor:
    def __init__(self, datadog_api_key=None):
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
        
        # Setup Datadog if API key provided
        if datadog_api_key:
            initialize(api_key=datadog_api_key)
        
        # Start Prometheus metrics server
        start_http_server(8000)
    
    def log_job_start(self, job_name, job_type):
        """Log job start with metrics"""
        self.logger.info(f"Starting job: {job_name}")
        return time.time()
    
    def log_job_completion(self, job_name, job_type, start_time, records_count, status='success'):
        """Log job completion with metrics"""
        duration = time.time() - start_time
        
        # Update Prometheus metrics
        JOBS_TOTAL.labels(status=status, job_type=job_type).inc()
        JOB_DURATION.labels(job_type=job_type).observe(duration)
        RECORDS_PROCESSED.inc(records_count)
        
        # Send to Datadog
        statsd.increment(f'etl.jobs.{status}', tags=[f'job_type:{job_type}'])
        statsd.histogram(f'etl.job.duration', duration, tags=[f'job_type:{job_type}'])
        statsd.increment('etl.records.processed', records_count)
        
        self.logger.info(
            f"Job completed: {job_name}, Duration: {duration:.2f}s, "
            f"Records: {records_count}, Status: {status}"
        )
    
    def monitor_system_metrics(self):
        """Monitor system-level metrics"""
        while True:
            # Memory usage
            memory = psutil.virtual_memory()
            SYSTEM_MEMORY.set(memory.used)
            statsd.gauge('system.memory.used', memory.used)
            statsd.gauge('system.memory.percent', memory.percent)
            
            # CPU usage
            cpu_percent = psutil.cpu_percent(interval=1)
            statsd.gauge('system.cpu.percent', cpu_percent)
            
            # Disk usage
            disk = psutil.disk_usage('/')
            statsd.gauge('system.disk.used', disk.used)
            statsd.gauge('system.disk.percent', disk.percent)
            
            time.sleep(60)  # Update every minute
    
    def create_custom_dashboard_data(self, job_metrics):
        """Create custom dashboard data structure"""
        dashboard_data = {
            'timestamp': time.time(),
            'jobs': {
                'total_runs': sum(job_metrics.values()),
                'success_rate': job_metrics.get('success', 0) / sum(job_metrics.values()) * 100,
                'failure_rate': job_metrics.get('failure', 0) / sum(job_metrics.values()) * 100
            },
            'system': {
                'memory_usage_percent': psutil.virtual_memory().percent,
                'cpu_usage_percent': psutil.cpu_percent(),
                'disk_usage_percent': psutil.disk_usage('/').percent
            }
        }
        
        # Save to file for custom dashboard consumption
        with open('/tmp/etl_metrics.json', 'w') as f:
            json.dump(dashboard_data, f)
        
        return dashboard_data

# Usage examples
monitor = ETLMonitor(datadog_api_key=os.getenv('DATADOG_API_KEY'))

# Monitor a job
def sample_etl_job():
    start_time = monitor.log_job_start('daily_sales_etl', 'batch')
    
    try:
        # Simulate ETL work
        records_processed = 1500
        time.sleep(2)  # Simulate processing time
        
        monitor.log_job_completion(
            'daily_sales_etl', 
            'batch', 
            start_time, 
            records_processed, 
            'success'
        )
    except Exception as e:
        monitor.log_job_completion(
            'daily_sales_etl', 
            'batch', 
            start_time, 
            0, 
            'failure'
        )
        raise

# Run the sample job
sample_etl_job()

# ====================
# REQUIREMENTS.TXT
# ====================

"""
# Core data processing
pandas>=1.5.0
numpy>=1.20.0
sqlalchemy>=1.4.0

# Workflow orchestration
apache-airflow>=2.5.0
apache-airflow-providers-postgres
apache-airflow-providers-amazon
apache-airflow-providers-google

# Cloud storage
boto3>=1.26.0
google-cloud-storage>=2.7.0

# Data warehouses
snowflake-connector-python>=3.0.0
google-cloud-bigquery>=3.4.0

# Secrets management
cryptography>=3.4.0
hvac>=1.0.0  # HashiCorp Vault

# Monitoring
prometheus-client>=0.15.0
datadog>=0.44.0
psutil>=5.9.0

# Development
pytest>=7.0.0
black>=22.0
flake8>=5.0.0
"""