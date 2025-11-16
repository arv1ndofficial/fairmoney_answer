#!/usr/bin/env python3
"""
FairMoney ETL Pipeline: OLTP to RAW Layer
===========================================

Simple functional approach for daily batch ETL from PostgreSQL OLTP to RAW layer
- Robust error handling and logging
- Idempotent execution (can be run multiple times safely)
- Fault tolerance with retry mechanisms

Author: FairMoney Data Engineering Team
Version: 1.0
Schedule: Daily at 2:00 AM
"""

import sys
import logging
import traceback
from datetime import datetime, timedelta
import os
import time

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit, current_timestamp
from pyspark.sql.types import DateType, BooleanType

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/opt/fairmoney/logs/etl_oltp_to_raw.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


def get_db_configs():
    """Get database connection configurations"""
    return {
        'oltp': {
            'url': os.getenv('OLTP_DB_URL', 'jdbc:postgresql://localhost:5432/fairmoney_oltp'),
            'user': os.getenv('OLTP_DB_USER', 'postgres'),
            'password': os.getenv('OLTP_DB_PASSWORD', 'password'),
            'driver': 'org.postgresql.Driver'
        },
        'raw': {
            'url': os.getenv('RAW_DB_URL', 'jdbc:postgresql://localhost:5432/fairmoney_raw'),
            'user': os.getenv('RAW_DB_USER', 'postgres'),
            'password': os.getenv('RAW_DB_PASSWORD', 'password'),
            'driver': 'org.postgresql.Driver'
        }
    }


def get_tables_config():
    """Get configuration for all tables to extract"""
    return {
        'countries': {
            'schema': 'oltp',
            'primary_key': 'country_id',
            'updated_column': 'created_at',
            'load_type': 'full'  # Reference data - always full load
        },
        'customers': {
            'schema': 'oltp',
            'primary_key': 'customer_id',
            'updated_column': 'updated_at',
            'load_type': 'incremental'
        },
        'customer_kyc': {
            'schema': 'oltp',
            'primary_key': 'kyc_id',
            'updated_column': 'created_at',
            'load_type': 'incremental'
        },
        'loan_products': {
            'schema': 'oltp',
            'primary_key': 'product_id',
            'updated_column': 'created_at',
            'load_type': 'full'  # Reference data
        },
        'loan_applications': {
            'schema': 'oltp',
            'primary_key': 'application_id',
            'updated_column': 'created_at',
            'load_type': 'incremental'
        },
        'loans': {
            'schema': 'oltp',
            'primary_key': 'loan_id',
            'updated_column': 'updated_at',
            'load_type': 'incremental'
        },
        'emi_schedule': {
            'schema': 'oltp',
            'primary_key': 'schedule_id',
            'updated_column': 'created_at',
            'load_type': 'incremental'
        },
        'payments': {
            'schema': 'oltp',
            'primary_key': 'payment_id',
            'updated_column': 'created_at',
            'load_type': 'incremental'
        },
        'collections': {
            'schema': 'oltp',
            'primary_key': 'collection_id',
            'updated_column': 'created_at',
            'load_type': 'incremental'
        }
    }
def run_etl_pipeline(run_date=None):
    """Main ETL function - extract data from OLTP and load to RAW layer"""
    
    # Setup
    if not run_date:
        run_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    
    batch_id = f"oltp_to_raw_{run_date}_{datetime.now().strftime('%H%M%S')}"
    logger.info(f"Starting ETL pipeline for {run_date}, batch_id: {batch_id}")
    
    # Get configs
    db_configs = get_db_configs()
    tables_config = get_tables_config()
    
    # Create Spark session
    spark = SparkSession.builder \
        .appName(f"FairMoney-OLTP-to-RAW-{batch_id}") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    logger.info("Spark session created successfully")
    
    try:
        # Process tables in order (reference tables first)
        table_order = [
            'countries', 'loan_products', 'customers', 'customer_kyc',
            'loan_applications', 'loans', 'emi_schedule', 'payments', 'collections'
        ]
        
        success_count = 0
        total_tables = len(table_order)
        
        for table_name in table_order:
            logger.info(f"Processing table: {table_name}")
            
            # Get table config
            table_config = tables_config[table_name]
            
            # Retry logic for each table
            max_retries = 3
            table_success = False
            
            for attempt in range(max_retries):
                try:
                    logger.info(f"Attempt {attempt + 1}/{max_retries} for {table_name}")
                    
                    # 1. Extract data from OLTP
                    base_query = f"SELECT * FROM {table_config['schema']}.{table_name}"
                    
                    # For incremental loads, try to get last successful timestamp
                    if table_config['load_type'] == 'incremental':
                        try:
                            last_run_query = f"""
                                SELECT MAX(ingestion_date) as last_run
                                FROM raw.{table_name}_audit
                                WHERE status = 'SUCCESS'
                            """
                            
                            last_run_df = spark.read \
                                .format("jdbc") \
                                .option("url", db_configs['raw']['url']) \
                                .option("dbtable", f"({last_run_query}) as lr") \
                                .option("user", db_configs['raw']['user']) \
                                .option("password", db_configs['raw']['password']) \
                                .option("driver", db_configs['raw']['driver']) \
                                .load()
                            
                            last_run = last_run_df.collect()[0]['last_run']
                            if last_run:
                                base_query += f" WHERE {table_config['updated_column']} > '{last_run}'"
                                logger.info(f"Incremental load since {last_run}")
                            
                        except Exception:
                            logger.info(f"Full load for {table_name} (no previous run found)")
                    
                    # Extract data
                    df = spark.read \
                        .format("jdbc") \
                        .option("url", db_configs['oltp']['url']) \
                        .option("dbtable", f"({base_query}) as extract") \
                        .option("user", db_configs['oltp']['user']) \
                        .option("password", db_configs['oltp']['password']) \
                        .option("driver", db_configs['oltp']['driver']) \
                        .option("fetchsize", "10000") \
                        .load()
                    
                    record_count = df.count()
                    logger.info(f"Extracted {record_count} records from {table_name}")
                    
                    # 2. Add audit columns
                    enriched_df = df.withColumn("ingestion_date", lit(run_date).cast(DateType())) \
                                   .withColumn("record_source", lit("OLTP_DAILY_BATCH")) \
                                   .withColumn("is_deleted", lit(False).cast(BooleanType())) \
                                   .withColumn("etl_created_at", current_timestamp()) \
                                   .withColumn("etl_updated_at", current_timestamp()) \
                                   .withColumn("batch_id", lit(batch_id))
                    
                    # 3. Basic data quality check
                    if record_count > 0:
                        # Check for nulls in primary key
                        primary_key = table_config['primary_key']
                        null_pk_count = enriched_df.filter(col(primary_key).isNull()).count()
                        if null_pk_count > 0:
                            logger.warning(f"{table_name}: {null_pk_count} records with null primary key")
                        
                        # Check for duplicates on primary key
                        unique_count = enriched_df.select(primary_key).distinct().count()
                        if unique_count != record_count:
                            duplicate_count = record_count - unique_count
                            logger.warning(f"{table_name}: {duplicate_count} duplicate records found")
                    
                    # 4. Load to RAW layer
                    if record_count > 0:
                        enriched_df.write \
                            .format("jdbc") \
                            .option("url", db_configs['raw']['url']) \
                            .option("dbtable", f"raw.{table_name}") \
                            .option("user", db_configs['raw']['user']) \
                            .option("password", db_configs['raw']['password']) \
                            .option("driver", db_configs['raw']['driver']) \
                            .mode("append") \
                            .save()
                        
                        logger.info(f"Successfully loaded {record_count} records to raw.{table_name}")
                    
                    # 5. Log audit record
                    try:
                        audit_data = [{
                            'table_name': table_name,
                            'batch_id': batch_id,
                            'ingestion_date': run_date,
                            'status': 'SUCCESS',
                            'records_processed': record_count,
                            'start_time': datetime.now(),
                            'end_time': datetime.now(),
                            'error_message': None
                        }]
                        
                        audit_df = spark.createDataFrame(audit_data)
                        audit_df.write \
                            .format("jdbc") \
                            .option("url", db_configs['raw']['url']) \
                            .option("dbtable", f"raw.{table_name}_audit") \
                            .option("user", db_configs['raw']['user']) \
                            .option("password", db_configs['raw']['password']) \
                            .option("driver", db_configs['raw']['driver']) \
                            .mode("append") \
                            .save()
                    except Exception as audit_error:
                        logger.warning(f"Failed to log audit record: {audit_error}")
                    
                    table_success = True
                    break  # Success, no need to retry
                    
                except Exception as e:
                    logger.error(f"Attempt {attempt + 1} failed for {table_name}: {str(e)}")
                    if attempt < max_retries - 1:
                        logger.info("Retrying in 30 seconds...")
                        time.sleep(30)
                    else:
                        logger.error(f"All retries exhausted for {table_name}")
                        
                        # Log failed audit record
                        try:
                            audit_data = [{
                                'table_name': table_name,
                                'batch_id': batch_id,
                                'ingestion_date': run_date,
                                'status': 'FAILED',
                                'records_processed': 0,
                                'start_time': datetime.now(),
                                'end_time': datetime.now(),
                                'error_message': str(e)
                            }]
                            
                            audit_df = spark.createDataFrame(audit_data)
                            audit_df.write \
                                .format("jdbc") \
                                .option("url", db_configs['raw']['url']) \
                                .option("dbtable", f"raw.{table_name}_audit") \
                                .option("user", db_configs['raw']['user']) \
                                .option("password", db_configs['raw']['password']) \
                                .option("driver", db_configs['raw']['driver']) \
                                .mode("append") \
                                .save()
                        except Exception:
                            logger.warning("Failed to log error audit record")
            
            if table_success:
                success_count += 1
        
        # Final result
        pipeline_success = success_count == total_tables
        logger.info(f"ETL completed. Success: {success_count}/{total_tables} tables")
        
        if pipeline_success:
            logger.info("ETL pipeline completed successfully!")
        else:
            logger.error(f"ETL pipeline completed with {total_tables - success_count} failures")
        
        return pipeline_success
        
    except Exception as e:
        logger.error(f"ETL pipeline failed: {str(e)}")
        logger.error(traceback.format_exc())
        return False
    
    finally:
        spark.stop()
        logger.info("Spark session stopped")

def main():
    """Main entry point - run the ETL pipeline"""
    try:
        # Get run date from command line or use yesterday
        run_date = sys.argv[1] if len(sys.argv) > 1 else None
        
        # Run the ETL pipeline
        success = run_etl_pipeline(run_date)
        
        if success:
            logger.info("Pipeline completed successfully!")
            sys.exit(0)
        else:
            logger.error("Pipeline failed!")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"Pipeline execution failed: {str(e)}")
        logger.error(traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    main()
