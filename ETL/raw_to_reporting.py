#!/usr/bin/env python3
"""
FairMoney ETL Pipeline: RAW to REPORTING Layer
Simple functional approach for transforming RAW data to dimensional model
"""

import sys
import logging
import traceback
from datetime import datetime, timedelta
import os

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, lit, current_timestamp, when, coalesce,
    row_number, concat, date_format, to_date, 
    year, month, dayofmonth, dayofweek, quarter
)
from pyspark.sql.types import IntegerType, DateType, BooleanType
from pyspark.sql.window import Window

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def get_db_configs():
    """Get database connection configurations"""
    return {
        'raw': {
            'url': os.getenv('RAW_DB_URL', 'jdbc:postgresql://localhost:5432/fairmoney_raw'),
            'user': os.getenv('RAW_DB_USER', 'postgres'),
            'password': os.getenv('RAW_DB_PASSWORD', 'password'),
            'driver': 'org.postgresql.Driver'
        },
        'reporting': {
            'url': os.getenv('REPORTING_DB_URL', 'jdbc:postgresql://localhost:5432/fairmoney_reporting'),
            'user': os.getenv('REPORTING_DB_USER', 'postgres'),
            'password': os.getenv('REPORTING_DB_PASSWORD', 'password'),
            'driver': 'org.postgresql.Driver'
        }
    }


def get_transform_config():
    """Get configuration for dimension and fact table transformations"""
    return {
        'dimensions': [
            'dim_date',
            'dim_customer', 
            'dim_product',
            'dim_loan',
            'dim_payment_method'
        ],
        'facts': [
            'fact_loan_applications',
            'fact_loan_portfolio', 
            'fact_payments',
            'fact_collections'
        ],
        'raw_tables': [
            'customers',
            'countries',
            'loan_products',
            'loan_applications', 
            'loans',
            'payments',
            'collections'
        ]
    }
def run_etl_pipeline(run_date=None):
    """Main ETL function - transform RAW data to REPORTING dimensional model"""
    
    if not run_date:
        run_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    
    batch_id = f"raw_to_reporting_{run_date}_{datetime.now().strftime('%H%M%S')}"
    logger.info(f"Starting RAW to REPORTING ETL for {run_date}, batch_id: {batch_id}")
    
    db_configs = get_db_configs()
    transform_config = get_transform_config()
    
    spark = SparkSession.builder \
        .appName(f"FairMoney-RAW-to-REPORTING-{batch_id}") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    logger.info("Spark session created")
    
    def read_raw_table(table_name):
        """Read table from RAW layer"""
        return spark.read \
            .format("jdbc") \
            .option("url", db_configs['raw']['url']) \
            .option("dbtable", f"raw.{table_name}") \
            .option("user", db_configs['raw']['user']) \
            .option("password", db_configs['raw']['password']) \
            .option("driver", db_configs['raw']['driver']) \
            .load()
    
    def write_to_reporting(df, table_name, mode="overwrite"):
        """Write dataframe to reporting layer"""
        if df.count() == 0:
            logger.warning(f"No data for {table_name}")
            return True
        
        try:
            df.write \
                .format("jdbc") \
                .option("url", db_configs['reporting']['url']) \
                .option("dbtable", f"reporting.{table_name}") \
                .option("user", db_configs['reporting']['user']) \
                .option("password", db_configs['reporting']['password']) \
                .option("driver", db_configs['reporting']['driver']) \
                .mode(mode) \
                .save()
            
            logger.info(f"Written {df.count()} records to {table_name}")
            return True
        except Exception as e:
            logger.error(f"Failed to write {table_name}: {e}")
            return False
    
    try:
        success_count = 0
        total_tables = len(transform_config['dimensions']) + len(transform_config['facts'])
        
        # Build Date Dimension
        logger.info("Building dim_date")
        start_date = datetime(2023, 1, 1)
        end_date = datetime(2028, 12, 31)
        dates = []
        current = start_date
        while current <= end_date:
            dates.append((current,))
            current += timedelta(days=1)
        
        date_df = spark.createDataFrame(dates, ["date_value"])
        dim_date = date_df.select(
            date_format(col("date_value"), "yyyyMMdd").cast(IntegerType()).alias("date_key"),
            col("date_value"),
            year(col("date_value")).alias("year_number"),
            quarter(col("date_value")).alias("quarter_number"),
            month(col("date_value")).alias("month_number"),
            dayofmonth(col("date_value")).alias("day_of_month"),
            dayofweek(col("date_value")).alias("day_of_week"),
            when(dayofweek(col("date_value")).isin([1, 7]), True).otherwise(False).alias("is_weekend"),
            current_timestamp().alias("created_at")
        )
        
        if write_to_reporting(dim_date, "dim_date"):
            success_count += 1
        
        # Build Customer Dimension
        logger.info("Building dim_customer")
        customers_df = read_raw_table("customers")
        countries_df = read_raw_table("countries")
        
        dim_customer = customers_df.alias("c") \
            .join(countries_df.alias("co"), col("c.country_id") == col("co.country_id"), "left") \
            .select(
                row_number().over(Window.orderBy("c.customer_id")).alias("customer_key"),
                col("c.customer_id"),
                col("c.phone_number"),
                col("c.email"),
                col("c.first_name"),
                col("c.last_name"),
                concat(col("c.first_name"), lit(" "), col("c.last_name")).alias("full_name"),
                col("c.date_of_birth"),
                col("c.customer_status"),
                col("c.customer_segment"),
                col("c.registration_date"),
                col("co.country_name"),
                col("co.currency_code"),
                to_date(lit(run_date)).alias("effective_date"),
                lit(None).cast(DateType()).alias("expiry_date"),
                lit(True).alias("is_current"),
                current_timestamp().alias("created_at")
            )
        
        if write_to_reporting(dim_customer, "dim_customer"):
            success_count += 1
        
        # Build Product Dimension
        logger.info("Building dim_product")
        products_df = read_raw_table("loan_products")
        
        dim_product = products_df.alias("p") \
            .join(countries_df.alias("c"), col("p.country_id") == col("c.country_id"), "left") \
            .select(
                row_number().over(Window.orderBy("p.product_id")).alias("product_key"),
                col("p.product_id"),
                col("p.product_code"),
                col("p.product_name"),
                col("c.country_name"),
                col("p.min_amount"),
                col("p.max_amount"),
                col("p.interest_rate"),
                to_date(lit(run_date)).alias("effective_date"),
                lit(None).cast(DateType()).alias("expiry_date"),
                lit(True).alias("is_current"),
                current_timestamp().alias("created_at")
            )
        
        if write_to_reporting(dim_product, "dim_product"):
            success_count += 1
        
        # Build Loan Dimension
        logger.info("Building dim_loan")
        loans_df = read_raw_table("loans")
        
        dim_loan = loans_df.select(
            row_number().over(Window.orderBy("loan_id")).alias("loan_key"),
            col("loan_id"),
            col("loan_number"),
            col("principal_amount"),
            col("interest_amount"),
            col("total_amount"),
            col("tenure_days"),
            col("disbursement_date"),
            col("maturity_date"),
            col("loan_status"),
            current_timestamp().alias("created_at")
        )
        
        if write_to_reporting(dim_loan, "dim_loan"):
            success_count += 1
        
        # Build Payment Method Dimension
        logger.info("Building dim_payment_method")
        payments_df = read_raw_table("payments")
        unique_methods = payments_df.select("payment_method").distinct()
        
        dim_payment_method = unique_methods.select(
            row_number().over(Window.orderBy("payment_method")).alias("payment_method_key"),
            col("payment_method"),
            when(col("payment_method") == "MOBILE_MONEY", "Mobile Money Transfer")
                .when(col("payment_method") == "BANK_TRANSFER", "Bank Account Transfer")
                .when(col("payment_method") == "CASH", "Cash Payment")
                .otherwise("Other").alias("payment_method_description"),
            current_timestamp().alias("created_at")
        )
        
        if write_to_reporting(dim_payment_method, "dim_payment_method"):
            success_count += 1
        
        # Build Loan Applications Fact
        logger.info("Building fact_loan_applications")
        applications_df = read_raw_table("loan_applications")
        
        fact_applications = applications_df.alias("a") \
            .join(customers_df.alias("c"), col("a.customer_id") == col("c.customer_id"), "inner") \
            .join(products_df.alias("p"), col("a.product_id") == col("p.product_id"), "inner") \
            .select(
                row_number().over(Window.orderBy("a.application_id")).alias("loan_application_key"),
                col("a.application_id"),
                col("a.customer_id"),
                col("a.product_id"),
                date_format(col("a.submitted_at"), "yyyyMMdd").cast(IntegerType()).alias("application_date"),
                col("a.requested_amount"),
                coalesce(col("a.approved_amount"), lit(0)).alias("approved_amount"),
                col("a.application_status"),
                when(col("a.application_status") == "APPROVED", 1).otherwise(0).alias("is_approved_flag"),
                when(col("a.application_status") == "REJECTED", 1).otherwise(0).alias("is_rejected_flag"),
                current_timestamp().alias("created_at")
            )
        
        if write_to_reporting(fact_applications, "fact_loan_applications"):
            success_count += 1
        
        # Build Portfolio Fact
        logger.info("Building fact_loan_portfolio")
        snapshot_date_key = int(run_date.replace('-', ''))
        
        fact_portfolio = loans_df.alias("l") \
            .join(customers_df.alias("c"), col("l.customer_id") == col("c.customer_id"), "inner") \
            .join(products_df.alias("p"), col("l.product_id") == col("p.product_id"), "inner") \
            .select(
                row_number().over(Window.orderBy("l.loan_id")).alias("portfolio_key"),
                col("l.loan_id"),
                col("l.customer_id"),
                col("l.product_id"),
                lit(snapshot_date_key).alias("snapshot_date"),
                col("l.outstanding_amount"),
                col("l.principal_amount"),
                col("l.days_past_due"),
                when(col("l.loan_status") == "ACTIVE", 1).otherwise(0).alias("is_active_flag"),
                when(col("l.days_past_due") > 30, 1).otherwise(0).alias("is_par30_flag"),
                when(col("l.days_past_due") > 30, col("l.outstanding_amount"))
                    .otherwise(0).alias("par30_amount"),
                current_timestamp().alias("created_at")
            )
        
        if write_to_reporting(fact_portfolio, "fact_loan_portfolio", "append"):
            success_count += 1
        
        # Build Payments Fact
        logger.info("Building fact_payments")
        fact_payments = payments_df.alias("p") \
            .join(customers_df.alias("c"), col("p.customer_id") == col("c.customer_id"), "inner") \
            .select(
                row_number().over(Window.orderBy("p.payment_id")).alias("payment_key"),
                col("p.payment_id"),
                col("p.customer_id"),
                coalesce(col("p.loan_id"), lit(-1)).alias("loan_id"),
                col("p.payment_method"),
                date_format(col("p.payment_date"), "yyyyMMdd").cast(IntegerType()).alias("payment_date"),
                col("p.amount").alias("payment_amount"),
                col("p.payment_type"),
                col("p.payment_status"),
                when(col("p.payment_status") == "COMPLETED", 1).otherwise(0).alias("is_successful_flag"),
                current_timestamp().alias("created_at")
            )
        
        if write_to_reporting(fact_payments, "fact_payments"):
            success_count += 1
        
        # Build Collections Fact
        logger.info("Building fact_collections")
        collections_df = read_raw_table("collections")
        
        fact_collections = collections_df.alias("co") \
            .join(loans_df.alias("l"), col("co.loan_id") == col("l.loan_id"), "inner") \
            .join(customers_df.alias("c"), col("co.customer_id") == col("c.customer_id"), "inner") \
            .select(
                row_number().over(Window.orderBy("co.collection_id")).alias("collection_key"),
                col("co.collection_id"),
                col("co.customer_id"),
                col("co.loan_id"),
                date_format(col("co.created_at"), "yyyyMMdd").cast(IntegerType()).alias("collection_date"),
                coalesce(col("co.overdue_amount"), lit(0)).alias("overdue_amount"),
                coalesce(col("co.collected_amount"), lit(0)).alias("collected_amount"),
                col("co.days_overdue"),
                col("co.collection_status"),
                when(col("co.collection_status") == "RESOLVED", 1).otherwise(0).alias("is_resolved_flag"),
                current_timestamp().alias("created_at")
            )
        
        if write_to_reporting(fact_collections, "fact_collections"):
            success_count += 1
        
        pipeline_success = success_count == total_tables
        logger.info(f"ETL completed. Success: {success_count}/{total_tables} tables")
        
        if pipeline_success:
            logger.info("RAW to REPORTING ETL completed successfully!")
        else:
            logger.error(f"ETL completed with {total_tables - success_count} failures")
        
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
        run_date = sys.argv[1] if len(sys.argv) > 1 else None
        
        success = run_etl_pipeline(run_date)
        
        if success:
            logger.info("RAW to REPORTING pipeline completed successfully!")
            sys.exit(0)
        else:
            logger.error("RAW to REPORTING pipeline failed!")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"Pipeline execution failed: {str(e)}")
        logger.error(traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    main()
