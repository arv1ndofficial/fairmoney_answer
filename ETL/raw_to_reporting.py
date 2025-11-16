#!/usr/bin/env python3
"""
FairMoney ETL Pipeline: RAW to REPORTING Layer
Simple functional approach for transforming RAW data to dimensional model
"""
#!/usr/bin/env python3
import sys
import logging
import traceback
from datetime import datetime, timedelta, date
import os
import json
import psycopg2
from psycopg2.extras import execute_values
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, current_timestamp, when, coalesce,
    date_format, to_date, year, month, dayofmonth, dayofweek,
    quarter, concat, floor, datediff, last_day
)
from pyspark.sql.types import IntegerType, DateType
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
def get_db_configs():
    return {
        'raw': {
            'url': os.getenv('RAW_DB_URL', 'jdbc:postgresql://localhost:5432/fairmoney_raw'),
            'user': os.getenv('RAW_DB_USER', 'postgres'),
            'password': os.getenv('RAW_DB_PASSWORD', 'password'),
            'driver': 'org.postgresql.Driver'
        },
        'reporting': {
            'host': os.getenv('REPORTING_DB_HOST', 'localhost'),
            'port': int(os.getenv('REPORTING_DB_PORT', '5432')),
            'dbname': os.getenv('REPORTING_DB_NAME', 'fairmoney_reporting'),
            'user': os.getenv('REPORTING_DB_USER', 'postgres'),
            'password': os.getenv('REPORTING_DB_PASSWORD', 'password'),
            'jdbc_url': os.getenv('REPORTING_DB_JDBC', 'jdbc:postgresql://localhost:5432/fairmoney_reporting'),
            'jdbc_driver': 'org.postgresql.Driver'
        }
    }
def pg_connect(cfg):
    return psycopg2.connect(
        host=cfg['host'],
        port=cfg['port'],
        dbname=cfg['dbname'],
        user=cfg['user'],
        password=cfg['password']
    )
def execute_sql(conn, sql, params=None):
    with conn.cursor() as cur:
        cur.execute(sql, params)
    conn.commit()
def upsert_dataframe_to_table(conn, df_rows, table_name, columns, conflict_target, update_columns):
    if not df_rows:
        return
    cols_sql = ','.join(columns)
    update_sql = ','.join([f"{c}=EXCLUDED.{c}" for c in update_columns])
    sql = f"INSERT INTO reporting.{table_name} ({cols_sql}) VALUES %s ON CONFLICT {conflict_target} DO UPDATE SET {update_sql};"
    with conn.cursor() as cur:
        execute_values(cur, sql, df_rows, template=None, page_size=1000)
    conn.commit()
def run_etl_pipeline(run_date=None):
    if not run_date:
        run_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    batch_id = f"raw_to_reporting_idempotent_{run_date}_{datetime.now().strftime('%H%M%S')}"
    logger.info(f"Starting idempotent RAW->REPORTING ETL for {run_date} batch_id={batch_id}")
    db_cfgs = get_db_configs()
    reporting_cfg = db_cfgs['reporting']
    spark = SparkSession.builder.appName(f"FairMoney-RAW-to-REPORTING-idempotent-{batch_id}").config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    def read_raw(table):
        return spark.read.format("jdbc").option("url", db_cfgs['raw']['url']).option("dbtable", f"raw.{table}").option("user", db_cfgs['raw']['user']).option("password", db_cfgs['raw']['password']).option("driver", db_cfgs['raw']['driver']).load()
    try:
        start_date = date(2020, 1, 1)
        end_date = date(2030, 12, 31)
        days = []
        d = start_date
        while d <= end_date:
            days.append((d,))
            d = d + timedelta(days=1)
        date_df = spark.createDataFrame(days, ["date_value"])
        dim_date = date_df.select(
            date_format(col("date_value"), "yyyyMMdd").cast(IntegerType()).alias("date_key"),
            col("date_value").alias("full_date"),
            year(col("date_value")).alias("year"),
            quarter(col("date_value")).alias("quarter"),
            month(col("date_value")).alias("month"),
            dayofmonth(col("date_value")).alias("day"),
            date_format(col("date_value"), "EEEE").alias("day_name"),
            date_format(col("date_value"), "MMMM").alias("month_name"),
            concat(lit("Q"), quarter(col("date_value"))).alias("quarter_name"),
            date_format(col("date_value"), "yyyyMM").alias("year_month"),
            concat(year(col("date_value")), lit("-Q"), quarter(col("date_value"))).alias("year_quarter"),
            when(dayofweek(col("date_value")).isin([1,7]), True).otherwise(False).alias("is_weekend"),
            (col("date_value") == last_day(col("date_value"))).alias("is_month_end"),
            ((month(col("date_value")) % 3 == 0) & (col("date_value") == last_day(col("date_value")))).alias("is_quarter_end"),
            ((col("date_value") == last_day(col("date_value"))) & (month(col("date_value")) == 12)).alias("is_year_end"),
            year(col("date_value")).alias("fiscal_year"),
            quarter(col("date_value")).alias("fiscal_quarter"),
            current_timestamp().alias("created_at")
        )
        dim_date.write.format("jdbc").option("url", reporting_cfg['jdbc_url']).option("dbtable", "reporting.dim_date").option("user", reporting_cfg['user']).option("password", reporting_cfg['password']).option("driver", reporting_cfg['jdbc_driver']).mode("overwrite").save()
        customers_df = read_raw("customers").cache()
        countries_df = read_raw("countries").cache()
        products_df = read_raw("loan_products").cache()
        loans_df = read_raw("loans").cache()
        payments_df = read_raw("payments").cache()
        applications_df = read_raw("loan_applications").cache()
        emi_df = read_raw("emi_schedule").cache()
        collections_df = read_raw("collections").cache()
        customer_kyc_df = read_raw("customer_kyc").cache()
        kyc_latest = customer_kyc_df.withColumn("rn", row_number().over(Window.partitionBy("customer_id").orderBy(col("verified_at").desc_nulls_last()))).filter(col("rn") == 1).select("customer_id", "verification_status", "verified_at")
        dim_customer_df = customers_df.alias("c").join(countries_df.alias("co"), col("c.country_id") == col("co.country_id"), "left").join(kyc_latest.alias("k"), col("c.customer_id") == col("k.customer_id"), "left").select(
            col("c.customer_id").alias("customer_id"),
            coalesce(col("co.country_code"), lit("UNKNOWN")).alias("country_code"),
            coalesce(col("co.country_name"), lit("UNKNOWN")).alias("country_name"),
            coalesce(col("co.currency_code"), lit("UNKNOWN")).alias("currency_code"),
            coalesce(concat(col("c.first_name"), lit(" "), col("c.last_name")), lit("UNKNOWN")).alias("customer_name"),
            col("c.phone_number").alias("phone_number"),
            col("c.email").alias("email"),
            col("c.date_of_birth").alias("date_of_birth"),
            (((floor(datediff(current_timestamp(), col("c.date_of_birth")) / 365.25))).cast("integer")).alias("age_years"),
            coalesce(col("c.customer_status"), lit("UNKNOWN")).alias("customer_status"),
            col("c.customer_segment").alias("customer_segment"),
            year(col("c.registration_date")).alias("registration_year"),
            month(col("c.registration_date")).alias("registration_month"),
            date_format(col("c.registration_date"), "yyyy-MM").alias("registration_cohort"),
            col("k.verification_status").alias("kyc_status"),
            col("k.verified_at").alias("kyc_verified_date"),
            to_date(lit(run_date)).alias("effective_date"),
            lit(None).cast(DateType()).alias("end_date"),
            lit(True).alias("is_current"),
            current_timestamp().alias("created_at"),
            current_timestamp().alias("updated_at")
        )
        dim_customer_df = dim_customer_df.withColumn("age_group", when(col("age_years") < 18, "0-17").when(col("age_years").between(18,24), "18-24").when(col("age_years").between(25,34), "25-34").when(col("age_years").between(35,44), "35-44").otherwise("45+")).drop("age_years")
        dim_customer_rows = [tuple(r) for r in dim_customer_df.collect()]
        customer_cols = ["customer_id", "country_code", "country_name", "currency_code", "customer_name", "phone_number", "email", "date_of_birth", "age_group", "customer_status", "customer_segment", "registration_year", "registration_month", "registration_cohort", "kyc_status", "kyc_verified_date", "effective_date", "end_date", "is_current", "created_at", "updated_at"]
        conn = pg_connect(reporting_cfg)
        upsert_dataframe_to_table(conn, dim_customer_rows, "dim_customer", customer_cols, "(customer_id)", [c for c in customer_cols if c not in ("customer_id","created_at")])
        conn.close()
        dim_product_df = products_df.alias("p").join(countries_df.alias("c"), col("p.country_id") == col("c.country_id"), "left").select(
            col("p.product_id").alias("product_id"),
            coalesce(col("c.country_code"), lit("UNKNOWN")).alias("country_code"),
            coalesce(col("c.country_name"), lit("UNKNOWN")).alias("country_name"),
            col("p.product_name").alias("product_name"),
            col("p.product_code").alias("product_code"),
            coalesce(col("p.product_category"), lit("UNKNOWN")).alias("product_category"),
            coalesce(col("p.min_amount"), lit(0)).alias("min_amount"),
            coalesce(col("p.max_amount"), lit(0)).alias("max_amount"),
            when(coalesce(col("p.min_amount"), lit(0)) < 10000, "micro").when(coalesce(col("p.min_amount"), lit(0)) < 50000, "small").otherwise("medium").alias("amount_tier"),
            coalesce(col("p.min_tenure_days"), lit(0)).alias("min_tenure_days"),
            coalesce(col("p.max_tenure_days"), lit(0)).alias("max_tenure_days"),
            when(coalesce(col("p.max_tenure_days"), lit(0)) <= 30, "short").when(coalesce(col("p.max_tenure_days"), lit(0)) <= 90, "medium").otherwise("long").alias("tenure_tier"),
            coalesce(col("p.interest_rate"), lit(0.0)).alias("interest_rate"),
            when(col("p.interest_rate") < 0.1, "low").when(col("p.interest_rate") < 0.25, "medium").otherwise("high").alias("interest_tier"),
            coalesce(col("p.processing_fee_rate"), lit(0)).alias("processing_fee_rate"),
            lit(None).alias("risk_tier"),
            coalesce(col("p.is_active"), lit(True)).alias("is_active"),
            to_date(lit(run_date)).alias("effective_date"),
            lit(None).cast(DateType()).alias("updated_at"),
            current_timestamp().alias("created_at")
        )
        dim_product_rows = [tuple(r) for r in dim_product_df.collect()]
        product_cols = ["product_id", "country_code", "country_name", "product_name", "product_code", "product_category", "min_amount", "max_amount", "amount_tier", "min_tenure_days", "max_tenure_days", "tenure_tier", "interest_rate", "interest_tier", "processing_fee_rate", "risk_tier", "is_active", "effective_date", "updated_at", "created_at"]
        conn = pg_connect(reporting_cfg)
        upsert_dataframe_to_table(conn, dim_product_rows, "dim_product", product_cols, "(product_id)", [c for c in product_cols if c != "product_id"])
        conn.close()
        loans_win = Window.partitionBy("customer_id").orderBy(col("disbursement_date").asc_nulls_last())
        dim_loan_df = loans_df.withColumn("vintage_year", year(col("disbursement_date"))).withColumn("vintage_month", month(col("disbursement_date"))).withColumn("tenure_months", (col("tenure_days") / 30).cast("integer")).withColumn("customer_loan_sequence", row_number().over(loans_win)).withColumn("is_first_loan", (col("customer_loan_sequence") == 1).cast("boolean")).select(
            col("loan_id").alias("loan_id"),
            col("loan_number").alias("loan_number"),
            coalesce(col("loan_status"), lit("UNKNOWN")).alias("loan_status"),
            lit(None).alias("loan_type"),
            col("vintage_year"),
            col("vintage_month"),
            concat(year(col("disbursement_date")), lit("-"), month(col("disbursement_date"))).alias("vintage_cohort"),
            year(col("maturity_date")).alias("maturity_year"),
            month(col("maturity_date")).alias("maturity_month"),
            col("tenure_days"),
            col("tenure_months"),
            when(col("tenure_months") <= 1, "short").when(col("tenure_months") <= 3, "medium").otherwise("long").alias("tenure_tier"),
            col("is_first_loan"),
            col("customer_loan_sequence"),
            current_timestamp().alias("created_at"),
            col("principal_amount"),
            col("interest_amount"),
            col("total_amount")
        )
        dim_loan_rows = [tuple(r) for r in dim_loan_df.collect()]
        loan_cols = ["loan_id", "loan_number", "loan_status", "loan_type", "vintage_year", "vintage_month", "vintage_cohort", "maturity_year", "maturity_month", "tenure_days", "tenure_months", "tenure_tier", "is_first_loan", "customer_loan_sequence", "created_at", "principal_amount", "interest_amount", "total_amount"]
        conn = pg_connect(reporting_cfg)
        upsert_dataframe_to_table(conn, dim_loan_rows, "dim_loan", loan_cols, "(loan_id)", [c for c in loan_cols if c != "loan_id"])
        conn.close()
        pm = payments_df.select("payment_method").distinct().na.fill("UNKNOWN")
        dim_pm_df = pm.select(
            col("payment_method"),
            when(col("payment_method").like("%MOBILE%"), lit("Mobile")).when(col("payment_method").like("%BANK%"), lit("Bank")).otherwise(lit("Other")).alias("payment_channel"),
            when(col("payment_method").like("%MOBILE%"), lit("Digital")).when(col("payment_method").like("%BANK%"), lit("Digital")).otherwise(lit("Physical")).alias("payment_category"),
            when(col("payment_method").like("%CASH%"), lit(False)).otherwise(lit(True)).alias("is_digital"),
            lit(0).alias("processing_fee_rate"),
            current_timestamp().alias("created_at")
        )
        dim_pm_rows = [tuple(r) for r in dim_pm_df.collect()]
        pm_cols = ["payment_method", "payment_channel", "payment_category", "is_digital", "processing_fee_rate", "created_at"]
        conn = pg_connect(reporting_cfg)
        upsert_dataframe_to_table(conn, dim_pm_rows, "dim_payment_method", pm_cols, "(payment_method)", [c for c in pm_cols if c != "payment_method"])
        conn.close()
        dim_customer_map = spark.read.format("jdbc").option("url", reporting_cfg['jdbc_url']).option("dbtable","reporting.dim_customer").option("user", reporting_cfg['user']).option("password", reporting_cfg['password']).option("driver", reporting_cfg['jdbc_driver']).load().select("customer_key","customer_id")
        dim_product_map = spark.read.format("jdbc").option("url", reporting_cfg['jdbc_url']).option("dbtable","reporting.dim_product").option("user", reporting_cfg['user']).option("password", reporting_cfg['password']).option("driver", reporting_cfg['jdbc_driver']).load().select("product_key","product_id")
        dim_loan_map = spark.read.format("jdbc").option("url", reporting_cfg['jdbc_url']).option("dbtable","reporting.dim_loan").option("user", reporting_cfg['user']).option("password", reporting_cfg['password']).option("driver", reporting_cfg['jdbc_driver']).load().select("loan_key","loan_id")
        dim_payment_method_map = spark.read.format("jdbc").option("url", reporting_cfg['jdbc_url']).option("dbtable","reporting.dim_payment_method").option("user", reporting_cfg['user']).option("password", reporting_cfg['password']).option("driver", reporting_cfg['jdbc_driver']).load().select("payment_method_key","payment_method")
        fact_applications_df = applications_df.alias("a").join(dim_customer_map.alias("dc"), col("a.customer_id") == col("dc.customer_id"), "left").join(dim_product_map.alias("dp"), col("a.product_id") == col("dp.product_id"), "left").select(
            date_format(col("a.submitted_at"), "yyyyMMdd").cast(IntegerType()).alias("application_date_key"),
            col("dc.customer_key").alias("customer_key"),
            col("dp.product_key").alias("product_key"),
            col("a.application_id").alias("application_id"),
            col("a.application_number").alias("application_number"),
            col("a.requested_amount").alias("requested_amount"),
            coalesce(col("a.requested_tenure_days"), lit(0)).alias("requested_tenure_days"),
            col("a.application_status").alias("application_status"),
            coalesce(col("a.risk_score"), lit(0)).alias("risk_score"),
            when(col("a.application_status") == "APPROVED", lit(True)).otherwise(lit(False)).alias("is_approved"),
            when(col("a.application_status") == "REJECTED", lit(True)).otherwise(lit(False)).alias("is_rejected"),
            lit(False).alias("is_disbursed"),
            lit(False).alias("is_repeat_customer"),
            lit(None).cast(IntegerType()).alias("processing_time_hours"),
            lit(None).cast(IntegerType()).alias("approval_date_key"),
            lit(None).cast(IntegerType()).alias("disbursement_date_key"),
            current_timestamp().alias("created_at")
        )
        la = loans_df.select("application_id", "loan_id", "disbursement_date").filter(col("application_id").isNotNull())
        fact_applications_df = fact_applications_df.alias("fa").join(la.alias("la"), col("fa.application_id") == col("la.application_id"), "left").withColumn("is_disbursed", when(col("la.loan_id").isNotNull(), True).otherwise(col("is_disbursed"))).withColumn("disbursement_date_key", when(col("la.disbursement_date").isNotNull(), date_format(col("la.disbursement_date"), "yyyyMMdd").cast(IntegerType())).otherwise(lit(None).cast(IntegerType()))).drop("la.application_id")
        loans_per_customer = loans_df.groupBy("customer_id").count().withColumnRenamed("count","loan_count")
        apps_with_customer = applications_df.select("application_id","customer_id","submitted_at").alias("a")
        apps_with_counts = apps_with_customer.join(loans_per_customer.alias("lpc"), "customer_id", "left")
        repeat_map = apps_with_counts.select("application_id", (col("lpc.loan_count") > 0).alias("is_repeat"))
        fact_applications_df = fact_applications_df.alias("fa").join(repeat_map.alias("rm"), col("fa.application_id")==col("rm.application_id"), "left").withColumn("is_repeat_customer", coalesce(col("rm.is_repeat"), lit(False))).drop("rm.application_id")
        payments_with_loan = payments_df.alias("p").join(loans_df.select("loan_id","product_id").alias("l"), col("p.loan_id") == col("l.loan_id"), "left")
        fact_payments_df = payments_with_loan.join(dim_customer_map.alias("dc"), col("p.customer_id") == col("dc.customer_id"), "left").join(dim_product_map.alias("dp"), col("l.product_id") == col("dp.product_id"), "left").join(dim_payment_method_map.alias("pm"), col("p.payment_method") == col("pm.payment_method"), "left").join(dim_loan_map.alias("dl"), col("p.loan_id") == col("dl.loan_id"), "left").select(
            date_format(col("p.payment_date"), "yyyyMMdd").cast(IntegerType()).alias("payment_date_key"),
            col("dc.customer_key").alias("customer_key"),
            col("dp.product_key").alias("product_key"),
            col("dl.loan_key").alias("loan_key"),
            col("pm.payment_method_key").alias("payment_method_key"),
            col("p.payment_id").alias("payment_id"),
            col("p.payment_reference").alias("payment_reference"),
            col("p.amount").alias("payment_amount"),
            col("p.payment_type").alias("payment_type"),
            col("p.payment_status").alias("payment_status"),
            when(col("p.payment_status") == "COMPLETED", True).otherwise(False).alias("is_successful"),
            lit(None).cast("boolean").alias("is_on_time"),
            lit(None).cast("boolean").alias("is_early_payment"),
            lit(None).cast("boolean").alias("is_late_payment"),
            lit(None).cast(IntegerType()).alias("days_late"),
            col("p.payment_date").alias("original_payment_date"),
            current_timestamp().alias("created_at")
        )
        emi_next = emi_df.alias("e").select("loan_id","emi_number","due_date")
        pp = fact_payments_df.alias("fp").join(emi_next.alias("e"), col("fp.loan_key")==col("e.loan_id"), "left").withColumn("is_on_time", lit(None))
        snapshot_date_key = int(run_date.replace('-', ''))
        payments_agg = payments_df.groupBy("loan_id").sum("amount").withColumnRenamed("sum(amount)", "total_paid_amount")
        portfolio_df = loans_df.alias("l").join(dim_customer_map.alias("dc"), col("l.customer_id") == col("dc.customer_id"), "left").join(dim_product_map.alias("dp"), col("l.product_id") == col("dp.product_id"), "left").join(payments_agg.alias("pa"), col("l.loan_id") == col("pa.loan_id"), "left").select(
            lit(snapshot_date_key).alias("date_key"),
            col("dc.customer_key").alias("customer_key"),
            col("dp.product_key").alias("product_key"),
            col("l.loan_id").alias("loan_id"),
            col("l.principal_amount").alias("principal_amount"),
            col("l.interest_amount").alias("interest_amount"),
            col("l.total_amount").alias("total_loan_amount"),
            coalesce(col("l.outstanding_amount"), lit(0)).alias("total_outstanding"),
            coalesce(col("pa.total_paid_amount"), lit(0)).alias("total_paid_amount"),
            lit(0).alias("principal_paid"),
            lit(0).alias("interest_paid"),
            coalesce(col("l.days_past_due"), lit(0)).alias("days_past_due"),
            when(col("l.days_past_due") <= 0, lit("current")).when((col("l.days_past_due") > 0) & (col("l.days_past_due") <= 29), lit("0-29")).when((col("l.days_past_due") >= 30) & (col("l.days_past_due") <= 59), lit("30-59")).when((col("l.days_past_due") >= 60) & (col("l.days_past_due") <= 89), lit("60-89")).otherwise(lit("90+")).alias("dpd_bucket"),
            when(col("l.loan_status") == "ACTIVE", True).otherwise(False).alias("is_current"),
            when(col("l.days_past_due") > 0, True).otherwise(False).alias("is_overdue"),
            when(col("l.days_past_due") > 90, True).otherwise(False).alias("is_defaulted"),
            (col("l.days_past_due") >= 30).alias("ever_30_dpd"),
            (col("l.days_past_due") >= 60).alias("ever_60_dpd"),
            (col("l.days_past_due") >= 90).alias("ever_90_dpd"),
            current_timestamp().alias("created_at")
        )
        portfolio_df = portfolio_df.join(dim_loan_map.alias("dl"), col("loan_id") == col("dl.loan_id"), "left").select("date_key", "customer_key", "product_key", col("dl.loan_key").alias("loan_key"), "principal_amount", "interest_amount", "total_loan_amount", "total_outstanding", "total_paid_amount", "principal_paid", "interest_paid", "days_past_due", "dpd_bucket", "is_current", "is_overdue", "is_defaulted", "ever_30_dpd", "ever_60_dpd", "ever_90_dpd", "created_at")
        portfolio_rows = [tuple(r) for r in portfolio_df.collect()]
        portfolio_cols = ["date_key","customer_key","product_key","loan_key","principal_amount","interest_amount","total_loan_amount","total_outstanding","total_paid_amount","principal_paid","interest_paid","days_past_due","dpd_bucket","is_current","is_overdue","is_defaulted","ever_30_dpd","ever_60_dpd","ever_90_dpd","created_at"]
        conn = pg_connect(reporting_cfg)
        upsert_dataframe_to_table(conn, portfolio_rows, "fact_loan_portfolio", portfolio_cols, "(date_key, loan_key)", [c for c in portfolio_cols if c not in ("date_key","loan_key")])
        conn.close()
        collections_with_loan = collections_df.alias("co").join(loans_df.alias("l"), col("co.loan_id") == col("l.loan_id"), "left").join(dim_customer_map.alias("dc"), col("co.customer_id") == col("dc.customer_id"), "left").select(
            col("co.collection_id"),
            date_format(col("co.assignment_date"), "yyyyMMdd").cast(IntegerType()).alias("assignment_date_key"),
            col("dc.customer_key").alias("customer_key"),
            col("l.loan_id").alias("loan_id"),
            col("co.assigned_to").alias("assigned_agent"),
            col("co.case_status").alias("case_status"),
            lit(None).alias("priority_level"),
            col("l.outstanding_amount").alias("outstanding_at_assignment"),
            col("l.days_past_due").alias("days_past_due_at_assignment"),
            lit(0).alias("case_duration_days"),
            date_format(col("co.resolution_date"), "yyyyMMdd").cast(IntegerType()).alias("resolution_date_key"),
            lit(0).alias("amount_recovered"),
            lit(0.0).alias("recovery_rate"),
            when(col("co.case_status") == "RESOLVED", True).otherwise(False).alias("is_resolved"),
            lit(False).alias("is_successful_recovery"),
            current_timestamp().alias("created_at")
        )
        collections_with_loan = collections_with_loan.join(dim_loan_map.alias("dl"), col("loan_id") == col("dl.loan_id"), "left").select("collection_id","assignment_date_key","customer_key","dl.loan_key","assigned_agent","case_status","priority_level","outstanding_at_assignment","days_past_due_at_assignment","case_duration_days","resolution_date_key","amount_recovered","recovery_rate","is_resolved","is_successful_recovery","created_at")
        coll_rows = [tuple(r) for r in collections_with_loan.collect()]
        coll_cols = ["collection_id","assignment_date_key","customer_key","loan_key","assigned_agent","case_status","priority_level","outstanding_at_assignment","days_past_due_at_assignment","case_duration_days","resolution_date_key","amount_recovered","recovery_rate","is_resolved","is_successful_recovery","created_at"]
        conn = pg_connect(reporting_cfg)
        upsert_dataframe_to_table(conn, coll_rows, "fact_collections", coll_cols, "(collection_id)", [c for c in coll_cols if c != "collection_id"])
        conn.close()
        spark.stop()
        logger.info("RAW->REPORTING idempotent ETL completed successfully")
        return True
    except Exception as e:
        logger.error("ETL failed: %s", str(e))
        logger.error(traceback.format_exc())
        try:
            spark.stop()
        except:
            pass
        return False
if __name__ == "__main__":
    run_date = sys.argv[1] if len(sys.argv) > 1 else None
    success = run_etl_pipeline(run_date)
    if success:
        sys.exit(0)
    else:
        sys.exit(1)

