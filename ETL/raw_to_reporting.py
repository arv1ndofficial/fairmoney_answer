#!/usr/bin/env python3
"""FairMoney: RAW -> REPORTING ETL (refactored, modular, human-style)

Run:
    python etl_raw_to_reporting.py 2025-11-15

Notes:
- Requires pyspark and psycopg2 in the runtime environment.
- Expects RAW JDBC and REPORTING DB connection info via environment variables.
- This script writes staging tables to the reporting DB and executes server-side upserts.
"""

import os
import sys
import logging
from datetime import datetime, date, timedelta
from typing import List, Tuple

import psycopg2
from psycopg2.extras import execute_values
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, lit, current_timestamp, coalesce, concat, date_format, to_date,
    year, month, quarter, dayofmonth, dayofweek, when, floor, datediff, last_day
)
from pyspark.sql.types import IntegerType, DateType
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

LOG = logging.getLogger("etl")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def get_config():
    """Load DB connection strings and small settings from environment."""
    return {
        "raw_jdbc_url": os.getenv("RAW_DB_URL", "jdbc:postgresql://localhost:5432/fairmoney_raw"),
        "raw_user": os.getenv("RAW_DB_USER", "postgres"),
        "raw_password": os.getenv("RAW_DB_PASSWORD", "password"),
        "raw_driver": os.getenv("RAW_DB_DRIVER", "org.postgresql.Driver"),
        "reporting_host": os.getenv("REPORTING_DB_HOST", "localhost"),
        "reporting_port": int(os.getenv("REPORTING_DB_PORT", "5432")),
        "reporting_db": os.getenv("REPORTING_DB_NAME", "fairmoney_reporting"),
        "reporting_user": os.getenv("REPORTING_DB_USER", "postgres"),
        "reporting_password": os.getenv("REPORTING_DB_PASSWORD", "password"),
        "reporting_jdbc": os.getenv("REPORTING_DB_JDBC", "jdbc:postgresql://localhost:5432/fairmoney_reporting"),
        "reporting_driver": os.getenv("REPORTING_DB_JDBC_DRIVER", "org.postgresql.Driver"),
    }


def pg_connect(cfg):
    return psycopg2.connect(
        host=cfg["reporting_host"],
        port=cfg["reporting_port"],
        dbname=cfg["reporting_db"],
        user=cfg["reporting_user"],
        password=cfg["reporting_password"],
    )


def write_df_to_reporting_jdbc(df: DataFrame, jdbc_url: str, user: str, password: str, driver: str, table: str, mode: str = "overwrite"):
    """Write a Spark DataFrame to reporting via JDBC. Use mode=append/overwrite as needed."""
    df.write.format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", table) \
        .option("user", user) \
        .option("password", password) \
        .option("driver", driver) \
        .mode(mode) \
        .save()


def upsert_from_staging(
    conn,
    staging_table: str,
    target_table: str,
    key_cols: List[str],
    update_cols: List[str]
):
    """Upsert from staging table into target table using INSERT ... ON CONFLICT DO UPDATE.
    This runs in the reporting database and is atomic per statement.
    """
    all_cols = key_cols + update_cols
    col_list_sql = ", ".join(all_cols)
    conflict_target = "(" + ", ".join(key_cols) + ")"
    set_sql = ", ".join([f"{c}=EXCLUDED.{c}" for c in update_cols])

    sql = f"""
    INSERT INTO reporting.{target_table} ({col_list_sql})
    SELECT {col_list_sql} FROM reporting.{staging_table}
    ON CONFLICT {conflict_target} DO UPDATE
    SET {set_sql};
    """

    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()


def clear_staging(conn, staging_table: str):
    with conn.cursor() as cur:
        cur.execute(f"TRUNCATE TABLE reporting.{staging_table};")
    conn.commit()


def build_spark_session(app_name: str):
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def load_raw_tables(spark: SparkSession, cfg: dict) -> dict:
    """Read raw.* tables into a dictionary of DataFrames."""
    def read(table):
        return spark.read.format("jdbc") \
            .option("url", cfg["raw_jdbc_url"]) \
            .option("dbtable", f"raw.{table}") \
            .option("user", cfg["raw_user"]) \
            .option("password", cfg["raw_password"]) \
            .option("driver", cfg["raw_driver"]) \
            .load()

    LOG.info("Loading raw tables")
    tables = {}
    for name in ["countries", "customers", "customer_kyc", "loan_products", "loan_applications", "loans", "emi_schedule", "payments", "collections"]:
        tables[name] = read(name).cache()
        LOG.info("Loaded raw.%s rows=%s", name, tables[name].count())
    return tables


def build_dim_date(spark: SparkSession, run_date: str) -> DataFrame:
    start = date(2020, 1, 1)
    end = date(2030, 12, 31)
    days = []
    d = start
    while d <= end:
        days.append((d,))
        d += timedelta(days=1)
    df = spark.createDataFrame(days, ["date_value"])
    dd = df.select(
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
        when(dayofweek(col("date_value")).isin([1, 7]), True).otherwise(False).alias("is_weekend"),
        (col("date_value") == last_day(col("date_value"))).alias("is_month_end"),
        ((month(col("date_value")) % 3 == 0) & (col("date_value") == last_day(col("date_value")))).alias("is_quarter_end"),
        ((col("date_value") == last_day(col("date_value"))) & (month(col("date_value")) == 12)).alias("is_year_end"),
        year(col("date_value")).alias("fiscal_year"),
        quarter(col("date_value")).alias("fiscal_quarter"),
        current_timestamp().alias("created_at")
    )
    return dd


def build_dim_customer(raw_customers: DataFrame, raw_countries: DataFrame, raw_kyc: DataFrame, run_date: str) -> DataFrame:
    latest_kyc = (
        raw_kyc.withColumn("rn", row_number().over(Window.partitionBy("customer_id").orderBy(col("verified_at").desc_nulls_last())))
        .filter(col("rn") == 1)
        .select("customer_id", "verification_status", "verified_at")
    )

    df = (
        raw_customers.alias("c")
        .join(raw_countries.alias("co"), col("c.country_id") == col("co.country_id"), "left")
        .join(latest_kyc.alias("k"), col("c.customer_id") == col("k.customer_id"), "left")
        .select(
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
            current_timestamp().alias("updated_at"),
        )
    )

    df = (df.withColumn("age_group", 
            when(col("age_years") < 18, "0-17")
            .when(col("age_years").between(18, 24), "18-24")
            .when(col("age_years").between(25, 34), "25-34")
            .when(col("age_years").between(35, 44), "35-44")
            .otherwise("45+")
        ).drop("age_years")
    )

    return df


def build_dim_product(raw_products: DataFrame, raw_countries: DataFrame, run_date: str) -> DataFrame:
    df = (
        raw_products.alias("p")
        .join(raw_countries.alias("c"), col("p.country_id") == col("c.country_id"), "left")
        .select(
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
            current_timestamp().alias("created_at"),
        )
    )
    return df


def build_dim_loan(raw_loans: DataFrame) -> DataFrame:
    win = Window.partitionBy("customer_id").orderBy(col("disbursement_date").asc_nulls_last())
    df = (
        raw_loans
        .withColumn("vintage_year", year(col("disbursement_date")))
        .withColumn("vintage_month", month(col("disbursement_date")))
        .withColumn("tenure_months", (col("tenure_days") / 30).cast("integer"))
        .withColumn("customer_loan_sequence", row_number().over(win))
        .withColumn("is_first_loan", (col("customer_loan_sequence") == 1).cast("boolean"))
        .select(
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
            col("total_amount"),
        )
    )
    return df


def build_dim_payment_method(raw_payments: DataFrame) -> DataFrame:
    pm = raw_payments.select("payment_method").distinct().na.fill("UNKNOWN")
    df = pm.select(
        col("payment_method"),
        when(col("payment_method").like("%MOBILE%"), lit("Mobile")).when(col("payment_method").like("%BANK%"), lit("Bank")).otherwise(lit("Other")).alias("payment_channel"),
        when(col("payment_method").like("%MOBILE%"), lit("Digital")).when(col("payment_method").like("%BANK%"), lit("Digital")).otherwise(lit("Physical")).alias("payment_category"),
        when(col("payment_method").like("%CASH%"), lit(False)).otherwise(lit(True)).alias("is_digital"),
        lit(0).alias("processing_fee_rate"),
        current_timestamp().alias("created_at"),
    )
    return df


def build_fact_loan_applications(raw_apps: DataFrame, raw_loans: DataFrame, dim_customer_map: DataFrame, dim_product_map: DataFrame) -> DataFrame:
    apps = (
        raw_apps.alias("a")
        .join(dim_customer_map.alias("dc"), col("a.customer_id") == col("dc.customer_id"), "left")
        .join(dim_product_map.alias("dp"), col("a.product_id") == col("dp.product_id"), "left")
        .select(
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
            lit(None).cast("integer").alias("processing_time_hours"),
            lit(None).cast("integer").alias("approval_date_key"),
            lit(None).cast("integer").alias("disbursement_date_key"),
            current_timestamp().alias("created_at"),
        )
    )

    loans_map = raw_loans.select("application_id", "loan_id", "disbursement_date").filter(col("application_id").isNotNull())
    apps = apps.alias("fa").join(loans_map.alias("la"), col("fa.application_id") == col("la.application_id"), "left") \
        .withColumn("is_disbursed", when(col("la.loan_id").isNotNull(), True).otherwise(col("is_disbursed"))) \
        .withColumn("disbursement_date_key", when(col("la.disbursement_date").isNotNull(), date_format(col("la.disbursement_date"), "yyyyMMdd").cast(IntegerType())).otherwise(lit(None).cast(IntegerType()))) \
        .drop("la.application_id")

    # mark repeat customer: a simple heuristic — customer has previous loans
    loans_per_customer = raw_loans.groupBy("customer_id").count().withColumnRenamed("count", "loan_count")
    apps = apps.alias("fa").join(loans_per_customer.alias("lpc"), col("fa.customer_key") == col("lpc.customer_id"), "left") \
        .withColumn("is_repeat_customer", when(col("lpc.loan_count").isNotNull() & (col("lpc.loan_count") > 0), True).otherwise(False)) \
        .drop("lpc.loan_count")

    return apps


def build_fact_payments(raw_payments: DataFrame, raw_loans: DataFrame, dim_customer_map: DataFrame, dim_product_map: DataFrame, dim_payment_method_map: DataFrame, dim_loan_map: DataFrame) -> DataFrame:
    pay_with_loan = raw_payments.alias("p").join(raw_loans.select("loan_id", "product_id").alias("l"), col("p.loan_id") == col("l.loan_id"), "left")
    df = (
        pay_with_loan
        .join(dim_customer_map.alias("dc"), col("p.customer_id") == col("dc.customer_id"), "left")
        .join(dim_product_map.alias("dp"), col("l.product_id") == col("dp.product_id"), "left")
        .join(dim_payment_method_map.alias("pm"), col("p.payment_method") == col("pm.payment_method"), "left")
        .join(dim_loan_map.alias("dl"), col("p.loan_id") == col("dl.loan_id"), "left")
        .select(
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
            current_timestamp().alias("created_at"),
        )
    )
    return df


def build_fact_loan_portfolio(raw_loans: DataFrame, payments_agg: DataFrame, dim_customer_map: DataFrame, dim_product_map: DataFrame, dim_loan_map: DataFrame, snapshot_date_key: int) -> DataFrame:
    df = (
        raw_loans.alias("l")
        .join(dim_customer_map.alias("dc"), col("l.customer_id") == col("dc.customer_id"), "left")
        .join(dim_product_map.alias("dp"), col("l.product_id") == col("dp.product_id"), "left")
        .join(payments_agg.alias("pa"), col("l.loan_id") == col("pa.loan_id"), "left")
        .select(
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
            current_timestamp().alias("created_at"),
        )
    )

    df = df.join(dim_loan_map.alias("dl"), col("loan_id") == col("dl.loan_id"), "left").select(
        "date_key", "customer_key", "product_key", col("dl.loan_key").alias("loan_key"),
        "principal_amount", "interest_amount", "total_loan_amount", "total_outstanding", "total_paid_amount",
        "principal_paid", "interest_paid", "days_past_due", "dpd_bucket", "is_current", "is_overdue",
        "is_defaulted", "ever_30_dpd", "ever_60_dpd", "ever_90_dpd", "created_at"
    )
    return df


def build_fact_collections(raw_collections: DataFrame, raw_loans: DataFrame, dim_customer_map: DataFrame, dim_loan_map: DataFrame) -> DataFrame:
    df = (
        raw_collections.alias("co")
        .join(raw_loans.alias("l"), col("co.loan_id") == col("l.loan_id"), "left")
        .join(dim_customer_map.alias("dc"), col("co.customer_id") == col("dc.customer_id"), "left")
        .select(
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
            current_timestamp().alias("created_at"),
        )
    )
    df = df.join(dim_loan_map.alias("dl"), col("loan_id") == col("dl.loan_id"), "left").select(
        "collection_id", "assignment_date_key", "customer_key", col("dl.loan_key").alias("loan_key"),
        "assigned_agent", "case_status", "priority_level", "outstanding_at_assignment", "days_past_due_at_assignment",
        "case_duration_days", "resolution_date_key", "amount_recovered", "recovery_rate", "is_resolved", "is_successful_recovery", "created_at"
    )
    return df


def dataframe_to_staging_and_upsert(
    df: DataFrame,
    staging_table: str,
    target_table: str,
    key_cols: List[str],
    update_cols: List[str],
    reporting_cfg: dict
):
    # write staging (overwrite)
    write_df_to_reporting_jdbc(df, reporting_cfg["reporting_jdbc"], reporting_cfg["reporting_user"], reporting_cfg["reporting_password"], reporting_cfg["reporting_driver"], f"reporting.{staging_table}", mode="overwrite")

    # then upsert from staging -> target using psycopg2
    conn = pg_connect(reporting_cfg)
    try:
        upsert_from_staging(conn, staging_table, target_table, key_cols, update_cols)
    finally:
        conn.close()


def run(run_date: str):
    cfg = get_config()
    spark = build_spark_session(f"fairmoney_raw_to_reporting_{run_date}")
    raw = load_raw_tables(spark, cfg)

    # build basic dims
    dim_date = build_dim_date(spark, run_date)
    write_df_to_reporting_jdbc(dim_date, cfg["reporting_jdbc"], cfg["reporting_user"], cfg["reporting_password"], cfg["reporting_driver"], "reporting.dim_date", mode="overwrite")

    dim_customer = build_dim_customer(raw["customers"], raw["countries"], raw["customer_kyc"], run_date)
    dataframe_to_staging_and_upsert(
        dim_customer,
        staging_table="stg_dim_customer",
        target_table="dim_customer",
        key_cols=["customer_id"],
        update_cols=["country_code", "country_name", "currency_code", "customer_name", "phone_number", "email", "date_of_birth", "age_group", "customer_status", "customer_segment", "registration_year", "registration_month", "registration_cohort", "kyc_status", "kyc_verified_date", "effective_date", "end_date", "is_current", "created_at", "updated_at"],
        reporting_cfg=cfg
    )

    dim_product = build_dim_product(raw["loan_products"], raw["countries"], run_date)
    dataframe_to_staging_and_upsert(
        dim_product,
        staging_table="stg_dim_product",
        target_table="dim_product",
        key_cols=["product_id"],
        update_cols=["country_code","country_name","product_name","product_code","product_category","min_amount","max_amount","amount_tier","min_tenure_days","max_tenure_days","tenure_tier","interest_rate","interest_tier","processing_fee_rate","risk_tier","is_active","effective_date","updated_at","created_at"],
        reporting_cfg=cfg
    )

    dim_loan = build_dim_loan(raw["loans"])
    dataframe_to_staging_and_upsert(
        dim_loan,
        staging_table="stg_dim_loan",
        target_table="dim_loan",
        key_cols=["loan_id"],
        update_cols=["loan_number","loan_status","loan_type","vintage_year","vintage_month","vintage_cohort","maturity_year","maturity_month","tenure_days","tenure_months","tenure_tier","is_first_loan","customer_loan_sequence","created_at","principal_amount","interest_amount","total_amount"],
        reporting_cfg=cfg
    )

    dim_payment_method = build_dim_payment_method(raw["payments"])
    dataframe_to_staging_and_upsert(
        dim_payment_method,
        staging_table="stg_dim_payment_method",
        target_table="dim_payment_method",
        key_cols=["payment_method"],
        update_cols=["payment_channel","payment_category","is_digital","processing_fee_rate","created_at"],
        reporting_cfg=cfg
    )

    # Load dimension maps from reporting (for fact builds)
    jdbc_opts = {"url": cfg["reporting_jdbc"], "user": cfg["reporting_user"], "password": cfg["reporting_password"], "driver": cfg["reporting_driver"]}
    dim_customer_map = spark.read.format("jdbc").option("url", jdbc_opts["url"]).option("dbtable", "reporting.dim_customer").option("user", jdbc_opts["user"]).option("password", jdbc_opts["password"]).option("driver", jdbc_opts["driver"]).load().select("customer_key", "customer_id")
    dim_product_map = spark.read.format("jdbc").option("url", jdbc_opts["url"]).option("dbtable", "reporting.dim_product").option("user", jdbc_opts["user"]).option("password", jdbc_opts["password"]).option("driver", jdbc_opts["driver"]).load().select("product_key", "product_id")
    dim_loan_map = spark.read.format("jdbc").option("url", jdbc_opts["url"]).option("dbtable", "reporting.dim_loan").option("user", jdbc_opts["user"]).option("password", jdbc_opts["password"]).option("driver", jdbc_opts["driver"]).load().select("loan_key", "loan_id")
    dim_payment_method_map = spark.read.format("jdbc").option("url", jdbc_opts["url"]).option("dbtable", "reporting.dim_payment_method").option("user", jdbc_opts["user"]).option("password", jdbc_opts["password"]).option("driver", jdbc_opts["driver"]).load().select("payment_method_key", "payment_method")

    fact_applications = build_fact_loan_applications(raw["loan_applications"], raw["loans"], dim_customer_map, dim_product_map)
    dataframe_to_staging_and_upsert(
        fact_applications,
        staging_table="stg_fact_loan_applications",
        target_table="fact_loan_applications",
        key_cols=["application_id"],
        update_cols=["application_date_key","customer_key","product_key","application_number","requested_amount","requested_tenure_days","application_status","risk_score","is_approved","is_rejected","is_disbursed","is_repeat_customer","processing_time_hours","approval_date_key","disbursement_date_key","created_at"],
        reporting_cfg=cfg
    )

    fact_payments = build_fact_payments(raw["payments"], raw["loans"], dim_customer_map, dim_product_map, dim_payment_method_map, dim_loan_map)
    dataframe_to_staging_and_upsert(
        fact_payments,
        staging_table="stg_fact_payments",
        target_table="fact_payments",
        key_cols=["payment_id"],
        update_cols=["payment_date_key","customer_key","product_key","loan_key","payment_method_key","payment_reference","payment_amount","payment_type","payment_status","is_successful","is_on_time","is_early_payment","is_late_payment","days_late","original_payment_date","created_at"],
        reporting_cfg=cfg
    )

    payments_agg = raw["payments"].groupBy("loan_id").sum("amount").withColumnRenamed("sum(amount)", "total_paid_amount")
    fact_portfolio = build_fact_loan_portfolio(raw["loans"], payments_agg, dim_customer_map, dim_product_map, dim_loan_map, int(run_date.replace("-", "")))
    dataframe_to_staging_and_upsert(
        fact_portfolio,
        staging_table="stg_fact_loan_portfolio",
        target_table="fact_loan_portfolio",
        key_cols=["date_key", "loan_key"],
        update_cols=["customer_key","product_key","principal_amount","interest_amount","total_loan_amount","total_outstanding","total_paid_amount","principal_paid","interest_paid","days_past_due","dpd_bucket","is_current","is_overdue","is_defaulted","ever_30_dpd","ever_60_dpd","ever_90_dpd","created_at"],
        reporting_cfg=cfg
    )

    fact_collections = build_fact_collections(raw["collections"], raw["loans"], dim_customer_map, dim_loan_map)
    dataframe_to_staging_and_upsert(
        fact_collections,
        staging_table="stg_fact_collections",
        target_table="fact_collections",
        key_cols=["collection_id"],
        update_cols=["assignment_date_key","customer_key","loan_key","assigned_agent","case_status","priority_level","outstanding_at_assignment","days_past_due_at_assignment","case_duration_days","resolution_date_key","amount_recovered","recovery_rate","is_resolved","is_successful_recovery","created_at"],
        reporting_cfg=cfg
    )

    spark.stop()
    LOG.info("ETL run completed successfully")


if __name__ == "__main__":
    run_date_arg = sys.argv[1] if len(sys.argv) > 1 else (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    run(run_date_arg)
