from datetime import datetime

from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp, regexp_replace, col, floor, when, lpad, concat_ws, substring, \
    to_date

from src.config.logging_conf import logger
from src.log.etl_log import insert_etl_log


def transform_data(df: DataFrame, table_name: str, step: str, source: str) -> DataFrame:
    logger.info(f"Transforming : {table_name}")
    log_msg = {
        "step": step,
        "process": "transform",
        "status": "-",
        "source": source,
        "table_name": table_name,
        "error_msg": "-",
        "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    new_df = None

    try:
        if table_name == "education_status":
            new_df = transform_education_status(df)
        elif table_name == "marital_status":
            new_df = transform_marital_status(df)
        elif table_name == "marketing_campaign_deposit":
            new_df = transform_marketing_campaign(df)
        elif table_name == "customers":
            new_df = transform_customers(df)
        elif table_name == "transactions":
            new_df = transform_transactions(df)

        log_msg["status"] = "success"
    except Exception as e:
        logger.error(f"Error transforming {table_name}, error: {str(e)}")
        log_msg["status"] = "failed"
        log_msg["error_msg"] = str(e)
    finally:
        logger.info(f"Finished transforming {table_name} data")
        insert_etl_log(log_msg)
        return new_df


def transform_education_status(df: DataFrame) -> DataFrame:
    new_df = (
        df.withColumn("created_at", current_timestamp())
        .withColumn("updated_at", current_timestamp())
    )
    return new_df


def transform_marital_status(df: DataFrame) -> DataFrame:
    new_df = (
        df.withColumn("created_at", current_timestamp())
        .withColumn("updated_at", current_timestamp())
    )
    return new_df


def transform_marketing_campaign(df: DataFrame) -> DataFrame:
    new_df = (
        df.withColumn("balance", regexp_replace(col("balance"), "\\$", "").cast("int"))
        .withColumn("duration", floor(col("duration") / 365).cast("int"))
        .withColumn("created_at", current_timestamp())
        .withColumn("updated_at", current_timestamp())
        .withColumnRenamed("pdays", "days_since_last_campaign")
        .withColumnRenamed("previous", "previous_campaign_contacts")
        .withColumnRenamed("poutcome", "previous_campaign_outcome")
    )
    return new_df


def transform_customers(df: DataFrame) -> DataFrame:
    new_df = (
        df.withColumn("CustomerDOB", to_date(df["CustomerDOB"], "d/M/yy"))
        .withColumn("CustGender",
                    when(col("CustGender") == "M", "Male")
                    .when(col("CustGender") == "F", "Female")
                    .otherwise("Other"))
        .withColumn("CustAccountBalance", col("CustAccountBalance").cast("decimal(10,2)"))
        .withColumn("created_at", current_timestamp())
        .withColumn("updated_at", current_timestamp())
        .withColumnRenamed("CustomerID", "customer_id")
        .withColumnRenamed("CustomerDOB", "birth_date")
        .withColumnRenamed("CustGender", "gender")
        .withColumnRenamed("CustLocation", "location")
        .withColumnRenamed("CustAccountBalance", "account_balance")
    )
    return new_df


def transform_transactions(df: DataFrame) -> DataFrame:
    new_df = (
        df.withColumn("TransactionDate", to_date(df["TransactionDate"], "d/M/yy"))
        .withColumn("TransactionTime",
                    concat_ws(":",
                              substring(lpad(col("TransactionTime"), 6, "0"), 1, 2),
                              substring(lpad(col("TransactionTime"), 6, "0"), 3, 2),
                              substring(lpad(col("TransactionTime"), 6, "0"), 5, 2)
                              )
                    )
        .withColumn("TransactionAmount (INR)", col("TransactionAmount (INR)").cast("decimal(10,2)"))
        .withColumnRenamed("TransactionID", "transaction_id")
        .withColumnRenamed("CustomerID", "customer_id")
        .withColumnRenamed("TransactionDate", "transaction_date")
        .withColumnRenamed("TransactionTime", "transaction_time")
        .withColumnRenamed("TransactionAmount (INR)", "transaction_amount")
    )
    return new_df
