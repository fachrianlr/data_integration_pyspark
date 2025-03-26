from datetime import datetime

from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp, regexp_replace, col, floor, when, lpad, concat_ws, substring, \
    to_date

from src.config.logging_conf import logger
from src.log.etl_log import insert_etl_log


def transform_education_status(df: DataFrame) -> DataFrame:
    logger.info("Transforming education status data")
    log_msg = {
        "step": "warehouse",
        "process": "transform",
        "status": "-",
        "source": "database",
        "table_name": "education_status",
        "error_msg": "-",
        "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    new_df = None

    try:
        new_df = (
            df.withColumn("created_at", current_timestamp())
            .withColumn("updated_at", current_timestamp())
        )
        log_msg["status"] = "success"
    except Exception as e:
        logger.error(f"Error transforming education status data, error: {str(e)}")
        log_msg["status"] = "failed"
        log_msg["error_msg"] = str(e)
    finally:
        logger.info(f"Finished transforming education status data")
        insert_etl_log(log_msg)
        return new_df


def transform_marital_status(df: DataFrame) -> DataFrame:
    logger.info("Transforming marital status data")
    log_msg = {
        "step": "warehouse",
        "process": "transform",
        "status": "-",
        "source": "database",
        "table_name": "marital_status",
        "error_msg": "-",
        "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    new_df = None

    try:
        new_df = (
            df.withColumn("created_at", current_timestamp())
            .withColumn("updated_at", current_timestamp())
        )
        log_msg["status"] = "success"

    except Exception as e:
        logger.error(f"Error transforming marital status data, error: {str(e)}")
        log_msg["status"] = "failed"
        log_msg["error_msg"] = str(e)
    finally:
        logger.info(f"Finished transforming marital status data")
        insert_etl_log(log_msg)
        return new_df


def transform_marketing_campaign(df: DataFrame) -> DataFrame:
    logger.info("Transforming marketing campaign data")
    log_msg = {
        "step": "warehouse",
        "process": "transform",
        "status": "-",
        "source": "database",
        "table_name": "marketing_campaign",
        "error_msg": "-",
        "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    new_df = None

    try:
        new_df = (
            df.withColumn("balance", regexp_replace(col("balance"), "\\$", "").cast("int"))
            .withColumn("duration", floor(col("duration") / 365).cast("int"))
            .withColumn("created_at", current_timestamp())
            .withColumn("updated_at", current_timestamp())
            .withColumnRenamed("pdays", "days_since_last_campaign")
            .withColumnRenamed("previous", "previous_campaign_contacts")
            .withColumnRenamed("poutcome", "previous_campaign_outcome")
        )
        log_msg["status"] = "success"

    except Exception as e:
        logger.error(f"Error transforming marital status data, error: {str(e)}")
        log_msg["status"] = "failed"
        log_msg["error_msg"] = str(e)
    finally:
        logger.info(f"Finished transforming marital status data")
        insert_etl_log(log_msg)
        return new_df


def transform_customers(df: DataFrame) -> DataFrame:
    logger.info("Transforming customer data")
    log_msg = {
        "step": "warehouse",
        "process": "transform",
        "status": "-",
        "source": "csv",
        "table_name": "customers",
        "error_msg": "-",
        "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    new_df = None

    try:
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
        log_msg["status"] = "success"

    except Exception as e:
        logger.error(f"Error transforming customer data, error: {str(e)}")
        log_msg["status"] = "failed"
        log_msg["error_msg"] = str(e)
    finally:
        logger.info(f"Finished transforming customer data")
        insert_etl_log(log_msg)
        return new_df

def transform_transactions(df: DataFrame) -> DataFrame:
    logger.info("Transforming transaction data")
    log_msg = {
        "step": "warehouse",
        "process": "transform",
        "status": "-",
        "source": "csv",
        "table_name": "transactions",
        "error_msg": "-",
        "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    new_df = None

    try:
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
        log_msg["status"] = "success"

    except Exception as e:
        logger.error(f"Error transforming transaction, error: {str(e)}")
        log_msg["status"] = "failed"
        log_msg["error_msg"] = str(e)
    finally:
        logger.info(f"Finished transforming transaction")
        insert_etl_log(log_msg)
        return new_df

