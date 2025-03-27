from datetime import datetime

from pyspark.sql import SparkSession, DataFrame

from src.config.logging_conf import logger
from src.config.variables import SRC_DB_URI, CONNECTION_PROPERTIES
from src.log.etl_log import insert_etl_log


def extract_src_db(spark: SparkSession, table_name: str, step: str, source: str) -> DataFrame:
    logger.info(f"Extracting {table_name} from {SRC_DB_URI}")

    log_msg = {
        "step": step,
        "process": "extraction",
        "status": "-",
        "source": source,
        "table_name": table_name,
        "error_msg": "-",
        "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    df = None

    try:
        log_msg["status"] = "success"

        # Read data from database table
        df = spark.read.jdbc(url=SRC_DB_URI, table=table_name, properties=CONNECTION_PROPERTIES)
    except Exception as e:
        logger.error(f"Can't extract {table_name} from {SRC_DB_URI}. Cause: {str(e)}")

        log_msg["status"] = "failed"
        log_msg["error_msg"] = str(e)

    finally:
        logger.info(f"Finished extracting {table_name} from {SRC_DB_URI}")

        insert_etl_log(log_msg)
        return df


def extract_src_csv(spark: SparkSession, path_file: str, table_name: str, step: str, source: str) -> DataFrame:
    logger.info(f"Extracting data from csv file: {path_file}")

    log_msg = {
        "step": step,
        "process": "extraction",
        "status": "-",
        "source": source,
        "table_name": table_name,
        "error_msg": "-",
        "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }

    df = None

    try:
        log_msg["status"] = "success"

        df = spark.read.csv(path_file, header=True, inferSchema=True)
        return df
    except Exception as e:
        logger.error(f"Can't extract {path_file}. Cause: {str(e)}")

        log_msg["status"] = "failed"
        log_msg["error_msg"] = str(e)
    finally:
        logger.info(f"Finished extracting data from csv file: {path_file}")
        insert_etl_log(log_msg)
        return df
