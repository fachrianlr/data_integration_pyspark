import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine

from src.config.db_conf import get_query_by_id
from src.config.logging_conf import logger
from src.config.variables import LOG_DB_URI_ACHEMY

load_dotenv()

ETL_LOG_PATH = "src/common/sql/etl_log.xml"


def insert_etl_log(log_msg: dict):
    """
    This function is used to save the log message to the database.
    """
    logger.info(f"insert etl log : {log_msg}")

    try:
        # create connection to database
        conn = create_engine(LOG_DB_URI_ACHEMY)
        sql_str = get_query_by_id(ETL_LOG_PATH, "insertEtlLog")

        with conn.connect() as conn:
            conn.execute(sql_str, log_msg)
            conn.commit()
        logger.info("Successfully saved etl log.")
    except Exception as e:
        logger.error(f"Can't save your log message. Cause: {str(e)}")


def read_etl_log(filter_params: dict):
    """
    This function read_etl_log that reads log information from the etl_log table and extracts the maximum etl_date for a specific process, step, table name, and status.
    """

    logger.info(f"read_etl_log : {filter_params}")
    try:
        # create connection to database
        conn = create_engine(LOG_DB_URI_ACHEMY)

        sql_str = get_query_by_id(ETL_LOG_PATH, "getMaxEtlDate")
        # Execute the query with pd.read_sql
        df = pd.read_sql(sql=sql_str, con=conn, params=filter_params)

        # return extracted data
        return df
    except Exception as e:
        logger.error(f"Can't execute your query. Cause: {str(e)}")
