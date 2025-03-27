from datetime import datetime

from pyspark.sql import DataFrame
from sqlalchemy import create_engine, text

from src.config.logging_conf import logger
from src.config.variables import DWH_DB_URI_ALCHEMY, DWH_DB_URI, CONNECTION_PROPERTIES
from src.log.etl_log import insert_etl_log


def load_dwh_db(df: DataFrame, table_name: str, step: str, source: str) -> None:
    logger.info(f"Loading dwh data to : {table_name}")

    log_msg = {
        "step": step,
        "process": "load",
        "status": "-",
        "source": source,
        "table_name": table_name,
        "error_msg": "-",
        "etl_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }

    try:

        # truncate table before insert new data
        engine = create_engine(DWH_DB_URI_ALCHEMY)
        with engine.connect() as conn:
            conn.execute(text(f"TRUNCATE TABLE {table_name} CASCADE"))
            conn.commit()
            logger.info(f"DWH DB truncated table : {table_name}")

        # insert dataframe to database table
        df.write.jdbc(url=DWH_DB_URI, table=table_name, mode="append", properties=CONNECTION_PROPERTIES)

        log_msg["status"] = "success"
    except Exception as e:
        logger.error(f"Can't load dwh data to: {table_name}. Cause: {str(e)}")

        log_msg["status"] = "failed"
        log_msg["error_msg"] = str(e)

    finally:
        logger.info(f"Finished loading dwh data to {table_name}")
        insert_etl_log(log_msg)
