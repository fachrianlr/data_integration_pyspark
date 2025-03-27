import os

from src.config.spark import get_spark_session
from src.extract.extract_src_data import extract_src_db, extract_src_csv
from src.profiling.profiling_data import get_profiling_result

# Set hadoop and driver db path
PARENT_DIR = os.getcwd()
HADOOP_PATH = os.path.join(PARENT_DIR, "library/hadoop")
POSTGRES_DRIVER_PATH = os.path.join(PARENT_DIR, "library/postgre/postgresql-42.7.5.jar")

# set environment variables
os.environ["HADOOP_HOME"] = HADOOP_PATH
os.environ["PATH"] += os.pathsep + os.path.join(HADOOP_PATH, "bin")

# set spark session
spark_session = get_spark_session("week6-profiling", POSTGRES_DRIVER_PATH)
spark_session.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# Extract data from source
education_status_df = extract_src_db(spark_session, "education_status", "profiling", "database")
marital_status_df = extract_src_db(spark_session, "marital_status", "profiling", "database")
marketing_campaign_df = extract_src_db(spark_session, "marketing_campaign_deposit", "profiling", "database")
csv_data_df = extract_src_csv(spark_session, "data/new_bank_transaction_csv/", "customers & transactions", "profiling",
                              "csv")
customers_df = csv_data_df.select("CustomerID", "CustomerDOB", "CustGender", "CustLocation", "CustAccountBalance")
transactions_df = csv_data_df.select("TransactionID", "CustomerID", "TransactionDate", "TransactionTime",
                                     "TransactionAmount (INR)")

# Collect data profiling
data_profiling = dict()
data_profiling["education_status"] = education_status_df
data_profiling["marital_status"] = marital_status_df
data_profiling["marketing_campaign"] = marketing_campaign_df
data_profiling["customers"] = customers_df
data_profiling["transactions"] = transactions_df

# get profiling result to json file
get_profiling_result(data_profiling)
spark_session.stop()
