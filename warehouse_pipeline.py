import os

from src.config.spark import get_spark_session
from src.extract.extract_src_data import extract_src_db, extract_src_csv
from src.load.load_dwh_data import load_dwh_db
from src.transform.transform_src_data import transform_data

# Set hadoop and driver db path
PARENT_DIR = os.getcwd()
HADOOP_PATH = os.path.join(PARENT_DIR, "library/hadoop")
POSTGRES_DRIVER_PATH = os.path.join(PARENT_DIR, "library/postgre/postgresql-42.7.5.jar")

# set environment variables
os.environ["HADOOP_HOME"] = HADOOP_PATH
os.environ["PATH"] += os.pathsep + os.path.join(HADOOP_PATH, "bin")

# set spark session
spark_session = get_spark_session("week6-warehouse-pipeline", POSTGRES_DRIVER_PATH)
spark_session.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
spark_session.conf.set("spark.sql.debug.maxToStringFields", "100")

# Extract data from source
education_status_df = extract_src_db(spark_session, "education_status", "warehouse", "database")
marital_status_df = extract_src_db(spark_session, "marital_status", "warehouse", "database")
marketing_campaign_df = extract_src_db(spark_session, "marketing_campaign_deposit", "warehouse", "database")
csv_data_df = extract_src_csv(spark_session, "data/new_bank_transaction_csv/", "customers & transactions", "warehouse",
                              "csv")
customers_df = csv_data_df.select("CustomerID", "CustomerDOB", "CustGender", "CustLocation", "CustAccountBalance")
transactions_df = csv_data_df.select("TransactionID", "CustomerID", "TransactionDate", "TransactionTime",
                                     "TransactionAmount (INR)")

# Transform data
new_education_status_df = transform_data(education_status_df, "education_status", "warehouse", "database")
new_marital_status_df = transform_data(marital_status_df, "marital_status", "warehouse", "database")
new_marketing_campaign_df = transform_data(marketing_campaign_df, "marketing_campaign_deposit", "warehouse", "database")
new_customers_df = transform_data(customers_df, "customers", "warehouse", "csv")
new_transactions_df = transform_data(transactions_df, "transactions", "warehouse", "csv")

# Load data to warehouse
load_dwh_db(new_education_status_df, "education_status", "warehouse", "database")
load_dwh_db(new_marital_status_df, "marital_status", "warehouse", "database")
load_dwh_db(new_marketing_campaign_df, "marketing_campaign_deposit", "warehouse", "database")
load_dwh_db(new_customers_df, "customers", "warehouse", "csv")
load_dwh_db(new_transactions_df, "transactions", "warehouse", "csv")

spark_session.stop()