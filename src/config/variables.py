# Load environment variables from .env file
import os

from dotenv import load_dotenv

load_dotenv()

# Read individual variables
src_db_user = os.getenv("SRC_DB_USER")
src_db_password = os.getenv("SRC_DB_PASSWORD")
src_db_host = os.getenv("SRC_DB_HOST")
src_db_port = os.getenv("SRC_DB_PORT")
src_db_name = os.getenv("SRC_DB_NAME")

dwh_db_user = os.getenv("DWH_DB_USER")
dwh_db_password = os.getenv("DWH_DB_PASSWORD")
dwh_db_host = os.getenv("DWH_DB_HOST")
dwh_db_port = os.getenv("DWH_DB_PORT")
dwh_db_name = os.getenv("DWH_DB_NAME")

log_db_user = os.getenv("LOG_DB_USER")
log_db_password = os.getenv("LOG_DB_PASSWORD")
log_db_host = os.getenv("LOG_DB_HOST")
log_db_port = os.getenv("LOG_DB_PORT")
log_db_name = os.getenv("LOG_DB_NAME")

DRIVER_DB = os.getenv("DRIVER_DB")

# Format the JDBC URL
SRC_DB_URI = f"jdbc:postgresql://{src_db_host}:{src_db_port}/{src_db_name}?user={src_db_user}&password={src_db_password}"
DWH_DB_URI = f"jdbc:postgresql://{dwh_db_host}:{dwh_db_port}/{dwh_db_name}?user={dwh_db_user}&password={dwh_db_password}"

LOG_DB_URI_ACHEMY = f"postgresql://{log_db_user}:{log_db_password}@{log_db_host}:{log_db_port}/{log_db_name}"
DWH_DB_URI_ALCHEMY = f"postgresql://{dwh_db_user}:{dwh_db_password}@{dwh_db_host}:{dwh_db_port}/{dwh_db_name}"

CONNECTION_PROPERTIES = {
    "driver": DRIVER_DB
}