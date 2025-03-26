import json
from datetime import datetime

from src.config.logging_conf import logger


def get_num_rows(df) -> int:
    return df.count()


def get_num_columns(df) -> int:
    return len(df.columns)


def get_basic_info(df) -> dict:
    return {
        "num_rows": get_num_rows(df),
        "num_columns": get_num_columns(df)
    }


def get_missing_values(df) -> dict:
    missing_values = {}
    for column in df.columns:
        total_missing = df.where(df[column].isNull()).count()
        total_data = get_num_rows(df)
        percentage_missing = (total_missing / total_data) * 100
        missing_values[column] = f"{percentage_missing:.2f}% missing values: {total_missing}/{total_data}"
    return missing_values


def get_data_types(df) -> dict:
    data_types = {}
    for column in df.dtypes:
        data_types[column[0]] = column[1]
    return data_types


def get_profiling_result(data_profiling: dict) -> dict:
    result = {}
    for key, value in data_profiling.items():
        result[key] = {}

        basic_info = get_basic_info(value)
        missing_values = get_missing_values(value)
        data_types = get_data_types(value)

        result[key]["basic_info"] = basic_info
        result[key]["data_types"] = data_types
        result[key]["missing_values"] = missing_values
    date_now = datetime.now().strftime("%Y_%m_%d")
    output_file = f"output/profiling_result_{date_now}.json"
    logger.info(result)
    with open(output_file, "w") as json_file:
        json.dump(result, json_file, indent=4)  # Beautify with 4-space indentation
    return result
