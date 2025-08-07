
from pyspark.sql.functions import col, count, row_number, min, max
from pyspark.sql.window import Window
from pyspark.errors import AnalysisException
from pyspark.sql.utils import AnalysisException
from pyspark.sql import DataFrame
import glob
from scripts.notebook_logger import get_notebook_logger

logger = get_notebook_logger(__name__)
# ------------------------------------------------------------------------------------------------------------------------
# this function checks if all CSV files in a directory have the same column order
def check_csv_column_order_consistency(directory_path: str, spark) -> bool:
    try:
        logger.info(f"Checking column order consistency for files in: {directory_path}")
        schema_reference = None
        mismatches = []
        csv_files = sorted(glob.glob(directory_path))

        for file_path in csv_files:
            try:
                df = spark.read.option("header", True).option("inferSchema", True).csv(file_path)
                current_columns = df.columns

                if schema_reference is None:
                    schema_reference = current_columns
                elif current_columns != schema_reference:
                    mismatches.append((file_path, current_columns))
            except AnalysisException as e:
                logger.error(f"Failed to read file: {file_path} — {str(e)}")

        if mismatches:
            logger.warning("Column order mismatch found in the following files:")
            for file, cols in mismatches:
                logger.warning(f" - {file}")
                logger.warning(f"   Columns: {cols}")
            return False

        logger.info("All CSV files have the same column order.")
        return True
    except Exception as e:
        logger.exception("Failed during column order consistency check.")
        raise

# ------------------------------------------------------------------------------------------------------------------------
# This function filters rows in a DataFrame that contain null values in any column
def get_rows_with_nulls(df: DataFrame) -> DataFrame:
    try:
        logger.info("Checking for rows with null values in any column.")
        if not df.columns:
            logger.warning("Input DataFrame has no columns.")
            return df

        condition = None
        for column_name in df.columns:
            null_check = col(column_name).isNull()
            condition = null_check if condition is None else condition | null_check

        if condition is None:
            logger.info("No null condition created. Returning original DataFrame.")
            return df

        filtered_df = df.filter(condition)
        logger.info(f"Found {filtered_df.count()} rows with at least one null value.")
        return filtered_df
    except Exception as e:
        logger.exception("Failed to filter rows with null values.")
        raise

# ------------------------------------------------------------------------------------------------------------------------

# This function renames DataFrame columns to snake_case format
def rename_columns_to_snake_case(df: DataFrame) -> DataFrame:
    try:
        logger.info("Renaming columns to snake_case format.")
        for col_name in df.columns:
            new_col = col_name.lower().replace(" ", "_")
            df = df.withColumnRenamed(col_name, new_col)
        logger.info("Successfully renamed all columns to snake_case.")
        return df
    except Exception as e:
        logger.exception("Failed to rename columns.")
        raise

# ------------------------------------------------------------------------------------------------------------------------

# This function finds any duplicated values in a specified column of a DataFrame
def find_any_duplication(df: DataFrame, column_name: str) -> DataFrame:
    try:
        logger.info(f"Finding duplicated values in column: {column_name}")
        duplicated_values = (
            df.groupBy(column_name)
              .agg(count("*").alias("count"))
              .filter("count > 1")
              .select(column_name)
        )
        duplicated_rows = df.join(duplicated_values, on=column_name, how="inner")
        logger.info(f"Found {duplicated_rows.count()} duplicated rows.")
        return duplicated_rows
    except Exception as e:
        logger.exception(f"Failed to find duplicated values in column: {column_name}")
        raise

# ------------------------------------------------------------------------------------------------------------------------

# This function identifies fully duplicated rows in a DataFrame, where all fields match
def get_fully_duplicated_rows(df: DataFrame) -> DataFrame:
    try:
        logger.info("Identifying fully duplicated rows (all fields match).")
        all_columns = df.columns
        duplicate_groups = (
            df.groupBy(*all_columns)
              .agg(count("*").alias("count"))
              .filter("count > 1")
        )
        result = df.join(duplicate_groups.drop("count"), on=all_columns, how="inner")
        logger.info(f"Found {result.count()} fully duplicated rows.")
        return result
    except Exception as e:
        logger.exception("Failed to identify fully duplicated rows.")
        raise

# ------------------------------------------------------------------------------------------------------------------------

# This function drops fully duplicated rows from a DataFrame, keeping only one instance of each duplicate
def drop_fully_duplicated_rows(df: DataFrame) -> DataFrame:
    try:
        logger.info("Dropping fully duplicated rows.")
        order_column = df.columns[0]
        window_spec = Window.partitionBy(*df.columns).orderBy(order_column)

        cleaned_df = (
            df.withColumn("rn", row_number().over(window_spec))
              .filter("rn = 1")
              .drop("rn")
        )
        logger.info("Fully duplicated rows removed successfully.")
        return cleaned_df
    except Exception as e:
        logger.exception("Failed to drop fully duplicated rows.")
        raise

# ------------------------------------------------------------------------------------------------------------------------

# This function reads a Parquet file from the specified path and displays a preview of the data
def read_and_preview_parquet(path: str, spark) -> DataFrame:
    try:
        logger.info(f"Reading parquet files from: {path}")
        df = spark.read.parquet(path)
        df.show(5, truncate=True)
        logger.info("Preview of parquet data displayed successfully.")
        return df
    except Exception as e:
        logger.exception(f"Failed to read or preview parquet at path: {path}")
        raise

# ------------------------------------------------------------------------------------------------------------------------

# This function counts the number of fully duplicated record groups in a DataFrame
def count_fully_duplicated_groups(df: DataFrame) -> DataFrame:
    try:
        logger.info("Counting fully duplicated record groups.")
        all_columns = df.columns
        result = (
            df.groupBy(*all_columns)
              .agg(count("*").alias("count"))
              .filter("count > 1")
              .groupBy("count")
              .agg(count("*").alias("groups"))
              .orderBy("count")
        )
        logger.info("Counted fully duplicated groups successfully.")
        return result
    except Exception as e:
        logger.exception("Failed to count fully duplicated record groups.")
        raise

# ------------------------------------------------------------------------------------------------------------------------

# This function displays the minimum and maximum dates for each partition defined by year and month columns
def show_min_max_dates_per_partition(
    df: DataFrame,
    year_col: str = "order_year",
    month_col: str = "order_month",
    date_col: str = "order_date"
) -> None:
    try:
        logger.info(f"Displaying min/max dates for each ({year_col}, {month_col}) partition.")
        df.groupBy(year_col, month_col) \
          .agg(
              min(date_col).alias("min_date"),
              max(date_col).alias("max_date")
          ) \
          .orderBy(year_col, month_col) \
          .show(truncate=False)
        logger.info("Min and max dates per partition displayed successfully.")
    except Exception as e:
        logger.exception("Failed to display min/max dates per partition.")
        raise

