# Sales Data Processing Pipeline – PySpark Implementation

- **Company**: Metyis  
- **Maintainer**: Hamed Mahdavi Far  
- **Email**: hamedmahdavifar31@gmail.com  
- **Date**: 2025-08-08  

---

## Overview

This project implements a PySpark-based pipeline that processes sales data from raw CSV files into a cleansed, deduplicated, and partitioned Parquet dataframe.
The goal is to simulate the transformation of semi-structured operational data into an analytics-ready format, with considerations for scalability, performance, and data quality.

---

## Purpose

The pipeline simulates a scalable, production-ready data ingestion and transformation workflow suitable for enterprise data lake environments. While the storage layer is local in this project, the approach can be easily adapted to cloud-based platforms such as Azure Data Lake Storage (ADLS).

---

## data

The dataset consists of 12 monthly CSV files, each containing sales order records with the following columns:
after inspecting the data I had the following realization:
- `order_id`: not a Unique identifier for the order
- `product`: string, may contain nulls
- `quantity ordered`: integer, may contain nulls
- `price each`: string, may contain nulls
- `order_date`: string in `YYYY-MM-DD HH:mm:ss` format, may contain nulls
- `order_address`: string, may contain nulls

#### secondly,
- there are some rows that are fully null in the original data.
- there are some rows that are repetition of the header row.
- these mentioned rows will not contribute any information to the analysis and should be removed.

##### what will happen to these rows?
- **Fully null rows**: These will be removed as they do not provide any useful information.
- in this pipeline since I am implementing a schema with string and integers, when spark reaches to a row that is repetition of the header.
it will assign the values to null. in next phase all rows with null values are going to be checked in details and removed.
### schema

```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
sales_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("product", StringType(), True),
    StructField("quantity_ordered", IntegerType(), True),
    StructField("price_each", StringType(), True),
    StructField("order_date", TimestampType(), True),
    StructField("order_address", StringType(), True)
])
```

## Key Capabilities

- **Schema definition** using `StructType` to enforce column types and avoid costly inference errors  
- **Ingestion** of multiple monthly CSV files using a wildcard pattern  
- **Validation** of column headers and order to prevent silent mismatches  
- **Null value analysis** and safe removal of completely null or malformed rows  
- **Data type checks** to ensure proper casting and to eliminate invalid rows  
- **Column renaming** for consistency with Python naming conventions  
- **Deduplication** of fully identical rows only, preserving valid multi-item orders  
- **Date conversion** for enabling time-based filtering and partitioning  
- **Time component extraction** (`order_time`) and splitting date into year/month  
- **Parquet output** with compression for efficient analytics  
- **Partitioning** by `order_year` and `order_month` for improved query performance  
- **Validation utilities** to inspect output schema, partition ranges, and duplication integrity  

---

## Technologies and Tools

- Apache Spark (PySpark)  
- Python 3.x  
- Local filesystem (simulating cloud storage)  
- Jupyter Notebook or any Spark-compatible IDE  

---

## Ingestion and Schema Enforcement

- All 12 CSV files were programmatically inspected to ensure consistent headers and column ordering.
- A strict schema was defined via PySpark's `StructType` to prevent misaligned data ingestion.
- Rows containing only nulls (often due to repeated headers) were identified and dropped.

---

## Deduplication Strategy

Initially, `order_id` was assumed to be a unique identifier. However, deeper analysis revealed that multiple entries with the same `order_id` may represent valid multi-item purchases (same timestamp and address, different products).

### Final Approach:
- Deduplication was applied **only** to rows that are **fully identical** across all fields.
- This ensures valid multi-line orders are preserved and only redundant exact duplicates are removed.

---

## Timestamp Conversion and Partitioning

- The `order_date` column was parsed from string to a proper `TimestampType`.
- `order_year` and `order_month` were extracted from `order_date` to support partitioning.

Partitioning is important to:
- Enable time-based filtering and efficient queries
- Support seasonal sales analysis
---

## Output Format and Storage

### Output Format: **Parquet**
- **Parquet** is a columnar storage format, which means it stores data by column rather than by row.
- It is highly efficient for **analytical queries** (like filtering or aggregation), especially when only a subset of columns is needed.
- Built-in **compression** and **schema evolution** support make it ideal for big data workflows.
- Parquet is widely supported across data engineering tools (Spark, Hive, AWS Athena, BigQuery, etc.), making it a robust and portable format.

---

### Partitioned By: `order_year`, `order_month`
- Partitioning improves performance by allowing queries to **skip irrelevant data** (partition pruning).
- Storing data by `order_year` and `order_month` allows faster access to time-specific subsets (e.g., "Get all orders from April 2019").
- This is especially important for **large datasets** in a data lake or distributed storage environment.
- It also aligns with typical **data archiving and retention policies**.

---

### Output Location: `../data/cleansed/`
- The output is written to a **cleaned/cleansed** folder to separate it from raw data, following data lake zone best practices.


---

## Output Validation Steps

- Checked output schema and previewed records  
- Verified partition structure and confirmed no data spillover by inspecting min/max order dates per partition  
- Used helper functions to explore nulls, duplication stats, and logical inconsistencies  

---

# How to Run the Pipeline

## Environment Setup (Windows + Jupyter Notebook)

If running Spark on Windows:

- Download `winutils.exe` and place it under `C:\hadoop\bin`
- Set the following environment variables **before** starting Spark:

    ```python
    import os
    os.environ["HADOOP_HOME"] = "C:\\hadoop"
    os.environ["hadoop.home.dir"] = "C:\\hadoop"
    os.environ["PATH"] += os.pathsep + "C:\\hadoop\\bin"
    ```

---

## Running the Pipeline

1. **Install PySpark**

    Ensure you have PySpark installed. If not, run:

    ```bash
    pip install pyspark
    ```

2. **Prepare Your Data**

    Place all your monthly CSV sales files inside the `sales_data` folder of your project directory.

3. **Open the explore Notebook**

    Open the Jupyter notebook and scroll to the top.

4. **Validate CSV Column Order**

    - Run the cell that checks whether all CSV files have the correct column order.
    - If any files are problematic, their filenames will be displayed.  
      **Fix these issues before continuing.**

5. **Run the Pipeline inside the pipeline notebook pipeline_2019data_ingestion_03_08_2025.ipynb**

    - Once your data passes the column order test, run the notebook **cell by cell** from top to bottom, or run all cells at once.
    - you will see the result of each step in the output cells.
    - in case of any inconsistencies an action needs to be taken based on the error that i caused by the new data.


6. **Inspect the Output**

    - After the pipeline completes, check the `../data/cleansed/` directory for the output Parquet files.
    - You can use Spark or any compatible tool to read and validate the Parquet files.
    - Note: the output by default overwrites any existing files in the output directory, so ensure you have backups if needed or if you are adding new data you need to change your strategy to append the new data to the existing files.

---

> **Tip:**  
> Always validate your CSV files before running the pipeline to prevent downstream errors and ensure a smooth process.
---


















