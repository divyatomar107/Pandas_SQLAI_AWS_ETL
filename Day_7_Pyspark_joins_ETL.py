"""
Day 7: PySpark Joins & ETL

Description:
This script demonstrates how to use PySpark for joining DataFrames and performing basic ETL (Extract, Transform, Load) operations. It covers creating DataFrames, performing various types of joins, writing results to different file formats, and reading data back for analysis.

Steps & Code Explanation:

1. Import PySpark and Create SparkSession:
   - Import the necessary PySpark module.
   - Initialize a SparkSession, which is the entry point for working with DataFrames in PySpark.

2. Create Employee and Department DataFrames:
   - Define two DataFrames: one for employees and one for departments, simulating relational tables.

3. Perform Different Types of Joins:
   - INNER JOIN: Returns only rows with matching dept_id in both DataFrames.
   - LEFT JOIN: Returns all employees, with department info where available.
   - RIGHT JOIN: Returns all departments, with employee info where available.
   - FULL OUTER JOIN: Returns all employees and departments, matched where possible.

4. Write Output to Files:
   - Save the inner join result as a CSV file (with header).
   - Save the full outer join result as a Parquet file.
   - Save the left join result as partitioned Parquet files by dept_id.

5. Read Parquet Files:
   - Load the Parquet file back into a DataFrame for further analysis or validation.

6. Stop the SparkSession:
   - Properly close the SparkSession to free up resources.

Typical Use Cases:
- Combining HR and department data for analytics.
- Preparing data for reporting or further processing.
- Exporting results in multiple formats for downstream use.

Author: [Your Name]
Date: [Date]
"""

from pyspark.sql import SparkSession

# 1. Import PySpark and Create SparkSession
spark = SparkSession.builder.appName("PySpark Joins ETL").getOrCreate()

# 2. Create Employee and Department DataFrames
employees = spark.createDataFrame([
    (1, "Divya", 10),
    (2, "Amit", 20),
    (3, "Neha", 30)
], ["emp_id", "emp_name", "dept_id"])

departments = spark.createDataFrame([
    (10, "HR"),
    (20, "Finance"),
    (40, "IT")
], ["dept_id", "dept_name"])

# 3. Perform Different Types of Joins

# INNER JOIN: Only rows with matching dept_id in both DataFrames
inner_join_df = employees.join(departments, on="dept_id", how="inner")
print("Inner Join Result:")
inner_join_df.show()

# LEFT JOIN: All employees, with department info where available
left_join_df = employees.join(departments, on="dept_id", how="left")
print("Left Join Result:")
left_join_df.show()

# RIGHT JOIN: All departments, with employee info where available
right_join_df = employees.join(departments, on="dept_id", how="right")
print("Right Join Result:")
right_join_df.show()

# FULL OUTER JOIN: All employees and departments, matched where possible
full_outer_join_df = employees.join(departments, on="dept_id", how="outer")
print("Full Outer Join Result:")
full_outer_join_df.show()

# 4. Write Output to Files

# Write inner join result to CSV (with header, overwrite if exists)
inner_join_df.write.mode("overwrite").option("header", True).csv("/output/csv")

# Write full outer join result to Parquet format (overwrite if exists)
full_outer_join_df.write.mode("overwrite").parquet("output/parquet")

# Write left join result as partitioned Parquet files by dept_id
left_join_df.write.mode("overwrite").partitionBy("dept_id").parquet("output/parquet_partitioned")

# 5. Read Parquet Files

# Read Parquet file back into a DataFrame for further analysis or validation
df = spark.read.parquet("output/parquet")
print("Read Parquet File:")
df.show()

# 6. Stop the SparkSession to free resources
spark.stop()