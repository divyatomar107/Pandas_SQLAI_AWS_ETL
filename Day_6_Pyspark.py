
"""
╔════════════════════════════════════════════════════════════════════════════════╗
║                   DAY 6: PYSPARK - DISTRIBUTED COMPUTING                       ║
║                                                                                ║
║  Learning Objectives:                                                          ║
║  1. Set up PySpark environment and handle Windows compatibility                ║
║  2. Create Spark Session and initialize Spark context                          ║
║  3. Create and work with Spark DataFrames                                      ║
║  4. Perform distributed data processing operations                             ║
║  5. Understand lazy evaluation and Spark execution model                       ║
║                                                                                ║
║  Key Concepts:                                                                 ║
║  - PySpark: Python API for Apache Spark                                        ║
║  - SparkSession: Entry point for Spark functionality                           ║
║  - Spark DataFrame: Distributed collection of rows with named columns          ║
║  - RDD: Resilient Distributed Dataset (lower-level abstraction)                ║
║  - Lazy Evaluation: Transformations not executed until action called            ║
║  - Distributed Computing: Data split across multiple nodes/partitions          ║
║                                                                                ║
║  Prerequisites:                                                                ║
║  - Python 3.6+ installed                                                       ║
║  - Java JDK 8 or 11 installed                                                  ║
║  - JAVA_HOME environment variable set correctly                                ║
║  - PySpark installed: pip install pyspark                                      ║
║  - findspark installed: pip install findspark                                  ║
║                                                                                ║
║  Windows-Specific Setup:                                                       ║
║  - Need to patch socketserver for Windows compatibility                        ║
║  - findspark helps locate Spark installation automatically                     ║
║                                                                                ║
║  Use Case:                                                                     ║
║  Processing large datasets (TB/PB scale) across distributed cluster,          ║
║  performing data transformations, aggregations in parallel                     ║
╚════════════════════════════════════════════════════════════════════════════════╝
"""

import sys
import socketserver

# ──────────────────────────────────────────────────────────────────────────────
# WINDOWS COMPATIBILITY PATCH
# ──────────────────────────────────────────────────────────────────────────────
print("=" * 80)
print("SETUP: Windows Compatibility Patch")
print("=" * 80)
print("\nExplanation:")
print("- Spark uses Unix sockets on Linux, but Windows doesn't support them")
print("- We replace UnixStreamServer with TCPServer for Windows compatibility")
print("- UnixStreamHandler → StreamRequestHandler (handles network communication)\n")

if sys.platform == "win32":
    socketserver.UnixStreamServer = socketserver.TCPServer
    socketserver.UnixStreamHandler = socketserver.StreamRequestHandler
    print("✅ Windows compatibility patch applied")
else:
    print("✅ Running on Unix/Linux (no patch needed)")

# ──────────────────────────────────────────────────────────────────────────────
# PYSPARK SETUP
# ──────────────────────────────────────────────────────────────────────────────
print("\n" + "=" * 80)
print("SETUP: PySpark Initialization")
print("=" * 80)
print("\nExplanation:")
print("- findspark.init() locates Spark installation automatically")
print("- Adds Spark libraries to Python path")
print("- Removes need to manually set SPARK_HOME\n")

import findspark
findspark.init()
print("✅ findspark initialized")

# ──────────────────────────────────────────────────────────────────────────────
# CREATE SPARK SESSION
# ──────────────────────────────────────────────────────────────────────────────
print("\n" + "=" * 80)
print("EXERCISE 1: Create Spark Session")
print("=" * 80)
print("\nExplanation:")
print("- SparkSession is the entry point for all Spark functionality")
print("- .builder: Fluent API for configuration")
print("- .appName(): Name of your Spark application (appears in Spark UI)")
print("- .getOrCreate(): Reuse existing session or create new one\n")

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("EnvTest").getOrCreate()

print("✅ SparkSession created:")
#print(f"   App Name: {spark.appName}")
print(f"   Spark Version: {spark.version}")

# ──────────────────────────────────────────────────────────────────────────────
# EXERCISE 2: Create DataFrame from List
# ──────────────────────────────────────────────────────────────────────────────
print("\n" + "=" * 80)
print("EXERCISE 2: Create Spark DataFrame from List")
print("=" * 80)
print("\nExplanation:")
print("- createDataFrame() creates a Spark DataFrame from data")
print("- Data: List of tuples (rows)")
print("- Schema: Column names as list")
print("- DataFrame: Distributed table with rows and columns")
print("- Similar to Pandas DataFrame but distributed across cluster\n")

print("Code Example:")
print("""
data = [("Rohit", 1), ("Divya", 2)]
df = spark.createDataFrame(data, ["Name", "Id"])
df.show()

Output:
+-----+---+
| Name| Id|
+-----+---+
|Rohit|  1|
|Divya|  2|
+-----+---+
""")

data = [("Rohit", 1), ("Divya", 2)]
df = spark.createDataFrame(data, ["Name", "Id"])

print("Spark DataFrame created:")
df.show()

# ──────────────────────────────────────────────────────────────────────────────
# EXERCISE 3: DataFrame Operations
# ──────────────────────────────────────────────────────────────────────────────
print("\n" + "=" * 80)
print("EXERCISE 3: DataFrame Operations")
print("=" * 80)

# 3.1 Print Schema
print("\n3.1 - Print DataFrame Schema")
print("─" * 40)
print("Explanation:")
print("- printSchema() shows the structure of DataFrame")
print("- Column names and data types\n")
df.printSchema()

# 3.2 Count rows
print("\n3.2 - Count Rows")
print("─" * 40)
print("Explanation:")
print("- count() returns total number of rows")
print("- Triggers actual computation (action)\n")
row_count = df.count()
print(f"Total rows: {row_count}")

# 3.3 Display with show()
print("\n3.3 - Display Data with show()")
print("─" * 40)
print("Explanation:")
print("- show() displays top 20 rows (default)")
print("- Pretty-printed table format")
print("- Useful for verification\n")
print("Displaying all data:")
df.show(truncate=False)

# ──────────────────────────────────────────────────────────────────────────────
# EXERCISE 4: Create DataFrame from Tuples (Sales Data)
# ──────────────────────────────────────────────────────────────────────────────
print("\n" + "=" * 80)
print("EXERCISE 4: DataFrame with Multiple Columns (Sales Data)")
print("=" * 80)
print("\nExplanation:")
print("- Create more realistic data with multiple attributes")
print("- Each tuple represents one row")
print("- Column names define the schema")
print("- Data suitable for analysis and aggregation\n")

sales_data = [
    ("Rohit", "North", 12000),
    ("Divya", "South", 18000),
    ("Amit", "East", 10000),
    ("Rohit", "North", 15000),
    ("Divya", "South", 45000)
]

sales_schema = ["Name", "Region", "Sales"]

sales_df = spark.createDataFrame(sales_data, sales_schema)

print("Sales DataFrame:")
sales_df.show()
sales_df.printSchema()

# ──────────────────────────────────────────────────────────────────────────────
# EXERCISE 5: Aggregations
# ──────────────────────────────────────────────────────────────────────────────
print("\n" + "=" * 80)
print("EXERCISE 5: Aggregation Operations")
print("=" * 80)

# 5.1 Group by and Sum
print("\n5.1 - GroupBy and Sum")
print("─" * 40)
print("Explanation:")
print("- groupBy(): Groups data by column")
print("- sum(): Aggregates numeric columns")
print("- Result: Total sales per region\n")

from pyspark.sql.functions import sum as spark_sum

region_summary = sales_df.groupBy("Region").agg(spark_sum("Sales").alias("TotalSales"))
print("Total Sales per Region:")
region_summary.show()

# 5.2 Multiple aggregations
print("\n5.2 - Multiple Aggregations")
print("─" * 40)
print("Explanation:")
print("- agg() with multiple functions")
print("- sum(): Total sales")
print("- count(): Number of transactions")
print("- Result: Comprehensive summary\n")

from pyspark.sql.functions import count as spark_count

summary = sales_df.groupBy("Name").agg(
    spark_sum("Sales").alias("TotalSales"),
    spark_count("*").alias("TransactionCount")
)
print("Summary by Name:")
summary.show()

# ──────────────────────────────────────────────────────────────────────────────
# EXERCISE 6: Filtering
# ──────────────────────────────────────────────────────────────────────────────
print("\n" + "=" * 80)
print("EXERCISE 6: Filtering Data")
print("=" * 80)
print("\nExplanation:")
print("- filter() / where() select rows matching condition")
print("- Similar to WHERE clause in SQL")
print("- Returns new DataFrame with filtered rows\n")

print("Sales > 15000:")
high_sales = sales_df.filter(sales_df["Sales"] > 15000)
high_sales.show()

# ──────────────────────────────────────────────────────────────────────────────
# EXERCISE 7: Select Specific Columns
# ──────────────────────────────────────────────────────────────────────────────
print("\n" + "=" * 80)
print("EXERCISE 7: Select Columns")
print("=" * 80)
print("\nExplanation:")
print("- select() chooses specific columns")
print("- Returns DataFrame with only selected columns\n")

selected = sales_df.select("Name", "Sales")
print("Selected Columns (Name, Sales):")
selected.show()

print("\n" + "=" * 80)
print("SUMMARY - PySpark Concepts")
print("=" * 80)
print("""
✅ Core Concepts Covered:

1. SETUP:
   - Windows socket compatibility patch
   - findspark for automatic Spark discovery
   - SparkSession initialization
   
2. DATAFRAME CREATION:
   - From Python lists/tuples
   - With explicit schema (column names)
   - Distributed across cluster
   
3. OPERATIONS:
   - show(): Display data
   - printSchema(): Column structure
   - count(): Row count
   
4. TRANSFORMATIONS:
   - groupBy(): Group rows
   - filter(): Select rows by condition
   - select(): Choose columns
   - agg(): Aggregate functions (sum, count, avg, etc.)
   
5. KEY DIFFERENCES FROM PANDAS:
   - Distributed: Data split across nodes
   - Lazy Evaluation: Transformations not executed immediately
   - Action vs Transformation: Only actions trigger computation
   - Scalability: Can handle TB/PB of data

🎯 Transformations (Lazy - Cached, not executed):
   ├── select()
   ├── filter() / where()
   ├── groupBy() / agg()
   ├── join()
   ├── sort()
   ├── map() / flatMap()
   └── union() / subtract()

⚡ Actions (Trigger Execution):
   ├── show(): Display results
   ├── count(): Count rows
   ├── collect(): Get all data to driver
   ├── first(): Get first row
   ├── take(n): Get first n rows
   ├── saveAsTextFile(): Write to file
   └── foreachRDD(): Process each partition

💡 Performance Tips:
   - Use filter early to reduce data
   - Select only needed columns
   - Partition data for distributed processing
   - Cache frequently used DataFrames (.cache())
   - Use Spark SQL for complex queries
   
🚀 Advanced Topics (Beyond Scope):
   - SparkSQL and Catalyst optimizer
   - MLlib for machine learning
   - Streaming with Spark
   - Graph processing with GraphX
""")

print("\n" + "=" * 80)
print("✅ All exercises completed!")
print("=" * 80)

###Read CSV and aggregate sales by region
from pyspark.sql.functions import sum

df=spark.read.csv("sales.csv",header=True,inferSchema=True)
df.show()

df_grouped = df.groupBy("Region").agg(sum("Sales").alias("TotalSales"))
df_sorted=df_grouped.orderBy(df_grouped.TotalSales.desc())
df_sorted.show()

