# Pandas & PySpark Learning Journey

This repository documents a step-by-step journey through data analysis and ETL using Pandas and PySpark. Each day introduces new concepts, building on previous knowledge with practical Python scripts and code explanations.

---

## Day 1: Pandas Basics (`Day_1_pandas_basics.py`)

**Description:**  
Introduces the Pandas library for data analysis in Python.

**Steps & Code Explanation:**

1. **Import Pandas:**  
   ```python
   import pandas as pd
   ```
   Loads the pandas library for data manipulation.

2. **Create DataFrames:**  
   ```python
   data = {'Name': ['Divya', 'Amit'], 'Age': [25, 30]}
   df = pd.DataFrame(data)
   ```
   Constructs a DataFrame from a dictionary.

3. **View Data:**  
   ```python
   print(df.head())
   print(df.info())
   ```
   `.head()` shows the first few rows; `.info()` gives column types and non-null counts.

4. **Select Columns:**  
   ```python
   print(df['Name'])
   ```
   Accesses a single column.

5. **Filter Rows:**  
   ```python
   print(df[df['Age'] > 25])
   ```
   Filters rows where Age is greater than 25.

6. **Basic Operations:**  
   ```python
   df['Age_plus_10'] = df['Age'] + 10
   ```
   Adds a new column by performing arithmetic on an existing column.

---

## Day 2: Data Cleaning (`Day_2_data_cleaning.py`)

**Description:**  
Focuses on cleaning and preparing data for analysis.

**Steps & Code Explanation:**

1. **Handle Missing Values:**  
   ```python
   df.dropna(inplace=True)
   df.fillna(0)
   ```
   Removes rows with missing values or fills them with zero.

2. **Remove Duplicates:**  
   ```python
   df.drop_duplicates(inplace=True)
   ```
   Removes duplicate rows.

3. **Change Data Types:**  
   ```python
   df['Age'] = df['Age'].astype(float)
   ```
   Converts the Age column to float type.

4. **Rename Columns:**  
   ```python
   df.rename(columns={'Name': 'FullName'}, inplace=True)
   ```
   Renames the 'Name' column to 'FullName'.

5. **String Operations:**  
   ```python
   df['FullName'] = df['FullName'].str.strip().str.lower()
   ```
   Cleans string data by stripping whitespace and converting to lowercase.

---

## Day 3: Data Aggregation (`Day_3_data_aggregation.py`)

**Description:**  
Demonstrates grouping and summarizing data.

**Steps & Code Explanation:**

1. **Group Data:**  
   ```python
   grouped = df.groupby('Department')
   ```

2. **Aggregate Functions:**  
   ```python
   grouped['Salary'].sum()
   grouped['Salary'].mean()
   ```

3. **Multiple Aggregations:**  
   ```python
   grouped.agg({'Salary': ['min', 'max', 'mean']})
   ```

4. **Pivot Tables:**  
   ```python
   pd.pivot_table(df, values='Salary', index='Department', columns='Gender', aggfunc='mean')
   ```
   Summarizes data by department and gender.

---

## Day 4: Data Visualization (`Day_4_data_visualization.py`)

**Description:**  
Introduces data visualization using Matplotlib and Seaborn.

**Steps & Code Explanation:**

1. **Import Libraries:**  
   ```python
   import matplotlib.pyplot as plt
   import seaborn as sns
   ```

2. **Line and Bar Plots:**  
   ```python
   df['Age'].plot(kind='bar')
   plt.show()
   ```

3. **Histograms and Boxplots:**  
   ```python
   df['Salary'].hist()
   plt.show()
   sns.boxplot(x='Department', y='Salary', data=df)
   plt.show()
   ```

4. **Scatter Plots:**  
   ```python
   plt.scatter(df['Age'], df['Salary'])
   plt.xlabel('Age')
   plt.ylabel('Salary')
   plt.show()
   ```

5. **Customize Plots:**  
   ```python
   plt.title('Salary Distribution')
   plt.legend(['Salary'])
   ```

---

## Day 5: Advanced Pandas (`Day_5_advanced_pandas.py`)

**Description:**  
Covers advanced data manipulation techniques.

**Steps & Code Explanation:**

1. **Merging DataFrames:**  
   ```python
   merged = pd.merge(df1, df2, on='EmployeeID', how='inner')
   ```
   Combines two DataFrames on a common column.

2. **Joining DataFrames:**  
   ```python
   df1.join(df2.set_index('EmployeeID'), on='EmployeeID')
   ```

3. **Concatenation:**  
   ```python
   pd.concat([df1, df2], axis=0)
   ```

4. **MultiIndexing:**  
   ```python
   df.set_index(['Department', 'Gender'], inplace=True)
   ```

5. **Reshaping Data:**  
   ```python
   pd.melt(df, id_vars=['Department'], value_vars=['Salary'])
   ```

---

## Day 6: Introduction to PySpark (`Day_6_intro_pyspark.py`)

**Description:**  
Introduces PySpark for big data processing.

**Steps & Code Explanation:**

1. **Set Up SparkSession:**  
   ```python
   from pyspark.sql import SparkSession
   spark = SparkSession.builder.appName("example").getOrCreate()
   ```

2. **Create Spark DataFrames:**  
   ```python
   data = [(1, "Divya", 25), (2, "Amit", 30)]
   columns = ["id", "name", "age"]
   df = spark.createDataFrame(data, columns)
   ```

3. **View Data:**  
   ```python
   df.show()
   df.printSchema()
   df.describe().show()
   ```

4. **Select and Filter:**  
   ```python
   df.select("name").show()
   df.filter(df.age > 25).show()
   ```

5. **Basic Transformations:**  
   ```python
   from pyspark.sql.functions import col
   df.withColumn("age_plus_10", col("age") + 10).show()
   ```

---

## Day 7: PySpark Joins & ETL (`Day_7_Pyspark_joins_ETL.py`)

**Description:**  
Demonstrates joining DataFrames and performing ETL operations in PySpark.

**Steps & Code Explanation:**

1. **Create Employee and Department DataFrames:**  
   ```python
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
   ```

2. **Perform Joins:**  
   ```python
   inner_join_df = employees.join(departments, on="dept_id", how="inner")
   left_join_df = employees.join(departments, on="dept_id", how="left")
   right_join_df = employees.join(departments, on="dept_id", how="right")
   full_outer_join_df = employees.join(departments, on="dept_id", how="outer")
   ```
   Demonstrates all major join types.

3. **Write Output:**  
   ```python
   inner_join_df.write.mode("overwrite").option("header", True).csv("/output/csv")
   full_outer_join_df.write.mode("overwrite").parquet("output/parquet")
   left_join_df.write.mode("overwrite").partitionBy("dept_id").parquet("output/parquet_partitioned")
   ```
   Saves results in different formats and partitioning schemes.

4. **Read Parquet Files:**  
   ```python
   df = spark.read.parquet("output/parquet")
   df.show()
   ```
   Loads Parquet data for further analysis.

---

## Extract Data from Database (`extract.py`)

**Description:**  
Shows how to extract data from a SQLite database into a Pandas DataFrame.

**Steps & Code Explanation:**

1. **Set Up SQLAlchemy Engine:**  
   ```python
   from sqlalchemy import create_engine
   engine = create_engine("sqlite:///users.db", echo=True)
   ```
   Connects to a SQLite database.

2. **Read SQL Table:**  
   ```python
   import pandas as pd
   df = pd.read_sql("SELECT * FROM sales_table", con=engine)
   ```
   Loads data from the `sales_table` into a DataFrame.

3. **Display Data:**  
   ```python
   print(df)
   ```
   Prints the DataFrame for inspection.

---

**End of Learning Journey**
```

---

## 🛠️ Technologies & Tools

| Technology | Purpose | Scale |
|-----------|---------|-------|
| **Pandas** | Data manipulation & analysis | Single machine (MB-GB) |
| **SQLAlchemy** | Database ORM & SQL toolkit | Relational databases |
| **DynamoDB** | AWS NoSQL database | Cloud-based, auto-scaling |
| **Boto3** | AWS SDK for Python | AWS services integration |
| **PySpark** | Distributed data processing | Cluster computing (TB-PB) |
| **Python 3.10** | Programming language | Core runtime |

---

## 📋 Prerequisites

### Required Packages
```
pandas >= 2.0
SQLAlchemy >= 2.0
boto3 >= 1.42
findspark >= 2.0
pyspark >= 3.0
```

### System Requirements
- **Python 3.10+** installed
- **Java JDK 11 or 17** (required for PySpark only)
- **AWS Account** (optional, for Days 4-5)

### Configuration
1. **PySpark Setup (Windows):**
   - Install Java JDK
   - Set `JAVA_HOME` environment variable

2. **AWS Setup (Optional):**
   ```bash
   aws configure
   ```
   Then provide your AWS Access Key and Secret Key

### Installation
```bash
pip install pandas sqlalchemy boto3 findspark pyspark
```

---

## 🚀 Getting Started

1. **Ensure Python 3.10 is installed** and available in PATH
2. **Install required packages:**
   ```bash
   pip install -r requirements.txt
   ```
3. **For PySpark (Day 6):**
   - Install Java JDK 11+
   - Set `JAVA_HOME` environment variable
   - Run the script: `python Day_6_Pyspark.py`

4. **For AWS services (Days 4-5):**
   - Configure AWS credentials: `aws configure`
   - Ensure IAM permissions for DynamoDB and S3

---

## 📝 Learning Outcomes

After completing this 6-day journey, you will understand:

✅ Data manipulation using Pandas  
✅ Relational database operations with SQLAlchemy  
✅ NoSQL database management with DynamoDB  
✅ Cloud storage with AWS S3  
✅ Distributed computing with PySpark  
✅ End-to-end data engineering workflows  

---

## 💡 Tips for Success

- Run each day's script independently to understand individual concepts
- Experiment with the sample datasets (Customer.csv, Order.csv, sales.csv)
- Modify the code to practice and reinforce learning
- Combine techniques from multiple days for real-world projects

---

## 📞 Notes

- Each day builds upon previous concepts
- Sample CSV files are provided for practice
- Comments in code provide detailed explanations
- Refer to official documentation for each library for deeper learning

---

**Last Updated:** January 2026  
**Learning Path Status:** In Progress ✨
