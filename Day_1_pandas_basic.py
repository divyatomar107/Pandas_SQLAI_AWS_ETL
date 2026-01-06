"""
╔════════════════════════════════════════════════════════════════════════════════╗
║                        DAY 1: PANDAS BASICS                                    ║
║                                                                                ║
║  Learning Objectives:                                                          ║
║  1. Load CSV data into a Pandas DataFrame                                      ║
║  2. Perform basic grouping and aggregation                                     ║
║  3. Filter data based on conditions                                            ║
║                                                                                ║
║  Key Concepts:                                                                 ║
║  - pd.read_csv(): Reads CSV files into DataFrames                              ║
║  - groupby(): Groups data by one or more columns                               ║
║  - Boolean indexing: Filters rows based on conditions                          ║
║                                                                                ║
║  Use Case:                                                                     ║
║  Analyzing sales data to find total sales by region and identify              ║
║  high-value transactions (> 5000)                                              ║
╚════════════════════════════════════════════════════════════════════════════════╝
"""

import pandas as pd

# ──────────────────────────────────────────────────────────────────────────────
# EXERCISE 1: Load CSV file into DataFrame
# ──────────────────────────────────────────────────────────────────────────────
# pd.read_csv() reads a CSV file and creates a DataFrame
# A DataFrame is like an Excel spreadsheet with rows and columns
print("=" * 80)
print("EXERCISE 1: Loading CSV File")
print("=" * 80)
df = pd.read_csv("sales.csv")
print("\nSales Data:")
print(df)
print(f"\nDataFrame Shape: {df.shape[0]} rows, {df.shape[1]} columns")
print(f"Columns: {df.columns.tolist()}")

# ──────────────────────────────────────────────────────────────────────────────
# EXERCISE 2: Grouping - Total Sales per Region
# ──────────────────────────────────────────────────────────────────────────────
# groupby() groups data by column values
# .sum() aggregates the grouped values by addition
# This is useful for generating summary reports
print("\n" + "=" * 80)
print("EXERCISE 2: Grouping - Total Sales per Region")
print("=" * 80)
print("\nExplanation:")
print("- Group all rows by 'Region' column")
print("- Sum the 'Sales' values for each region")
print("- Result: Total sales amount for each region\n")

total_sales_per_region = df.groupby("Region")["Sales"].sum()
print(total_sales_per_region)
print(f"\nHighest Region Sales: {total_sales_per_region.max()}")
print(f"Lowest Region Sales: {total_sales_per_region.min()}")

# ──────────────────────────────────────────────────────────────────────────────
# EXERCISE 3: Filtering - Sales Greater than 5000
# ──────────────────────────────────────────────────────────────────────────────
# Boolean indexing filters rows where a condition is True
# df["Sales"] > 5000 creates a boolean mask (True/False for each row)
# Apply this mask to filter only rows with high sales
print("\n" + "=" * 80)
print("EXERCISE 3: Filtering - Sales Greater than 5000")
print("=" * 80)
print("\nExplanation:")
print("- Create a boolean filter: Sales > 5000")
print("- Apply filter to DataFrame")
print("- Result: Only rows where Sales is greater than 5000\n")

high_sales = df[df["Sales"] > 5000]
print(high_sales)
print(f"\nTotal High-Value Transactions: {len(high_sales)}")
print(f"Average High Sales: {high_sales['Sales'].mean():.2f}")