"""
╔════════════════════════════════════════════════════════════════════════════════╗
║                      DAY 2: PANDAS ADVANCED                                    ║
║                                                                                ║
║  Learning Objectives:                                                          ║
║  1. Perform different types of joins (Inner, Left, Right, Outer)               ║
║  2. Master groupby with multiple aggregations                                  ║
║  3. Create pivot tables for data summarization                                 ║
║  4. Combine multiple datasets                                                  ║
║                                                                                ║
║  Key Concepts:                                                                 ║
║  - pd.merge(): Combines two DataFrames based on a common column                ║
║  - Different join types: INNER, LEFT, RIGHT, OUTER                            ║
║  - .agg(): Performs multiple aggregations simultaneously                       ║
║  - pivot_table(): Reshapes data for multi-dimensional analysis                 ║
║                                                                                ║
║  Use Case:                                                                     ║
║  Analyzing customer orders: combining customer info with orders,              ║
║  calculating totals, and creating summary reports                              ║
╚════════════════════════════════════════════════════════════════════════════════╝
"""

import pandas as pd

# ──────────────────────────────────────────────────────────────────────────────
# EXERCISE 1: Load Multiple CSV Files
# ──────────────────────────────────────────────────────────────────────────────
print("=" * 80)
print("EXERCISE 1: Loading Multiple CSV Files")
print("=" * 80)
print("\nExplanation:")
print("- Load Customer.csv containing customer information")
print("- Load Order.csv containing order details\n")

df = pd.read_csv("Customer.csv")
print("Customer Data:")
print(df)

df = pd.read_csv("Order.csv")
print("\nOrder Data:")
print(df)

# ──────────────────────────────────────────────────────────────────────────────
# EXERCISE 2: Join Real CSV Files
# ──────────────────────────────────────────────────────────────────────────────
print("\n" + "=" * 80)
print("EXERCISE 2: Inner Join with Real CSV Files")
print("=" * 80)

inner_join_df = pd.merge(pd.read_csv("Customer.csv"), pd.read_csv("Order.csv"), on="customer_id")
print("\nInner Join Result:")
print(inner_join_df)

# ──────────────────────────────────────────────────────────────────────────────
# EXERCISE 3: Create Sample DataFrames for Demonstrations
# ──────────────────────────────────────────────────────────────────────────────
print("\n" + "=" * 80)
print("EXERCISE 3: Creating Sample DataFrames")
print("=" * 80)
print("\nExplanation:")
print("- Create custom DataFrames from scratch using dictionaries")
print("- Useful for demonstrations and testing\n")

customers = pd.DataFrame({
    "customer_id": [1, 2, 3],
    "customer_name": ["Amit", "Neha", "Ravi"],
    "city": ["Delhi", "Mumbai", "Delhi"]
})
print("Customers DataFrame:")
print(customers)

orders = pd.DataFrame({
    "order_id": [101, 102, 103, 104],
    "customer_id": [1, 2, 1, 3],
    "amount": [500, 700, 300, 400]
})
print("\nOrders DataFrame:")
print(orders)

# ──────────────────────────────────────────────────────────────────────────────
# EXERCISE 4: Different Types of Joins
# ──────────────────────────────────────────────────────────────────────────────
print("\n" + "=" * 80)
print("EXERCISE 4: Different Types of Joins")
print("=" * 80)

# 4.1 INNER JOIN
print("\n4.1 - INNER JOIN")
print("─" * 40)
print("Returns only rows that match in BOTH tables")
print("(customer_id must exist in both DataFrames)\n")
merged_df = pd.merge(customers, orders, on="customer_id")
print(merged_df)

# 4.2 LEFT JOIN
print("\n4.2 - LEFT JOIN")
print("─" * 40)
print("Returns ALL rows from LEFT table (customers)")
print("Unmatched orders get NaN (missing values)\n")
left_join_df = pd.merge(customers, orders, on="customer_id", how="left")
print(left_join_df)

# 4.3 RIGHT JOIN
print("\n4.3 - RIGHT JOIN")
print("─" * 40)
print("Returns ALL rows from RIGHT table (orders)")
print("Unmatched customers get NaN\n")
right_join_df = pd.merge(customers, orders, on="customer_id", how="right")
print(right_join_df)

# 4.4 OUTER JOIN
print("\n4.4 - OUTER JOIN")
print("─" * 40)
print("Returns ALL rows from BOTH tables")
print("Non-matching rows get NaN\n")
outer_join_df = pd.merge(customers, orders, on="customer_id", how="outer")
print(outer_join_df)

# ──────────────────────────────────────────────────────────────────────────────
# EXERCISE 5: GroupBy with Single Aggregation
# ──────────────────────────────────────────────────────────────────────────────
print("\n" + "=" * 80)
print("EXERCISE 5: GroupBy with Single Aggregation")
print("=" * 80)
print("\nExplanation:")
print("- Group orders by customer name")
print("- Sum the order amounts for each customer")
print("- Result: Total spending per customer\n")

total_amount = merged_df.groupby("customer_name")["amount"].sum()
print(total_amount)

# ──────────────────────────────────────────────────────────────────────────────
# EXERCISE 6: Multiple Aggregations with .agg()
# ──────────────────────────────────────────────────────────────────────────────
print("\n" + "=" * 80)
print("EXERCISE 6: Multiple Aggregations with .agg()")
print("=" * 80)
print("\nExplanation:")
print("- Use .agg() to perform MULTIPLE calculations simultaneously")
print("- Calculate total_amount (SUM) and count_orders (COUNT)")
print("- Result: Summary table with multiple metrics per customer\n")

summary = merged_df.groupby("customer_name").agg(
    total_amount=("amount", "sum"),
    count_orders=("order_id", "count")
)
print(summary)

# ──────────────────────────────────────────────────────────────────────────────
# EXERCISE 7: Pivot Tables
# ──────────────────────────────────────────────────────────────────────────────
print("\n" + "=" * 80)
print("EXERCISE 7: Pivot Tables")
print("=" * 80)
print("\nExplanation:")
print("- Reshape data: rows = customer_name, columns = city, values = amount sum")
print("- Fill missing values with 0 (instead of NaN)")
print("- Useful for cross-tabulation and multi-dimensional analysis\n")

pivot = pd.pivot_table(
    merged_df,
    values="amount",
    index="customer_name",
    columns="city",
    aggfunc="sum",
    fill_value=0
)
print(pivot)
print("\nPivot Table Insights:")
print(f"- Shows amount spent by each customer in each city")
print(f"- 0 indicates no transactions between that customer-city pair")