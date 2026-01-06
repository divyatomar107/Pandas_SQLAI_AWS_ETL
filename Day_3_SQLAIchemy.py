"""
╔════════════════════════════════════════════════════════════════════════════════╗
║                    DAY 3: SQLALCHEMY - DATABASE ORM                            ║
║                                                                                ║
║  Learning Objectives:                                                          ║
║  1. Create database connections with SQLAlchemy                                ║
║  2. Load CSV data into SQL databases                                           ║
║  3. Perform ETL (Extract, Transform, Load) operations                          ║
║  4. Implement CRUD operations (Create, Read, Update, Delete)                   ║
║  5. Define and manage database models with ORM                                 ║
║                                                                                ║
║  Key Concepts:                                                                 ║
║  - create_engine(): Establishes database connections                           ║
║  - ORM (Object-Relational Mapping): Maps Python classes to DB tables           ║
║  - CRUD Operations: Basic database operations                                  ║
║  - ETL Pipeline: Data extraction, transformation, and loading                  ║
║  - Session Management: Handles transactions and commits                        ║
║                                                                                ║
║  Use Case:                                                                     ║
║  Building a user management system with database persistence,                 ║
║  managing user records, and generating sales summaries                         ║
╚════════════════════════════════════════════════════════════════════════════════╝
"""

import sqlalchemy
import pandas as pd
from sqlalchemy import create_engine, text, Column, Integer, String
from sqlalchemy.orm import declarative_base, sessionmaker

# ──────────────────────────────────────────────────────────────────────────────
# SETUP: Create Database Connection
# ──────────────────────────────────────────────────────────────────────────────
print("=" * 80)
print("SETUP: Database Connection")
print("=" * 80)
print("\nExplanation:")
print("- create_engine() creates a connection to SQLite database")
print("- 'sqlite:///users.db' = SQLite database named 'users.db' in current folder")
print("- echo=True shows all SQL commands executed (useful for learning)\n")

# Create SQLite database file (user.db) in current folder
engine = create_engine("sqlite:///users.db", echo=True)  # echo=True shows SQL logs
Base = declarative_base()

# ──────────────────────────────────────────────────────────────────────────────
# EXERCISE 1: Load CSV into SQLite Database
# ──────────────────────────────────────────────────────────────────────────────
print("\n" + "=" * 80)
print("EXERCISE 1: Load CSV into SQLite (Pandas + SQLAlchemy)")
print("=" * 80)
print("\nExplanation:")
print("- Read CSV file using Pandas")
print("- Convert DataFrame to SQL table using .to_sql()")
print("- Query the data back to verify\n")

# Extract: Read CSV
df = pd.read_csv("sales.csv")
print("CSV Data Loaded:")
print(df.head())

# Load: Save to SQLite
df.to_sql("sales_table", con=engine, if_exists="replace", index=False)
print("\n✅ Data saved to 'sales_table' in SQLite")

# Query back from SQLite to verify
query_df = pd.read_sql("SELECT * FROM sales_table", con=engine)
print("\nVerification - Data from SQLite:")
print(query_df)

# ──────────────────────────────────────────────────────────────────────────────
# EXERCISE 2: ETL Process (Extract, Transform, Load)
# ──────────────────────────────────────────────────────────────────────────────
print("\n" + "=" * 80)
print("EXERCISE 2: ETL Process - Sales Summary Pipeline")
print("=" * 80)
print("\nExplanation:")
print("1. EXTRACT: Load CSV data into Pandas DataFrame")
print("2. TRANSFORM: Aggregate sales by region (groupby + sum)")
print("3. LOAD: Save aggregated results to SQLite\n")

# Step 1: Extract - Load CSV
print("Step 1: EXTRACT")
df = pd.read_csv("sales.csv")
print(f"  Loaded {len(df)} sales records")

# Step 2: Transform - Aggregate
print("\nStep 2: TRANSFORM")
total_sales = df.groupby("Region")["Sales"].sum()
print("  Aggregated sales by region:")
print(total_sales)

# Step 3: Load - Save to Database
print("\nStep 3: LOAD")
total_sales.to_sql("sales_summary", con=engine, if_exists="replace")
print("  ✅ Summary saved to 'sales_summary' table")

# Query results
query_df = pd.read_sql("SELECT * FROM sales_summary", con=engine)
print("\nETL Result:")
print(query_df)

# ──────────────────────────────────────────────────────────────────────────────
# EXERCISE 3: Define ORM Model
# ──────────────────────────────────────────────────────────────────────────────
print("\n" + "=" * 80)
print("EXERCISE 3: Define ORM Model (Object-Relational Mapping)")
print("=" * 80)
print("\nExplanation:")
print("- Define a Python class 'User' that represents a database table")
print("- Each class attribute = database column")
print("- ORM automatically handles SQL operations\n")

class User(Base):
    """
    User model for database persistence
    Maps to 'users' table in the database
    """
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String)
    email = Column(String, unique=True)

    def __repr__(self):
        return f"<User(id={self.id}, name='{self.name}', email='{self.email}')>"

print("User Model defined with attributes:")
print("  - id: Integer (Primary Key, Auto-increment)")
print("  - name: String")
print("  - email: String (Unique constraint)")

# ──────────────────────────────────────────────────────────────────────────────
# EXERCISE 4: Create Table Schema
# ──────────────────────────────────────────────────────────────────────────────
print("\n" + "=" * 80)
print("EXERCISE 4: Create Table Schema")
print("=" * 80)
print("\nExplanation:")
print("- Base.metadata.create_all() creates all tables defined in models\n")

Base.metadata.create_all(engine)
print("✅ 'users' table created in database")

# ──────────────────────────────────────────────────────────────────────────────
# EXERCISE 5: CRUD Operations
# ──────────────────────────────────────────────────────────────────────────────
print("\n" + "=" * 80)
print("EXERCISE 5: CRUD Operations (Create, Read, Update, Delete)")
print("=" * 80)

# Create Session
Session = sessionmaker(bind=engine)
session = Session()

# ──────────────────────────────────────────────────────────────────────────────
# 5.1 CREATE: Insert Users
# ──────────────────────────────────────────────────────────────────────────────
print("\n5.1 - CREATE: Insert New Users")
print("─" * 40)
print("Explanation:")
print("- Create User objects")
print("- Add to session with session.add()")
print("- Commit to save permanently\n")

user1 = User(name="Alice", email="alice@example.com")
user2 = User(name="Bob", email="bob@example.com")

session.add_all([user1, user2])
session.commit()
print("✅ Users inserted:")
print(f"  - {user1}")
print(f"  - {user2}")

# ──────────────────────────────────────────────────────────────────────────────
# 5.2 READ: Query Users
# ──────────────────────────────────────────────────────────────────────────────
print("\n5.2 - READ: Query All Users")
print("─" * 40)
print("Explanation:")
print("- Use session.query() to retrieve data")
print("- .all() returns all matching records\n")

all_users = session.query(User).all()
print("📖 All Users in Database:")
for user in all_users:
    print(f"  {user}")

# ──────────────────────────────────────────────────────────────────────────────
# 5.3 UPDATE: Modify User
# ──────────────────────────────────────────────────────────────────────────────
print("\n5.3 - UPDATE: Modify User Record")
print("─" * 40)
print("Explanation:")
print("- Query the user to update")
print("- Modify the attribute")
print("- Commit the changes\n")

bob = session.query(User).filter_by(name="Bob").first()
bob.email = "bob_new@example.com"
session.commit()
print(f"✏️ Updated Bob's record:")
print(f"  {bob}")

# ──────────────────────────────────────────────────────────────────────────────
# 5.4 DELETE: Remove User
# ──────────────────────────────────────────────────────────────────────────────
print("\n5.4 - DELETE: Remove User Record")
print("─" * 40)
print("Explanation:")
print("- Query the user to delete")
print("- Use session.delete()")
print("- Commit to finalize deletion\n")

alice = session.query(User).filter_by(name="Alice").first()
session.delete(alice)
session.commit()
print(f"🗑️ Deleted Alice from database")

# ──────────────────────────────────────────────────────────────────────────────
# EXERCISE 6: Final Query - Verify CRUD
# ──────────────────────────────────────────────────────────────────────────────
print("\n" + "=" * 80)
print("EXERCISE 6: Verify Final State")
print("=" * 80)

remaining_users = session.query(User).all()
print(f"\nRemaining users in database: {len(remaining_users)}")
for user in remaining_users:
    print(f"  {user}")

print("\n" + "=" * 80)
print("SUMMARY")
print("=" * 80)
print("""
✅ Concepts Covered:
  1. Database connections and engine creation
  2. CSV to Database pipeline (Pandas + SQLAlchemy)
  3. ETL operations (Extract, Transform, Load)
  4. ORM model definition
  5. CRUD operations on database records
  
🎯 Real-World Applications:
  - User management systems
  - Data pipelines and ETL
  - Replacing flat files with persistent databases
  - Multi-application data sharing via central database
""")
# -----------------------------
remaining_users = session.query(User).all()
print("\n📖 Remaining Users:")
for user in remaining_users:
    print(user)