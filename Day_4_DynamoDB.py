"""
╔════════════════════════════════════════════════════════════════════════════════╗
║                   DAY 4: DYNAMODB - NOSQL CLOUD DATABASE                       ║
║                                                                                ║
║  Learning Objectives:                                                          ║
║  1. Connect to AWS DynamoDB using Boto3                                        ║
║  2. Create and manage DynamoDB tables                                          ║
║  3. Perform CRUD operations on DynamoDB                                        ║
║  4. Query and update items in DynamoDB                                         ║
║  5. Understand NoSQL data model (partition keys, attributes)                   ║
║                                                                                ║
║  Key Concepts:                                                                 ║
║  - boto3: AWS SDK for Python                                                   ║
║  - DynamoDB: NoSQL database service on AWS                                     ║
║  - Partition Key: Primary key for partitioning data                            ║
║  - Items: Records in DynamoDB (JSON-like documents)                            ║
║  - CRUD: Create, Read, Update, Delete operations                               ║
║                                                                                ║
║  Prerequisites:                                                                ║
║  - AWS Account with credentials configured                                     ║
║  - Boto3 installed: pip install boto3                                          ║
║  - AWS CLI configured with credentials                                         ║
║                                                                                ║
║  Use Case:                                                                     ║
║  Building a user profile management system with cloud-based storage,          ║
║  handling flexible schema, and scaling automatically                           ║
╚════════════════════════════════════════════════════════════════════════════════╝
"""

import boto3

print("=" * 80)
print("DYNAMODB CONNECTION SETUP")
print("=" * 80)
print("\nExplanation:")
print("- boto3.resource() creates a connection to AWS DynamoDB")
print("- region_name='ap-south-1' = Mumbai region")
print("- Requires AWS credentials configured on your system\n")

# Connect to DynamoDB
dynamodb = boto3.resource('dynamodb', region_name='ap-south-1')

# ──────────────────────────────────────────────────────────────────────────────
# EXERCISE 1: Create DynamoDB Table
# ──────────────────────────────────────────────────────────────────────────────
print("=" * 80)
print("EXERCISE 1: Create DynamoDB Table")
print("=" * 80)
print("\nExplanation:")
print("- Define table schema with partition key (primary key)")
print("- Specify attribute types: S=String, N=Number, B=Binary")
print("- Set provisioned capacity (read/write units)")
print("- Wait for table to be created before using\n")

print("Code Structure:")
print("""
table = dynamodb.create_table(
    TableName="Users",                          # Table name
    KeySchema=[
        {"AttributeName": "user_id", "KeyType": "HASH"}  # Partition key
    ],
    AttributeDefinitions=[
        {"AttributeName": "user_id", "AttributeType": "S"}  # String type
    ],
    ProvisionedThroughput={
        "ReadCapacityUnits": 5,
        "WriteCapacityUnits": 5
    }
)

table.wait_until_exists()  # Wait for creation
print("✅ Table created")
""")

# Code commented out - uncomment to actually create table
# table = dynamodb.create_table(
#     TableName="Users",
#     KeySchema=[
#         {"AttributeName": "user_id", "KeyType": "HASH"}  # Partition key
#     ],
#     AttributeDefinitions=[
#         {"AttributeName": "user_id", "AttributeType": "S"}
#     ],
#     ProvisionedThroughput={
#         "ReadCapacityUnits": 5,
#         "WriteCapacityUnits": 5
#     }
# )
#
# table.wait_until_exists()
# print("✅ Table created:", table.table_status)

# ──────────────────────────────────────────────────────────────────────────────
# EXERCISE 2: Get Reference to Existing Table
# ──────────────────────────────────────────────────────────────────────────────
print("=" * 80)
print("EXERCISE 2: Get Reference to Existing Table")
print("=" * 80)
print("\nExplanation:")
print("- dynamodb.Table() gets a reference to an existing table")
print("- Use this to perform CRUD operations\n")

table = dynamodb.Table("Users")
print("✅ Connected to 'Users' table")

# ──────────────────────────────────────────────────────────────────────────────
# EXERCISE 3: CREATE (Insert Items)
# ──────────────────────────────────────────────────────────────────────────────
print("\n" + "=" * 80)
print("EXERCISE 3: CREATE - Insert Items into DynamoDB")
print("=" * 80)
print("\nExplanation:")
print("- put_item() inserts or overwrites an item")
print("- Item is a dictionary with attributes")
print("- user_id is the partition key (must be unique)")
print("- Other attributes are flexible (NoSQL advantage)\n")

print("Code Example:")
print("""
# Insert Item 1
table.put_item(Item={
    'user_id': '1',
    'name': 'Alice',
    'email': 'alice@gmail.com',
    'city': 'Delhi',
    'age': 30
})
print("✅ Item 1 inserted")

# Insert Item 2
table.put_item(Item={
    'user_id': '2',
    'name': 'Bob',
    'email': 'bob@gmail.com',
    'city': 'Mumbai',
    'age': 25
})
print("✅ Item 2 inserted")
""")

# Uncomment to actually insert items
# table.put_item(Item={'user_id': '1', 'name': 'Alice',
#                      'email': 'alice@gmail.com', 'city': 'Delhi', 'age': 30})
# print("✅ Item 1 inserted")
#
# table.put_item(Item={'user_id': '2', 'name': 'Bob', 'email': 'bob@gmail.com',
#                      'city': 'Mumbai', 'age': 25})
# print("✅ Item 2 inserted")

# ──────────────────────────────────────────────────────────────────────────────
# EXERCISE 4: READ (Get Item)
# ──────────────────────────────────────────────────────────────────────────────
print("=" * 80)
print("EXERCISE 4: READ - Retrieve Item from DynamoDB")
print("=" * 80)
print("\nExplanation:")
print("- get_item() retrieves a single item by partition key")
print("- Must specify the Key (partition key value)")
print("- Returns the item as a dictionary\n")

print("Code Example:")
print("""
response = table.get_item(Key={'user_id': '1'})
print("Item:", response['Item'])
""")

# Uncomment to read item
# response = table.get_item(Key={'user_id': '1'})
# print("\n📖 Item with user_id 1:")
# if 'Item' in response:
#     print(response['Item'])
# else:
#     print("Item not found")

# Perform the read operation (assuming table has data)
response = table.get_item(Key={'user_id': '1'})
print("\n📖 Item with user_id 1:")
if 'Item' in response:
    print(response['Item'])
else:
    print("(Item not found - table may be empty)")

# ──────────────────────────────────────────────────────────────────────────────
# EXERCISE 5: UPDATE (Modify Item)
# ──────────────────────────────────────────────────────────────────────────────
print("\n" + "=" * 80)
print("EXERCISE 5: UPDATE - Modify Existing Item")
print("=" * 80)
print("\nExplanation:")
print("- update_item() modifies specific attributes of an item")
print("- UpdateExpression: Specifies what to update (SET, ADD, DELETE, REMOVE)")
print("- ExpressionAttributeValues: Placeholder values (:new_age)")
print("- Item must exist before updating (use put_item first if needed)\n")

print("Code Example:")
print("""
table.update_item(
    Key={'user_id': '2'},
    UpdateExpression="SET age = :new_age",
    ExpressionAttributeValues={':new_age': 26}
)
print("✏️ Updated age for user_id 2")
""")

# Perform update
table.update_item(
    Key={'user_id': '2'},
    UpdateExpression="SET age = :new_age",
    ExpressionAttributeValues={':new_age': 26}
)
print("\n✏️ Updated age for user_id 2 to 26")

# ──────────────────────────────────────────────────────────────────────────────
# EXERCISE 6: DELETE (Remove Item)
# ──────────────────────────────────────────────────────────────────────────────
print("\n" + "=" * 80)
print("EXERCISE 6: DELETE - Remove Item from DynamoDB")
print("=" * 80)
print("\nExplanation:")
print("- delete_item() removes an item from the table")
print("- Requires the partition key")
print("- Item is permanently deleted\n")

print("Code Example:")
print("""
table.delete_item(Key={'user_id': '1'})
print("🗑️ Deleted item with user_id 1")
""")

# Uncomment to delete item
# table.delete_item(Key={'user_id': '1'})
# print("\n🗑️ Deleted item with user_id 1")

print("\n" + "=" * 80)
print("SUMMARY - DynamoDB CRUD Operations")
print("=" * 80)
print("""
✅ Operations Covered:

1. CREATE (put_item):
   - Add new records to table
   - Automatically creates if user_id is new
   
2. READ (get_item):
   - Retrieve single record by primary key
   - Fast O(1) lookup
   
3. UPDATE (update_item):
   - Modify existing attributes
   - Partial updates (no need to replace entire item)
   - Supports SET, ADD, REMOVE, DELETE operations
   
4. DELETE (delete_item):
   - Remove record from table
   - Permanent deletion

🎯 DynamoDB Advantages:
   - Flexible schema (no rigid columns)
   - Auto-scaling throughput
   - Fully managed by AWS
   - High availability and durability
   - Global tables for multi-region replication

⚠️ Key Differences from SQL:
   - No JOIN operations (denormalize data instead)
   - No complex queries (design queries around access patterns)
   - Pay per request (no fixed costs)
   - Partition key is mandatory (design around this)
""")


