"""
╔════════════════════════════════════════════════════════════════════════════════╗
║               DAY 5: BOTO3 & AWS S3 - CLOUD STORAGE                            ║
║                                                                                ║
║  Learning Objectives:                                                          ║
║  1. Connect to AWS S3 using Boto3                                              ║
║  2. Create and manage S3 buckets                                               ║
║  3. Upload and download files from S3                                          ║
║  4. List objects in S3 buckets                                                 ║
║  5. Export DynamoDB tables to S3 for backup                                    ║
║                                                                                ║
║  Key Concepts:                                                                 ║
║  - boto3: AWS SDK for Python                                                   ║
║  - S3 (Simple Storage Service): Object storage service                         ║
║  - Bucket: Root container for S3 objects (like folders)                        ║
║  - Object Key: Full path to file in S3 (e.g., 'uploads/photo.jpg')             ║
║  - Region: Geographic location where bucket stores data                        ║
║  - Cross-service Integration: DynamoDB → S3 backups                            ║
║                                                                                ║
║  Prerequisites:                                                                ║
║  - AWS Account with S3 permissions                                             ║
║  - Boto3 installed: pip install boto3                                          ║
║  - AWS credentials configured (aws configure)                                  ║
║                                                                                ║
║  Use Case:                                                                     ║
║  Building a file management and backup system that uploads documents to       ║
║  cloud storage and maintains database backups                                  ║
╚════════════════════════════════════════════════════════════════════════════════╝
"""

import boto3

print("=" * 80)
print("AWS S3 CONNECTION SETUP")
print("=" * 80)
print("\nExplanation:")
print("- boto3.client() creates a connection to AWS S3 service")
print("- Requires AWS credentials configured locally")
print("- 's3' specifies we're connecting to S3 service\n")

# Initialize S3 client
s3 = boto3.client('s3')

# ──────────────────────────────────────────────────────────────────────────────
# EXERCISE 1: Create S3 Bucket
# ──────────────────────────────────────────────────────────────────────────────
print("=" * 80)
print("EXERCISE 1: Create S3 Bucket")
print("=" * 80)
print("\nExplanation:")
print("- Bucket name must be globally unique across all AWS accounts")
print("- CreateBucketConfiguration specifies region")
print("- Region 'ap-south-1' = Mumbai")
print("- Bucket acts as a root folder for all objects\n")

print("Code Example:")
print("""
s3 = boto3.client('s3')

bucket_name = "divya-dynamodb-backup-dec2025"
region = "ap-south-1"

response = s3.create_bucket(
    Bucket=bucket_name,
    CreateBucketConfiguration={'LocationConstraint': region}
)

print(f"Bucket {bucket_name} created in {region}")
""")

# Uncomment to create actual bucket
# bucket_name = "divya-dynamodb-backup-dec2025"
# region = "ap-south-1"
#
# response = s3.create_bucket(
#     Bucket=bucket_name,
#     CreateBucketConfiguration={'LocationConstraint': region}
# )
# print(f"✅ Bucket {bucket_name} created in {region}")

# ──────────────────────────────────────────────────────────────────────────────
# EXERCISE 2: List All Buckets
# ──────────────────────────────────────────────────────────────────────────────
print("\n" + "=" * 80)
print("EXERCISE 2: List All S3 Buckets")
print("=" * 80)
print("\nExplanation:")
print("- list_buckets() returns all buckets in your AWS account")
print("- Useful for discovering existing buckets")
print("- Response contains bucket metadata\n")

print("Code Example:")
print("""
response = s3.list_buckets()

print("Your S3 Buckets:")
for bucket in response['Buckets']:
    print(f"  - {bucket['Name']}")
""")

# List all buckets
response = s3.list_buckets()
print("\nYour S3 Buckets:")
if 'Buckets' in response:
    for bucket in response['Buckets']:
        print(f"  - {bucket['Name']}")
else:
    print("  (No buckets found)")

# ──────────────────────────────────────────────────────────────────────────────
# EXERCISE 3: Upload File to S3
# ──────────────────────────────────────────────────────────────────────────────
print("\n" + "=" * 80)
print("EXERCISE 3: Upload File to S3 Bucket")
print("=" * 80)
print("\nExplanation:")
print("- upload_file() uploads a local file to S3")
print("- file_path: Location on your computer")
print("- bucket_name: Target S3 bucket")
print("- object_name: Path/key where file appears in S3 (can include folders)")
print("- Useful for: backups, document management, media storage\n")

print("Code Example:")
print("""
s3 = boto3.client('s3')

bucket_name = "divya-dynamodb-backup-dec2025"
file_path = r"C:\\Users\\Hp\\OneDrive\\Desktop\\Medicine_Bill_01.jpg"
object_name = "uploads/Medicine_Bill_01.jpg"

s3.upload_file(file_path, bucket_name, object_name)
print(f"File uploaded to S3 as {object_name}")
""")

# Uncomment to upload actual file
# bucket_name = "divya-dynamodb-backup-dec2025"
# file_path = r"C:\Users\Hp\OneDrive\Desktop\Medicine_Bill_01.jpg"
# object_name = "uploads/Medicine_Bill_01.jpg"
#
# s3.upload_file(file_path, bucket_name, object_name)
# print(f"✅ File uploaded to S3: {object_name}")

# ──────────────────────────────────────────────────────────────────────────────
# EXERCISE 4: Download File from S3
# ──────────────────────────────────────────────────────────────────────────────
print("\n" + "=" * 80)
print("EXERCISE 4: Download File from S3")
print("=" * 80)
print("\nExplanation:")
print("- download_file() downloads a file from S3 to your local computer")
print("- bucket_name: Source S3 bucket")
print("- object_name: Path/key of file in S3")
print("- download_path: Where to save on your computer")
print("- Useful for: retrieving backups, sharing files, syncing data\n")

print("Code Example:")
print("""
s3 = boto3.client('s3')

bucket_name = "divya-dynamodb-backup-dec2025"
object_name = "uploads/Medicine_Bill_01.jpg"
download_path = r"C:\\Users\\Hp\\Divya Learning\\Pandas\\Downloaded_Bill.jpg"

s3.download_file(bucket_name, object_name, download_path)
print(f"File downloaded to {download_path}")
""")

# Uncomment to download actual file
# bucket_name = "divya-dynamodb-backup-dec2025"
# object_name = "uploads/Medicine_Bill_01.jpg"
# download_path = r"C:\Users\Hp\Divya Learning\Pandas\Downloaded_Bill.jpg"
#
# s3.download_file(bucket_name, object_name, download_path)
# print(f"✅ File downloaded: {download_path}")

# ──────────────────────────────────────────────────────────────────────────────
# EXERCISE 5: List Objects in S3 Bucket
# ──────────────────────────────────────────────────────────────────────────────
print("\n" + "=" * 80)
print("EXERCISE 5: List Objects in S3 Bucket")
print("=" * 80)
print("\nExplanation:")
print("- list_objects_v2() lists all objects (files) in a bucket")
print("- Contents: List of objects in the bucket")
print("- Key: Full path of the object")
print("- Useful for: inventory, data discovery, audit trails\n")

print("Code Example:")
print("""
s3 = boto3.client('s3')
bucket_name = "divya-dynamodb-backup-dec2025"

response = s3.list_objects_v2(Bucket=bucket_name)

print("Files in bucket:")
for obj in response.get('Contents', []):
    print(f"  - {obj['Key']}")
""")

# List objects in bucket
bucket_name = "divya-dynamodb-backup-dec2025"
response = s3.list_objects_v2(Bucket=bucket_name)

print("\nFiles in 'divya-dynamodb-backup-dec2025' bucket:")
if 'Contents' in response:
    for obj in response['Contents']:
        print(f"  - {obj['Key']} (Size: {obj['Size']} bytes)")
else:
    print("  (Bucket is empty or doesn't exist)")

# ──────────────────────────────────────────────────────────────────────────────
# EXERCISE 6: Export DynamoDB to S3
# ──────────────────────────────────────────────────────────────────────────────
print("\n" + "=" * 80)
print("EXERCISE 6: Export DynamoDB Table to S3 (Backup)")
print("=" * 80)
print("\nExplanation:")
print("- export_table_to_point_in_time() backs up entire DynamoDB table to S3")
print("- Uses native AWS export (more efficient than scanning)")
print("- TableArn: Amazon Resource Name of DynamoDB table")
print("- ExportFormat: DYNAMODB_JSON or ION")
print("- Useful for: disaster recovery, data archival, compliance\n")

print("Code Example:")
print("""
dynamodb = boto3.client('dynamodb')

response = dynamodb.export_table_to_point_in_time(
    TableArn="arn:aws:dynamodb:ap-south-1:YOUR_ACCOUNT_ID:table/Users",
    S3Bucket="divya-dynamodb-backup-dec2025",
    ExportFormat="DYNAMODB_JSON"
)

print("Export started")
print("Check S3 bucket for exported files")
""")

# Uncomment to export (requires correct TableArn)
# dynamodb = boto3.client('dynamodb')
#
# response = dynamodb.export_table_to_point_in_time(
#     TableArn="arn:aws:dynamodb:ap-south-1:181777504093:table/Users",
#     S3Bucket="divya-dynamodb-backup-dec2025",
#     ExportFormat="DYNAMODB_JSON"
# )
#
# print("✅ Export started")
# print(response)

print("\n" + "=" * 80)
print("SUMMARY - S3 Operations & Use Cases")
print("=" * 80)
print("""
✅ Core S3 Operations:

1. CREATE BUCKET:
   - Create new storage containers
   - Globally unique names required
   - Choose appropriate region
   
2. UPLOAD FILE:
   - Move data from local/other services to S3
   - Supports any file type
   - Can organize with folder-like keys
   
3. DOWNLOAD FILE:
   - Retrieve files from S3 to local system
   - Essential for backup recovery
   
4. LIST OBJECTS:
   - Discover what's in your bucket
   - Monitor storage usage
   - Audit data in bucket
   
5. EXPORT DYNAMODB:
   - Automatic backup without scanning
   - Point-in-time recovery capability
   - Efficient for large tables

🎯 Real-World Use Cases:

📦 Backup & Disaster Recovery:
   - Regular database exports to S3
   - Long-term archival storage
   - Cross-region replication for HA

📄 Document Management:
   - Store invoices, bills, reports
   - Version control with S3 versioning
   - Access control with IAM policies

🎬 Media Storage:
   - Images, videos, audio files
   - CDN integration with CloudFront
   - Multimedia content delivery

💾 Data Lake:
   - Central repository for all data
   - Mix structured and unstructured data
   - Analytics with Athena/Redshift

🔐 Security Features:
   - Server-side encryption
   - Access control (bucket policies, ACLs)
   - Logging and monitoring
   - Multi-factor delete protection

💰 Cost Optimization:
   - Storage classes (Standard, Glacier, Deep Archive)
   - Lifecycle policies for archival
   - Pay-as-you-go pricing

⚡ Performance Tips:
   - Use S3 Transfer Acceleration
   - Multipart uploads for large files
   - CloudFront for faster downloads
""")



