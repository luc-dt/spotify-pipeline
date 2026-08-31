"""
scripts/verify_aws_credentials.py
---------------------------------
Diagnostic tool to verify AWS connectivity and audit existing cloud resources
(S3 buckets, Lambda functions, Glue databases/jobs) before starting Day 3.
"""

import os
import boto3
from dotenv import load_dotenv

# 1. Load AWS Credentials from .env
load_dotenv()

AWS_REGION = os.getenv("AWS_REGION", "ap-southeast-2")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")


def inspect_aws_environment():
    print("=" * 70)
    print("☁️  AWS RESOURCE AUDIT & HEALTH CHECK")
    print("=" * 70)

    if not AWS_ACCESS_KEY_ID or not AWS_SECRET_ACCESS_KEY:
        print("❌ Missing AWS Credentials in .env!")
        print("   Please ensure AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY are set.")
        print("=" * 70)
        return

    # 2. Establish Boto3 Session
    session = boto3.Session(
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION,
    )

    # 3. Step 2 of Day 3: Verify Caller Identity (STS)
    print("1️⃣  CALLER IDENTITY (Security Token Service):")
    try:
        sts = session.client("sts")
        identity = sts.get_caller_identity()
        print(f"   ✓ Authenticated IAM User : {identity.get('Arn')}")
        print(f"   ✓ AWS Account ID         : {identity.get('Account')}")
        print(f"   ✓ Configured Region      : {AWS_REGION}\n")
    except Exception as e:
        print(f"   ❌ Authentication Failed: {e}\n")
        return

    # 4. List S3 Buckets
    print("2️⃣  S3 BUCKETS:")
    try:
        s3 = session.client("s3")
        buckets = s3.list_buckets().get("Buckets", [])
        if buckets:
            for b in buckets:
                print(f"   • {b['Name']} (Created: {b['CreationDate'].strftime('%Y-%m-%d')})")
        else:
            print("   (No S3 buckets found)")
        print()
    except Exception as e:
        print(f"   ❌ Error listing S3 buckets: {e}\n")

    # 5. List Lambda Functions
    print("3️⃣  LAMBDA FUNCTIONS:")
    try:
        lam = session.client("lambda")
        functions = lam.list_functions().get("Functions", [])
        if functions:
            for fn in functions:
                print(f"   • {fn['FunctionName']} (Runtime: {fn.get('Runtime')})")
        else:
            print("   (No Lambda functions found)")
        print()
    except Exception as e:
        print(f"   ❌ Error listing Lambda functions: {e}\n")

    # 6. List Glue Databases & Jobs
    print("4️⃣  GLUE DATABASES & JOBS:")
    try:
        glue = session.client("glue")
        databases = glue.get_databases().get("DatabaseList", [])
        print("   Databases:")
        if databases:
            for db in databases:
                print(f"     • {db['Name']}")
        else:
            print("     (None)")

        jobs = glue.get_jobs().get("Jobs", [])
        print("\n   Jobs:")
        if jobs:
            for j in jobs:
                print(f"     • {j['Name']}")
        else:
            print("     (None)")
    except Exception as e:
        print(f"   ❌ Error listing Glue resources: {e}")

    print("=" * 70)


if __name__ == "__main__":
    inspect_aws_environment()
