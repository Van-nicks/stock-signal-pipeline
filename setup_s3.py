import os
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv
from loguru import logger

load_dotenv()

BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
REGION      = os.getenv("AWS_DEFAULT_REGION", "us-east-1")

def create_bucket():
    s3 = boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=REGION
    )

    try:
        # us-east-1 doesn't accept LocationConstraint — all others do
        if REGION == "us-east-1":
            s3.create_bucket(Bucket=BUCKET_NAME)
        else:
            s3.create_bucket(
                Bucket=BUCKET_NAME,
                CreateBucketConfiguration={"LocationConstraint": REGION}
            )
        logger.info(f"Bucket created: s3://{BUCKET_NAME}")
    except ClientError as e:
        if e.response["Error"]["Code"] == "BucketAlreadyOwnedByYou":
            logger.info(f"Bucket already exists and owned by you: {BUCKET_NAME}")
        else:
            raise

    # Block all public access — security baseline
    s3.put_public_access_block(
        Bucket=BUCKET_NAME,
        PublicAccessBlockConfiguration={
            "BlockPublicAcls":       True,
            "IgnorePublicAcls":      True,
            "BlockPublicPolicy":     True,
            "RestrictPublicBuckets": True
        }
    )
    logger.info("Public access blocked")

    # Enable versioning — protects against accidental deletes
    s3.put_bucket_versioning(
        Bucket=BUCKET_NAME,
        VersioningConfiguration={"Status": "Enabled"}
    )
    logger.info("Versioning enabled")

    # Lifecycle rule — delete old versions after 30 days to control cost
    s3.put_bucket_lifecycle_configuration(
        Bucket=BUCKET_NAME,
        LifecycleConfiguration={
            "Rules": [{
                "ID": "delete-old-versions",
                "Status": "Enabled",
                "Filter": {"Prefix": ""},
                "NoncurrentVersionExpiration": {"NoncurrentDays": 30}
            }]
        }
    )
    logger.info("Lifecycle policy set: old versions deleted after 30 days")

    # Test write — upload a small marker file
    s3.put_object(
        Bucket=BUCKET_NAME,
        Key="__init__/.keep",
        Body=b""
    )
    logger.info("Test write successful")

    # Verify bucket exists and is readable
    response = s3.list_objects_v2(Bucket=BUCKET_NAME, MaxKeys=5)
    logger.info(f"Bucket verified. Objects found: {response['KeyCount']}")
    logger.info(f"\nS3 Data Lake ready: s3://{BUCKET_NAME}/")
    logger.info("Structure will be:")
    logger.info(f"  s3://{BUCKET_NAME}/stocks/year=YYYY/month=MM/day=DD/SYMBOL.parquet")
    logger.info(f"  s3://{BUCKET_NAME}/crypto/year=YYYY/month=MM/day=DD/SYMBOL.parquet")


if __name__ == "__main__":
    create_bucket()