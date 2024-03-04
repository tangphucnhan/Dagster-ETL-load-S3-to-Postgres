from dagster import job
from dagster_aws.s3 import s3_resource
from cafebills.ops.ops_with_aws import fetch_s3_objects, fetch_s3_objects_with_boto3, load_to_postgres
from cafebills.constants import *


@job(resource_defs={"s3": s3_resource})
def job_cafebills_s3():
    df = fetch_s3_objects()
    load_to_postgres(df)


job_cafebills_s3.execute_in_process(
    run_config={
        "resources": {
            "s3": {
                "config": {
                    "region_name": AWS_REGION_NAME,
                    "aws_access_key_id": AWS_ACCESS_KEY_ID,
                    "aws_secret_access_key": AWS_SECRET_ACCESS_KEY,
                    "endpoint_url": AWS_ENDPOINT_URL,
                    "use_ssl": True,
                }
            }
        }
    }
)


"""
To run job with boto3:
 comment above job_cafebills_s3() and statement job_cafebills_s3.execute_in_process()
 uncomment below job_cafebills_s3() function
"""

# @job
# def job_cafebills_s3():
#     df = fetch_s3_objects_with_boto3()
#     load_to_postgres(df)
