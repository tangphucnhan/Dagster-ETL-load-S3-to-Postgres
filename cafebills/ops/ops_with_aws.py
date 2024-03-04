import csv
import pandas as pd
from dagster import op, Out, Output, OpExecutionContext
from sqlalchemy import create_engine
import boto3
import boto3.resources.base
from cafebills.constants import *


@op(required_resource_keys={'s3'}, out={"df": Out(is_required=True)})
def fetch_s3_objects(context: OpExecutionContext):
    s3_res = context.resources.s3
    files: dict = s3_res.list_objects_v2(
        Bucket=S3_BUCKET,
        Prefix=S3_PREFIX,
    )
    cols = ["StoreId", "Item", "Quantity", "Price(USD)", "StaffId", "Datetime"]
    list_data = list()
    for file in files["Contents"]:
        if file["Size"] != 0:
            bytes_obj = s3_res.get_object(
                Bucket=S3_BUCKET,
                Key=file["Key"],
            )
            data = bytes_obj["Body"].read().decode()
            # Remove null bytes
            data = str(data).replace("\0", "").replace("'", "\"")
            data = data.split("\n")
            data.pop(0)
            data.remove("")
            data = csv.reader(data, delimiter=",")
            data = list(data)
            list_data.extend(data)
    df = pd.DataFrame(list_data, index=None, columns=cols)
    yield Output(df, "df")


@op(out={"df": Out(is_required=True)})
def fetch_s3_objects_with_boto3(context: OpExecutionContext):
    s3_res = boto3.resource(
        "s3",
        region_name=AWS_REGION_NAME,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        endpoint_url=AWS_ENDPOINT_URL,
        use_ssl=True,
    ).meta.client

    files: dict = s3_res.list_objects_v2(
        Bucket=S3_BUCKET,
        Prefix=S3_PREFIX,
    )
    cols = ["StoreId", "Item", "Quantity", "Price(USD)", "StaffId", "Datetime"]
    list_data = list()
    for file in files["Contents"]:
        if file["Size"] != 0:
            bytes_obj = s3_res.get_object(
                Bucket=S3_BUCKET,
                Key=file["Key"],
            )
            data = bytes_obj["Body"].read().decode()
            # Remove null bytes
            data = str(data).replace("\0", "").replace("'", "\"")
            data = data.split("\n")
            data.pop(0)
            data.remove("")
            data = csv.reader(data, delimiter=",")
            data = list(data)
            list_data.extend(data)
    df = pd.DataFrame(list_data, index=None, columns=cols)
    yield Output(df, "df")


@op
def load_to_postgres(context: OpExecutionContext, df: pd.DataFrame):
    try:
        engine = create_engine(f"postgresql://{DB_USER}:{DB_PWD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
        count = df.to_sql(name='daily_report', con=engine, index=False, if_exists='replace')
        context.log.info(f"!!!Inserted {count} rows of data")
    except Exception as e:
        context.log.info("Load data error:")
        context.log.info(e)
