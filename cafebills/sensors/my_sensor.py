from dagster import RunRequest, sensor
from cafebills.jobs.etl_aws_s3 import job_cafebills_s3


@sensor(job=job_cafebills_s3)
def my_sensor(context):
    should_run = True
    if should_run:
        yield RunRequest(run_key=None, run_config={})
