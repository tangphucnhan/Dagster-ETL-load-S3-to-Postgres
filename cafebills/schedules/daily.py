from dagster import ScheduleDefinition, DefaultScheduleStatus
from cafebills.jobs.etl_aws_s3 import job_cafebills_s3


daily_running = ScheduleDefinition(
    job=job_cafebills_s3,
    cron_schedule="0 17 * * *",
    execution_timezone="Asia/Singapore",
    default_status=DefaultScheduleStatus.RUNNING
)
