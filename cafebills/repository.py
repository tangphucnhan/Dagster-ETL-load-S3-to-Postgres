from dagster import repository
from cafebills.jobs.etl_aws_s3 import job_cafebills_s3
from cafebills.schedules.daily import daily_running
from cafebills.sensors.my_sensor import my_sensor


@repository
def circle():
    schedules = [daily_running]
    jobs = [job_cafebills_s3]
    sensors = [my_sensor]
    return schedules + jobs + sensors
