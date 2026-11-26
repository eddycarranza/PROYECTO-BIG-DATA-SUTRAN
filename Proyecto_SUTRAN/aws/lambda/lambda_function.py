"""Starts the SUTRAN Glue pipeline for a newly uploaded accident CSV."""

from urllib.parse import unquote_plus

import boto3

BUCKET = "sutran-g5-datos-2026-4821"
PREFIX = "entradas/accidentes/"
JOB_NAME = "sutran-pipeline-completo"
glue = boto3.client("glue")


def lambda_handler(event, context):
    keys = []
    for record in event.get("Records", []):
        if not record.get("eventName", "").startswith("ObjectCreated:"):
            continue
        s3_record = record["s3"]
        key = unquote_plus(s3_record["object"]["key"])
        if (
            s3_record["bucket"]["name"] == BUCKET
            and key.startswith(PREFIX)
            and key.endswith(".csv")
        ):
            keys.append(key)

    if not keys:
        return {"status": "ignored"}

    result = glue.start_job_run(JobName=JOB_NAME, JobRunQueuingEnabled=True)
    execution = {"status": "submitted", "job_run_id": result["JobRunId"], "objects": keys}
    print(execution)
    return execution
