"""
Take a output csv from mongo_db, run a loop, and push messages to SQS to begin ingestion.
"""
import boto3
from uuid import uuid4
import csv
import json

sqs = boto3.client("sqs", region_name="us-east-1")

PRIORITIZER_URL = "https://sqs.us-east-1.amazonaws.com/330914985129/intake-prioritizer-prod"

SFTP_BUCKET = "groundspeed-sftp-prod-storage"
API_BUCKET = "groundspeed-external-api-file-intake-prod"

with open('output.csv', newline='') as csvfile:
    spamreader = csv.DictReader(csvfile, delimiter=',')
    for row in spamreader:
        # If OCR is done, skip
        if row["OCR_DONE"] == 'TRUE':
            continue
        # SFTP INTAKE
        path_to_file = row["path_to_file"]
        account = row["account"]
        if "Travelers" in row["path_to_file"]:
            message = {
                "api_key": "lE9Tl1szIepJ0S9QG1kxpmiyMMFRx6sH9wzEMF00",
                "intake_strategy": "api",
                "s3_bucket": API_BUCKET,
                "s3_key": path_to_file,
                "file_name": f"{account}.zip",
                "trace_id": str(uuid4()),
                "project_id": "ZDZhD8jJgyWNuJuRD"
            }
            response = sqs.send_message(
                QueueUrl=PRIORITIZER_URL,
                MessageBody=json.dumps(message)
            )
            print(response)