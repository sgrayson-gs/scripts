"""
Take a csv of file_ids. Query mongo for needed information and send message to sqs for OCR.
- install argparse if needed & gs libraries
- YOU MUST BE ON VPN
"""
import boto3
import csv
import json
import argparse
import time

sqs = boto3.client("sqs", region_name="us-east-1")

parser = argparse.ArgumentParser()

parser.add_argument(
    "--input", help="Input file path of file_ids", default=None)
parser.add_argument("--env", help="Environment to run script", default="dev")
parser.add_argument("--username", help="Mongo username", default=None)
parser.add_argument("--password", help="Mongo password", default=None)
parser.add_argument("--hostname", help="Hostname of mongo db", default=None)

args = parser.parse_args()

ENVIRONMENT = args.env
INPUT = args.input
USERNAME = args.username
PASSWORD = args.password
HOSTNAME = args.hostname

OCR_URL = f"https://sqs.us-east-1.amazonaws.com/330914985129/word-ensembler-jobs-{ENVIRONMENT}"

import gs_api_sdk  # noqa E402


def python_sdk_login():
    """
    uses credentials in secretsmanager to log in to gs-app
    Returns:
        gs_api_sdk: authenticated instance of the sdk
    """
    print("Logging into gs-app python sdk")
    gs_app_host = HOSTNAME

    gs_api = gs_api_sdk.SDK(host=gs_app_host, service_name="word-ensembler")
    gs_api.login(
        username=USERNAME, password=PASSWORD
    )
    return gs_api


def send_to_ocr_queue(payload):
    response = sqs.send_message(
        QueueUrl=OCR_URL,
        MessageBody=json.dumps(payload)
    )
    return response


if not INPUT or not USERNAME or not PASSWORD or not HOSTNAME:
    print("You're missing required fields, please check the arguments.")
else:
    GS_API_SDK = python_sdk_login()
    chunk = 0
    with open(INPUT, newline="") as csvfile:
        csvread = csv.DictReader(
            csvfile, delimiter=",", fieldnames=['file_id'])
        for row in csvread:
            file_id = row["file_id"]
            response = GS_API_SDK.files.find_by_id(file_id)
            file_data = response["data"]
            ocr_payload = {
                "file_id": file_id,
                "config": {
                    "s3_key": file_data["s3Key"],
                    "s3_bucket": file_data["s3Bucket"]
                }
            }
            response = send_to_ocr_queue(ocr_payload)
            chunk = chunk + 1
            # Sleep for 5 minutes after 1000 go through the queue
            if (chunk % 1000) == 0:
                print(
                    f"{chunk} files successfuly sent back to OCR - sleeping for 5 minutes for throughput.")
                time.sleep(300)
                # Re-login just in case we need another token
                GS_API_SDK = python_sdk_login()

print("--Complete--")
