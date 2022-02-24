"""
Ingest 1.0 & 2.0 simple load testing script

information on load testing:
https://groundspeed.atlassian.net/wiki/spaces/ENG/pages/2328494097/Ingest+Orchestration+Load+Testing

As the document suggests, create a folder with the folloing files from the doc above:
- pdf_lite_LT.zip, pdf_heavy_LT.zip, broad_LT.zip **(NAMES MUST REMAIN THE SAME)**

Arguments:
- version: 1 or 2 (old/new ingest)
- iterations: # of iterations
- account_timeout: account timeout in seconds (the timeout in-between each account ingestion)
- ingest_timeout: ingest timeout between each complete iteration in seconds (the timeout between each iteration)
- intake_account_path: path to folder that has the files required in the load testing doc

Requirements:
- folder containing required files listed in doc
- You must be on VPN
- pip install argparse
"""

# Import the library
import argparse
import random
import os
import boto3
import time
import glob

from uuid import uuid4

from database_model.db import session_scope
from database_model.schemas import AgencyConfiguration
from pathlib import Path


s3_client = boto3.client("s3")
cwd = os.getcwd()

parser = argparse.ArgumentParser()
parser.add_argument("--account_timeout", help="Timeout between each account ingested", type=int, required=True)
parser.add_argument("--ingest_timeout", help="Timeout between each ingestion iteration", type=int, required=True)
parser.add_argument("--version", help="Version of load test: 1 or 2 (old vs new)", type=int, required=True)
parser.add_argument("--iterations", type=int, required=False, default=1)
parser.add_argument("--intake_account_path", help="Path to folder that has the files required in the load testing doc", type=str,
                    required=False, default=f"{cwd}/intake_accounts")

args = parser.parse_args()

env = input("Please enter your environment (dev or prod): ")

STAGE = env.lower()
S3_BUCKET = f"groundspeed-sftp-{STAGE}-storage"
VERSION = args.version
ITERATIONS = args.iterations
ACCOUNT_TIMEOUT = args.account_timeout
INGEST_TIMEOUT = args.ingest_timeout
INTAKE_ACCOUNT_PATH = args.intake_account_path

legacy_configuration_id = 851
new_configuration_id = 190


def get_intake_configuration(session, ingest_version=2):
    """
    Get's the agency configuration based on the ingest_version
    """

    if ingest_version == 2:
        configuration_id = new_configuration_id
    else:
        configuration_id = legacy_configuration_id

    try:
        agency_configuration = session.query(AgencyConfiguration).\
            filter(AgencyConfiguration.id == configuration_id).\
            one()

        return agency_configuration
    except Exception as e:
        print(
            f"Error fetching the agency_configuration for id {configuration_id}")


def generate_account_collection():
    """
    Create an ingest account collection to randomly begin ingesting accounts
    Each collection should contain the following in a random order:
    - 7 lite zips
    - 2 heavy zips
    - 1 broad zip
    """
    total_lite = 7
    total_heavy = 2
    total_broad = 1
    list_of_files = glob.glob(f"{INTAKE_ACCOUNT_PATH}/*")

    ingest_account_collection = []

    # randomly look in file list until each total amount above is in the collection.
    while (total_lite + total_heavy + total_broad > 0):
        file = random.choice(list_of_files)
        unique_id = "".join(str(uuid4()).split("-"))

        append = False

        # Track if account should be added to collection
        if "lite" in file and total_lite > 0:
            type = "lite"
            total_lite -= 1
            append = True
            if total_lite == 0:
                # remove file when complete
                list_of_files.remove(file)
        if "heavy" in file and total_heavy > 0:
            type = "heavy"
            total_heavy -= 1
            append = True
            if total_heavy == 0:
                # remove file when complete
                list_of_files.remove(file)
        if "broad" in file and total_broad > 0:
            type = "broad"
            total_broad -= 1
            append = True
            if total_broad == 0:
                # remove file when complete
                list_of_files.remove(file)

        if append:
            ingest_account_collection.append({
                "file_path": file,
                "account_name": f"load_test_{type}_{unique_id}.zip"
            })

    return ingest_account_collection


def upload_to_s3(account, intake_storage_path):
    # Method 2: Client.put_object()
    account_file_path = account["file_path"]
    account_name = account["account_name"]
    response = s3_client.put_object(Body=open(f"{account_file_path}", "rb"),
                                    Bucket=S3_BUCKET,
                                    Key=f"{intake_storage_path}/{account_name}")
    return response


def run():
    # Quick prod sanity check
    print(f"-- Starting load test on version {VERSION} in stage {STAGE}--")
    start_time = time.time()

    if STAGE == "prod":
        answer = input(
            "Your selected environment is prod, are you sure you want to do this? (y or n): ").lower()
        if answer == "n":
            return

    if STAGE not in ["prod", "dev"]:
        print ("Please select valid env: dev/prod")
        return

    with session_scope() as session:
        # Get the agency configuration we're using
        agency_configuration: AgencyConfiguration = get_intake_configuration(
            session, VERSION)

        # Track iterations
        complete_iterations = 0

        # Information about each iteration for logging purposes
        accounts = 0
        pages = 0
        excel_docs = 0
        other_docs = 0
        email_files = 0

        # Loop through the # of iterations
        for _ in range(ITERATIONS):
            account_collection = generate_account_collection()

            for account in account_collection:
                try:
                    upload_to_s3(
                        account, agency_configuration.intake_storage_path)
                except Exception as e:
                    print(
                        f"Encountered exception uploading account to s3 {account}")
                time.sleep(ACCOUNT_TIMEOUT)

            # Each iteration contains the following:
            accounts += 10
            pages += 330
            excel_docs += 5
            other_docs += 5
            email_files += 1

            complete_iterations += 1
            print(f"Iterations complete: {complete_iterations}")
            if ITERATIONS > 1:
                time.sleep(INGEST_TIMEOUT)

        end_time = time.time()

        time_elapsed = (end_time - start_time)

        print(f"""
        ** Ingest Load Test Script Complete **
        accounts: {accounts}
        total pages: {pages}
        excel docs: {excel_docs}
        other docs: {other_docs}
        email files: {email_files}

        total time: {time_elapsed} seconds
        """)


if __name__ == "__main__":
    # Do the thang.
    run()
