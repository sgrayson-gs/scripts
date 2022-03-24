"""
Ingest 1.0 re-ingest script.

This script can be used as a tool to reingest a list of uploads into Ingest 1.0. It does the following:
- Gets a list of agency_configurations in the uploads csv
- Removes their orchestration path
- Reads the intake_storage_path of the agency_config
- Moves the files from orchestration path to intake_storage_path for each upload


information on re-ingesting into 1.0 from 2.0:
https://groundspeed.atlassian.net/wiki/spaces/ENG/pages/2356805653/Ingest+Orchestration+Triage+Run-book#Re-ingestion-Steps-(send-to-1.0)

Arguments:
- uploads_path: path to CSV containing uploads to send to ingest 1.0
- remove_orch_path: include this flag if you want to remove the orchestration path

Requirements:
- Set stage when running script ex: STAGE=dev
- A file containing uploads exported as csv
- You must be on VPN
- pip install argparse
- database_model 
- intake
"""

# Import the library
import argparse
import datetime
import os
from uuid import uuid4
import boto3
import time
import copy
import csv

from intake.celery_tasks import ingest_file  # noqa E402
from database_model.db import session_scope
from database_model.schemas import AgencyConfiguration, Upload, Agency


s3 = boto3.resource("s3")
sqs = boto3.client("sqs")

cwd = os.getcwd()

parser = argparse.ArgumentParser()
parser.add_argument("--uploads_path",
                    help="Path to uploads csv file exported from postgres",
                    type=str,
                    required=False)
parser.add_argument("--remove_orch_path", action="store_true", default=False)

args = parser.parse_args()

STAGE = os.environ["STAGE"]
UPLOAD_PATH = args.uploads_path
REMOVE_ORCH_PATH = args.remove_orch_path
SFTP_BUCKET = f"groundspeed-sftp-{STAGE}-storage"
API_BUCKET = f"groundspeed-external-api-file-intake-{STAGE}"
ORCH_BUCKET = f"groundspeed-ingest-orchestration-{STAGE}"
SFTP_BUCKET_INTAKE_METHODS = ["sftp", "email", "batch", "api_batch"]
API_BUCKET_INTAKE_METHODS = ["api"]

# Maintain list of agency configurations and their information for uploads
AGENCY_CONFIGURATION_MAPPING = {}


def set_upload_to_reingested(upload_id, session):
    try:
        # Update Agency Configuration
        session.query(Upload).\
            filter(Upload.id == upload_id).\
            update({"status_code": "Reingested"})
        session.commit()

    except Exception as e:
        session.rollback()
        print(
            f"Error updating the upload to Reingestd for id {upload_id}, {e}")


def remove_orchestration_path(configuration_id, session):
    """
    Takes in a configuration_id, store it's value in agency_config_mapping, set orchestration_path to None
    """

    if not configuration_id:
        return None

    try:
        # Get Agency Config
        result = session.query(AgencyConfiguration, Agency).\
            join(Agency, AgencyConfiguration.agency_id == Agency.id).\
            filter(AgencyConfiguration.id == configuration_id).\
            one()
        # Copy the agency configuration since orchestration path will change
        result = copy.deepcopy(result)
        agency_config = result[0].__dict__  # 0 index = config
        agency = result[1].__dict__  # 1 index = agency
        agency_config["agency_name"] = agency["name"]
        AGENCY_CONFIGURATION_MAPPING[configuration_id] = copy.deepcopy(agency_config)
        # Update Agency Configuration
        if REMOVE_ORCH_PATH:
            session.query(AgencyConfiguration).\
                filter(AgencyConfiguration.id == configuration_id).\
                update({"orchestration_path": None})
        session.commit()

        return result

    except Exception as e:
        session.rollback()
        print(
            f"Error updating the agency_configuration for id {configuration_id}, {e}")


def get_uploads_list():
    csvfile = open(UPLOAD_PATH, newline='')
    uploads = csv.DictReader(csvfile, delimiter=',')
    # Due to dictreader being a reader class, convert to standard list
    return [upload for upload in uploads]


def copy_to_api(orchestration_path, agency_name, project_id, file_name) -> str:
    # Copy files from orchestration to api bucket, return new key
    copy_source = {
        "Bucket": ORCH_BUCKET,
        "Key": orchestration_path
    }
    day = datetime.datetime.now().strftime("%Y-%m-%d")
    api_path = f"{agency_name}/{project_id}/{day}/{file_name}"
    s3.meta.client.copy(copy_source, API_BUCKET, api_path)
    return api_path


def copy_to_sftp(orchestration_path, intake_storage_path, file_name) -> str:
    # Copy files from orchestration to sftp bucket, returns sftp key
    copy_source = {
        "Bucket": ORCH_BUCKET,
        "Key": orchestration_path
    }
    sftp_path = intake_storage_path + "/" + file_name
    s3.meta.client.copy(copy_source, SFTP_BUCKET, sftp_path)
    return sftp_path


def ingest_account_directly(hashed_api_key, s3_bucket, s3_key, file_name):
    # ingest files directly into the intake celery app. Priority is always highest "0"
    HIGHEST = 0
    account_id = file_name.rsplit(".", 1)[0]
    message = {
        "hashed_api_key": hashed_api_key,
        "intake_stragey": "api",
        "s3_bucket": s3_bucket,
        "s3_key": s3_key,
        "file_name": file_name,
        "trace_id": str(uuid4()),
        "priority": HIGHEST,
        "account_id": account_id
    }
    ingest_file.s(message=message).apply_async(priority=HIGHEST)


def run():
    # Quick prod sanity check
    print(f"-- Starting re-ingestion in stage {STAGE}--")
    start_time = time.time()

    if STAGE == "prod":
        answer = input(
            "Your selected environment is prod, are you sure you want to do this? (y or n): ").lower()
        if answer != "y":
            return

    if STAGE not in ["prod", "dev", "local"]:
        print("Please have valid env: dev/prod/local")
        return

    with session_scope() as session:
        # Get the agency configuration we're using
        print(f"Getting list of uploads at {UPLOAD_PATH}")
        uploads_list = get_uploads_list()

        # Get list of unique agency_configurations from uploads
        agency_configuration_ids = set([upload["agency_configuration_id"] for upload in uploads_list])

        print(f"Setting orchestration path to none for configurations {agency_configuration_ids}")
        for id in agency_configuration_ids:
            # Remove orchestration_path and get agency_info
            remove_orchestration_path(int(id), session)

        migrated_uploads = 0
        for upload in uploads_list:
            agency_config = AGENCY_CONFIGURATION_MAPPING[int(upload["agency_configuration_id"])]
            intake_method = upload["intake_method"]
            if intake_method not in SFTP_BUCKET_INTAKE_METHODS + API_BUCKET_INTAKE_METHODS:
                print(f"intake_method not supported by script: {intake_method}")
                continue
            if intake_method in SFTP_BUCKET_INTAKE_METHODS:
                copy_to_sftp(
                    upload["file_s3_path"],
                    agency_config["intake_storage_path"],
                    upload["original_file_name"])
            elif intake_method in API_BUCKET_INTAKE_METHODS:
                api_path = copy_to_api(
                    orchestration_path=upload["file_s3_path"],
                    agency_name=agency_config["agency_name"],
                    project_id=agency_config["default_project_id"],
                    file_name=upload["original_file_name"]
                )
                ingest_account_directly(
                    agency_config["hashed_api_key"],
                    s3_bucket=API_BUCKET,
                    s3_key=api_path,
                    file_name=upload["original_file_name"]
                )
            migrated_uploads += 1
            set_upload_to_reingested(upload["id"], session)

        end_time = time.time()

        time_elapsed = (end_time - start_time)

        print(f"""
        ** Reingest Script Complete **

        total time: {time_elapsed} seconds
        total migrated uploads: {migrated_uploads}
        """)


if __name__ == "__main__":
    # Do the thang.
    run()
