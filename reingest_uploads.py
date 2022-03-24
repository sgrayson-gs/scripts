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
- env: environment for reingestion. DEFAULT = DEV

Requirements:
- A file containing uploads exported as csv
- You must be on VPN
- pip install argparse
"""

# Import the library
import argparse
import os
import boto3
import time
import copy
import csv

from database_model.db import session_scope
from database_model.schemas import AgencyConfiguration


s3_client = boto3.client("s3")
cwd = os.getcwd()

parser = argparse.ArgumentParser()
parser.add_argument("--uploads_path",
                    help="Path to uploads csv file exported from postgres",
                    type=str,
                    required=False)

args = parser.parse_args()

STAGE = os.environ['STAGE']
UPLOAD_PATH = args.uploads_path

# Maintain list of agency configurations and their information for uploads
AGENCY_CONFIGURATION_MAPPING = {}


def remove_orchestration_path(configuration_id, session):
    """
    Takes in a configuration_id, store it's value in agency_config_mapping, set orchestration_path to None
    """

    if not configuration_id:
        return None

    try:
        # Get Agency Config
        result: AgencyConfiguration = session.query(AgencyConfiguration).\
            filter(AgencyConfiguration.id == configuration_id).\
            one()
        # Copy the agency configuration since orchestration path will change
        AGENCY_CONFIGURATION_MAPPING[configuration_id] = copy.deepcopy(result)
        # Update Agency Configuration
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

        for upload in uploads_list:
            intake_method = upload["intake_method"]
            if intake_method in ["sftp", "email", "batch", "api_batch"]:
                # Move the file to sftp bucket from orchestration
                print("email")
            elif intake_method in ["api"]:
                # Use orchestration path to send sqs message & hashed api
                print("api")
            else:
                print(f"intake_method not supported by script: {intake_method}")
            print(upload["id"])

        end_time = time.time()

        time_elapsed = (end_time - start_time)

        print(f"""
        ** Reingest Load Test Script Complete **

        total time: {time_elapsed} seconds
        """)


if __name__ == "__main__":
    # Do the thang.
    run()
