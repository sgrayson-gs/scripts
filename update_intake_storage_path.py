"""
Simple script to to update the intake_storage path in postgres using the screts value.

Requirements:
- You must be on VPN
- set the STAGE environment, default to dev
"""

from gs_common.config.cryptography_manager import CryptoManager
from database_model.db import session_scope
from database_model.schemas import AgencyConfiguration

import os
import json
import boto3

client = boto3.client("secretsmanager")
STAGE = os.getenv("STAGE", "dev")

def update_intake_storage_path(path, api_key, session):
    """
    Takes in a hashed api & folder path. Stores the path in the agency_configuration table in the intake_storage path
    """

    if not path or not api_key:
        return None

    hashed_api_key = (
        CryptoManager("intake-processor", STAGE)
        .hash("api_key_hash_salt", api_key)
        .decode("utf-8")
    )

    updated_path = path.split(".")[-1]+"/inbound"

    print(hashed_api_key, updated_path)

    try:
        session.query(AgencyConfiguration).\
            filter(AgencyConfiguration.hashed_api_key == hashed_api_key).\
            update({"intake_storage_path": updated_path})
        session.commit()
    except Exception as e:
        session.rollback()
        print(f"Error updating the agency_configuration for hashed_key {hashed_api_key}: {e}")

    return True


def run():
    # Get the mapping of intake paths/api_keys
    secret_response = client.get_secret_value(
        SecretId=f"{STAGE}-intake-sftp-api-key-lookup"
    )

    if secret_response["ResponseMetadata"]["HTTPStatusCode"] == 200:
        secret_json = json.loads(secret_response["SecretString"])
        with session_scope() as session:
            # Loop through each of the keys and update the path in postgres
            for path, api_key in secret_json.items():
                update_intake_storage_path(path, api_key, session)

            print(f"-- Script complete. Records updated {len(secret_json)} --")

    else:
        print("Error fetching aws secret")


if __name__ == "__main__":
    # Do the thang.
    run()