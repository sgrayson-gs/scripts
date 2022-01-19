"""
Simple script to to update the intake_storage path in postgres using the screts value.

Prior to running the script set the following variables:
- STAGE
- INTERNAL_API_ENDPOINT
"""

from gs_common.config.cryptography_manager import CryptoManager
from database.operations.agency_config import get_agency_configuration_by_api_key

# from typing import Optional

import os
import json
import boto3
import requests

client = boto3.client('secretsmanager')

STAGE = os.getenv("STAGE", "dev")
INTERNAL_API_ENDPOINT = os.getenv("INTERNAL_API_ENDPOINT", None)

def update_intake_storage_path(path, api_key):
    """
    Takes in a hashed api & folder path. Stores the path in the agency_configuration table in the intake_storage path
    """

    if not path or not api_key:
        return None
    else:
        hashed_api_key = (
            CryptoManager("intake-processor", STAGE)
            .hash("api_key_hash_salt", api_key)
            .decode("utf-8")
        )

        updated_path = path.replace(".", "/")

        print(hashed_api_key, updated_path)

        # Need a way to login to dev db...
        agency_configuration = get_agency_configuration_by_api_key(hashed_api_key)
        
        update_json = {
            "agencyConfigurationId": "1",
            "data": {"intakeStoragePath": updated_path}
        }


        # response = requests.patch(
        #     url=f"{INTERNAL_API_ENDPOINT}/api/v2/upload", json=update_json,
        # )
        # if response.status_code == 200:
        #     print(f"Error updating the following path wish hash: {hashed_api_key}")

        return True

def run():
    secret_response = client.get_secret_value(
        SecretId = f"{STAGE}-intake-sftp-api-key-lookup" 
    )

    if secret_response["ResponseMetadata"]["HTTPStatusCode"] == 200:
        secret_json = json.loads(secret_response["SecretString"])
        for path, api_key in secret_json.items():
            update_intake_storage_path(path, api_key)
    else:
        print("Error fetching aws secret")




if __name__ == "__main__":
    # Do the thang.
    if not INTERNAL_API_ENDPOINT:
        print("Set INTERNAL_API_ENDPOINT environment variable before running script.")
    run()


