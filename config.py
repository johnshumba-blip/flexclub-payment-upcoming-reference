import os
import json
from google.oauth2 import service_account
# Retrieve credentials from environment variables
storage_creds_json = os.getenv('BIGQUERY_CREDS_2026')




env_helpful_error = """
Please set the BIGQUERY_CREDS and DOCAI_CREDS environment variables.
1. Acquire the JSON keyfiles for the service accounts needed
2. Add these JSONs as text to secret manager with an appropriate secret name
3. You may need to make sure that these secrets have the appropriate access permissions for the compute service account
4. Attach these secrets to Cloud Run in the Github Actions workflows like so: --set-secrets=BIGQUERY_CREDS=BIGQUERY_CREDS:1
4. cont. Note that the name before the = is going to be the name available in the python env. The name after the = is the secret name.
"""
# Ensure that the environment variables are set
if storage_creds_json is None :
    raise ValueError(env_helpful_error)
# Load credentials from environment variables
storage_creds_dict = json.loads(storage_creds_json)

credentials_bigquery = service_account.Credentials.from_service_account_info(
    storage_creds_dict, scopes=["https://www.googleapis.com/auth/cloud-platform"],
)