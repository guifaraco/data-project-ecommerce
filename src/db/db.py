import os
import sys
import logging
from google.cloud import bigquery
from google.oauth2 import service_account
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

CREDENTIALS_PATH = "service_account.json" 

def get_bigquery_client() -> bigquery.Client:
    """
    Creates and returns an authenticated BigQuery Client.
    """
    if not os.path.exists(CREDENTIALS_PATH):
        logger.error(f"Credentials file not found at: {CREDENTIALS_PATH}")
        sys.exit(1)
        
    credentials = service_account.Credentials.from_service_account_file(CREDENTIALS_PATH)
    return bigquery.Client(credentials=credentials, project=credentials.project_id)