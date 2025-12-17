import logging
import sys
import os
from google.cloud import bigquery
from google.oauth2 import service_account
from dotenv import load_dotenv

load_dotenv()

from src.load import load_to_bigquery, create_dataset_if_not_exists
from src.extract import extract_data_from_kaggle


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
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

def main():
    try:
        logger.info("Initializing BigQuery Client...")
        client = get_bigquery_client()
        dataset_id = "raw_olist"
        
        create_dataset_if_not_exists(client, dataset_id)
        
        logger.info("Starting Extraction from Kaggle...")
        dfs = extract_data_from_kaggle(
            "olistbr/brazilian-ecommerce", 
            "./data" 
        )
        
        logger.info("Starting Load to BigQuery...")
        for table_name, df in dfs.items():
            load_to_bigquery(df, table_name, client, dataset_id=dataset_id)
            
        logger.info("Pipeline finished successfully!")
        
    except Exception as e:
        logger.error(f"Critical Pipeline Error: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()