import pandas as pd
import logging
from google.cloud import bigquery
from google.api_core.exceptions import NotFound

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_to_bigquery(df: pd.DataFrame, table_name: str, client: bigquery.Client, dataset_id: str) -> None:
    """
    Loads a pandas DataFrame into a Google BigQuery table using Parquet format.

    This function performs a full replace (WRITE_TRUNCATE): if the table already 
    exists in the specified dataset, it will be overwritten. The DataFrame index 
    is discarded during the load. Schema is autodetected.

    Args:
        df (pd.DataFrame): The data payload to upload.
        table_name (str): The name of the target table in BigQuery.
        client (bigquery.Client): A valid BigQuery Client object.
        dataset_id (str): The dataset ID (equivalent to schema) to write to.

    Raises:
        Exception: Re-raises any API error after logging it.
    """
    table_id = f"{client.project}.{dataset_id}.{table_name}"

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
    )

    try:
        job = client.load_table_from_dataframe(
            df, table_id, job_config=job_config
        )
        job.result()
        
        table = client.get_table(table_id)
        logger.info(f"Table '{table_id}' successfully loaded! ({table.num_rows} rows)")
        
    except Exception as e:
        logger.error(f"Error loading the table {table_name}: {e}")
        raise e

def create_dataset_if_not_exists(client: bigquery.Client, dataset_id: str) -> None:
    """
    Checks for the existence of a BigQuery Dataset and creates it if missing.

    Args:
        client (bigquery.Client): A valid BigQuery Client object.
        dataset_id (str): The name of the dataset to create (e.g., 'raw_olist').

    Raises:
        Exception: Re-raises any API error after logging it.
    """
    dataset_ref = f"{client.project}.{dataset_id}"
    
    try:
        client.get_dataset(dataset_ref)
        logger.info(f"Dataset '{dataset_id}' found.")
    except NotFound:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"
        client.create_dataset(dataset)
        logger.info(f"Dataset '{dataset_id}' created successfully.")
    except Exception as e:
        logger.error(f"Error creating dataset: {e}")
        raise e