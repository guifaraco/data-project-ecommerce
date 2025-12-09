import pandas as pd
import logging
from sqlalchemy import Engine, text

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_to_postgres(df: pd.DataFrame, table_name: str, engine: Engine, schema: str = 'public') -> None:
    """
    Loads a pandas DataFrame into a PostgreSQL database table.

    This function performs a full replace: if the table already exists in the 
    specified schema, it will be dropped and recreated. The DataFrame index 
    is discarded during the load.

    Args:
        df (pd.DataFrame): The data payload to upload.
        table_name (str): The name of the target table in the database.
        engine (Engine): A valid SQLAlchemy Engine object connected to the database.
        schema (str, optional): The database schema to write to. Defaults to 'public'.

    Raises:
        Exception: Re-raises any database or connection error after logging it.
    """
    try:
        df.to_sql(name=table_name, con=engine, schema=schema, if_exists='replace', index=False)
        logger.info(f"Table '{schema}.{table_name}' successfully loaded! ({len(df)} rows)")
    except Exception as e:
        logger.error(f"Error loading the table {table_name}: {e}")
        raise e

def create_schema_if_not_exists(engine: Engine, schema_name: str) -> None:
    """
    Checks for the existence of a database schema and creates it if missing.

    Uses a raw SQL execution to run a 'CREATE SCHEMA IF NOT EXISTS' command. 
    This operation is transactional and commits immediately upon success.

    Args:
        engine (Engine): A valid SQLAlchemy Engine object connected to the database.
        schema_name (str): The name of the schema to create.

    Raises:
        Exception: Re-raises any database execution error after logging it.
    """
    try:
        with engine.connect() as connection:
            query = text(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
            connection.execute(query)
            connection.commit()
            logger.info(f"Schema '{schema_name}' found/created successfully.")
    except Exception as e:
        logger.error(f"Error creating a schema: {e}")
        raise e