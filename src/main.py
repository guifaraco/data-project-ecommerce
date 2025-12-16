import logging
import sys
from load import load_to_postgres, create_schema_if_not_exists
from db.db import get_db_engine
from dotenv import load_dotenv
load_dotenv()
from extract import extract_data_from_kaggle


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    try:
        engine = get_db_engine()
        schema = "raw_olist"
        create_schema_if_not_exists(engine, schema)
        
        # Extract
        dfs = extract_data_from_kaggle("olistbr/brazilian-ecommerce", "~/code/projects/data-project-ecommerce/data")
        
        #Load
        for table_name, df in dfs.items():
            load_to_postgres(df, table_name, engine, schema=schema)
        
    except Exception as e:
        logger.error(f"Error: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()