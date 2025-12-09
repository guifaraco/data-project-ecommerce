import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, Engine
import logging

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_db_engine() -> Engine:
    """
    Constructs a SQLAlchemy Engine and verifies the database connection.

    Reads database credentials from environment variables (POSTGRES_USER, 
    POSTGRES_PASSWORD, POSTGRES_PORT, POSTGRES_DB) and defaults the host 
    to 'localhost'. It attempts a test connection before returning the engine.

    Returns:
        Engine: A fully initialized and tested SQLAlchemy Engine object 
            connected to the PostgreSQL database.

    Raises:
        Exception: If the connection test fails or environment variables are invalid.
    """
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT")
    db = os.getenv("POSTGRES_DB")

    url = f"postgresql://{user}:{password}@{host}:{port}/{db}"
    
    try:
        engine = create_engine(url)
        with engine.connect() as conn:
            pass
        return engine
    except Exception as e:
        logger.error(f"Error while connecting to database: {e}")
        raise e
