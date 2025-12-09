import pandas as pd
import os
import kaggle

def extract_data_from_kaggle(dataset_slug: str, save_path: str) -> dict[str, pd.DataFrame]:
    """
    Downloads a dataset from Kaggle if not locally present, and loads CSV files into a dictionary.

    This function follows an idempotent 'get-or-create' pattern:
    1. If the `save_path` directory does not exist or is empty, it downloads and extracts the dataset.
    2. If the directory already exists and contains files, it skips the download to reuse existing data.

    Finally, it converts the CSV files in the directory into pandas DataFrames.

    Args:
        dataset_slug (str): The unique identifier for the Kaggle dataset 
            (e.g., 'olistbr/brazilian-ecommerce').
        save_path (str): The local file system path where the data is stored. 
            Supports user directory expansion (e.g., '~/projects/data').

    Returns:
        dict[str, pd.DataFrame]: A dictionary where keys are the table names (e.g., 'orders')
            and values are the corresponding pandas DataFrames.
    """
    
    expanded_path = os.path.expanduser(save_path)
    abs_save_path = os.path.abspath(expanded_path)
    
    if not os.path.exists(abs_save_path) or not os.listdir(abs_save_path):
        os.makedirs(abs_save_path, exist_ok=True)

        kaggle.api.dataset_download_files(
        dataset_slug,
        path=abs_save_path,
        unzip=True
        )
    
    return make_dataframe_from_csv(abs_save_path)


def make_dataframe_from_csv(abs_save_path: str) -> dict[str, pd.DataFrame]:
    """
    Reads all CSV files from a directory and converts them into a dictionary of DataFrames.

    Iterates through the specified directory, identifies files with the '.csv' 
    extension, and loads them into memory. The resulting dictionary allows lookup 
    by filename.

    Args:
        abs_save_path (str): The absolute path to the directory containing the CSV files.

    Returns:
        dict[str, pd.DataFrame]: A dictionary where keys are the table names (e.g., 'orders')
            and values are the corresponding pandas DataFrames. Returns an empty dict 
            if no CSVs are found.
    """
    dfs_dict = {}
    for file_name in os.listdir(abs_save_path):
        if file_name.endswith(".csv"):
            file_path = os.path.join(abs_save_path, file_name)
            dfs_dict[clean_table_name(file_name)] = pd.read_csv(file_path)
    
    return dfs_dict

def clean_table_name(file_name: str) -> str:
    """
    Sanitizes a filename to create a clean database table name.

    Removes specific substrings ('olist_', '_dataset', '.csv') to simplify 
    the name.

    Args:
        file_name (str): The original filename (e.g., 'olist_orders_dataset.csv').

    Returns:
        str: The cleaned table name (e.g., 'orders').
    """
    name = file_name.replace(".csv", "")
    name = name.replace("olist_", "")
    name = name.replace("_dataset", "")
    return name