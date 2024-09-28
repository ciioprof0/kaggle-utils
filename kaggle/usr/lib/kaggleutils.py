"""
Please upvote and comment if you find this Kaggle utility script useful.

kagutils - A collection of utility functions for Kaggle notebooks.

This module provides essential utility functions to enhance workflows within Kaggle notebooks. 
The goal is to streamline common operations such as inventorying available files in the Kaggle 
environment and loading input files into Python variables. 

Key Features:
    - Inventorying Files: The `inventory_files` function lists directories and files present 
      in the Kaggle environment, including data files and utility scripts.
    - Loading Inputs: The `load_inputs` function automatically loads CSV and JSON files into 
      Python variables (DataFrames for CSVs, dictionaries for JSONs), allowing for quick access 
      and manipulation within the notebook.

Usage:
    1. Import the module after adding it to your Kaggle notebook:
    
       `from kagutils import inventory_files, load_inputs`

    2. Inventory available files in the Kaggle notebook:
    
       `inventory_files(parent_dir='/kaggle', max_files=5, max_depth=2)`
    
    3. Load input files into Python variables:
    
       `load_inputs(scope=globals())`
    
This module is designed to be simple, intuitive, and compatible with Kaggle's environment, 
making it easier to manage files and focus on analysis.

Author: ciioprf0
"""


import os
import pandas as pd
import json
import sqlite3
import zipfile
from typing import Any, Optional

def inventory_files(parent_dir: str = '/kaggle', max_files: int = 5, max_depth: int = 2) -> None:
    """
    Inventory the files and directories starting from the parent directory, 
    printing up to max_files per directory. Optionally restrict the depth 
    of the directory tree traversal with max_depth.

    This function inventories directories and files, specifically identifying utility scripts 
    in subdirectories under '/kaggle/usr/lib/'. Utility scripts are listed as subdirectory names, 
    while other files are listed as "Filename".

    Args:
        parent_dir (str): The base directory to start the inventory from. Defaults to '/kaggle'.
        max_files (int): The maximum number of files to display per directory. Defaults to 5.
        max_depth (int): The maximum depth to walk into the directory structure. Defaults to 2.

    Example Usage:
        inventory_files(parent_dir='/kaggle', max_files=5, max_depth=3)

    The function will output the structure of directories and files within the specified depth, 
    differentiating between utility scripts in '/kaggle/usr/lib/' and regular files in other directories.
    """
    
    print(f"\n# Inventorying directory: {parent_dir}")
    parent_dir_depth = parent_dir.rstrip(os.sep).count(os.sep)

    # Walk the directory tree starting from the parent directory
    for root, dirs, files in os.walk(parent_dir):
        current_depth = root.count(os.sep) - parent_dir_depth

        if max_depth is not None and current_depth > max_depth:
            continue

        # Handle /kaggle/usr/lib/ directories as utility scripts
        if root == '/kaggle/usr/lib':
            print(f"Directory: {root}")
            for subdir in dirs[:max_files]:
                print(f"  Local Library: {subdir}")
        else:
            print(f"Directory: {root}")
            for filename in files[:max_files]:
                print(f"  Filename: {os.path.join(root, filename)}")


def load_inputs(input_dir: str = '/kaggle/input', scope: Optional[dict] = None) -> None:
    """
    Walk through the input directory and load files into separate variables in the provided scope.
    Appends '_df' to DataFrame variables for CSV files and '_dict' to dictionary variables for JSON files.

    Args:
        input_dir (str): The directory where the input files are located. Defaults to '/kaggle/input'.
        scope (Optional[dict]): The dictionary representing the calling scope (e.g., globals() or locals()).

    Example Usage:
        load_inputs(scope=globals())
    """
    
    if scope is None:
        scope = globals()  # Default to the global scope

import os
import pandas as pd
import json
import sqlite3
import zipfile
from typing import Any, Callable, Optional


def load_csv(filepath: str) -> pd.DataFrame:
    """Load a CSV file into a pandas DataFrame."""
    return pd.read_csv(filepath)


def load_json(filepath: str) -> dict:
    """Load a JSON file into a dictionary."""
    with open(filepath, 'r') as file:
        return json.load(file)


def load_sqlite(filepath: str) -> sqlite3.Connection:
    """Load an SQLite database file into a sqlite3 connection."""
    return sqlite3.connect(filepath)


def load_zip(filepath: str, extract_dir: Optional[str] = None) -> str:
    """
    Extract a ZIP file to a given directory or the current directory and return the extraction directory.
    
    :param filepath: Path to the ZIP file.
    :param extract_dir: Directory to extract the contents. If None, extracts to the current directory.
    :return: The directory where the contents are extracted.
    """
    if extract_dir is None:
        extract_dir = os.path.splitext(filepath)[0]  # Extract to a folder named after the ZIP file
    
    with zipfile.ZipFile(filepath, 'r') as zip_ref:
        zip_ref.extractall(extract_dir)
    
    return extract_dir


# Dictionary that maps file extensions to their corresponding loader functions
file_loaders: dict[str, Callable[[str], Any]] = {
    '.csv': load_csv,
    '.json': load_json,
    '.sqlite': load_sqlite,
    '.zip': load_zip,
    # Add more loaders here for other file types
}


def load_file(filepath: str) -> Any:
    """
    Dynamically load a file based on its extension using a registered loader function.
    If the file type is not supported, returns None.
    
    :param filepath: Path to the file to load.
    :return: The loaded data, or None if the file type is unsupported.
    """
    ext = os.path.splitext(filepath)[1]  # Get file extension
    loader = file_loaders.get(ext)
    
    if loader:
        return loader(filepath)
    else:
        print(f"Unsupported file type: {ext}")
        return None


def load_inputs(input_dir: str = '/kaggle/input', scope: dict = None) -> None:
    """
    Walk through the input directory and load files into separate variables in the provided scope.
    Appends '_df' to DataFrame variables for CSV files and '_dict' to dictionary variables for JSON files.
    If a ZIP archive is encountered, it is extracted, and the files inside are processed recursively.
    
    :param input_dir: The base directory where input files are located.
    :param scope: The dictionary representing the calling scope (e.g., globals() or locals()).
    """
    if scope is None:
        scope = globals()  # Default to the global scope

    for dirname, _, filenames in os.walk(input_dir):
        for filename in filenames:
            filepath = os.path.join(dirname, filename)
            data = None
            
            # Handle ZIP files by extracting and processing their contents recursively
            if filename.endswith('.zip'):
                extract_dir = load_zip(filepath)  # Extract the ZIP file
                load_input_data(extract_dir, scope)  # Recursively process extracted files
                
            else:
                data = load_file(filepath)  # Process regular files (CSV, JSON, etc.)
            
            if data is not None:
                # Create a variable name from the file name (without extension)
                file_key = os.path.splitext(filename)[0]
                
                # If the file is a DataFrame, append '_df' to the variable name
                if isinstance(data, pd.DataFrame):
                    file_key += '_df'
                
                # If the file is a Dictionary (JSON), append '_dict' to the variable name
                elif isinstance(data, dict):
                    file_key += '_dict'
                
                # Inject the variable into the provided scope (global or local)
                scope[file_key] = data
                print(f"Loaded '{file_key}' as a {type(data).__name__} from {filename}")