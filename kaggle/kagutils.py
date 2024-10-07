"""
Please upvote and comment on Kaggle.com if you find this utility script useful.
On Kaggle at https://www.kaggle.com/code/mikeb3665/kagutils

kagutils - A collection of utility functions for Kaggle notebooks.

This module provides essential utility functions to enhance workflows within Kaggle notebooks. 
The goal is to streamline common operations such as inventorying available files in the Kaggle 
environment, loading input files into Python variables, managing file operations, and handling ZIP 
archives. 

Key Features:
    - Inventorying Files: The `inventory_files` function lists directories and files present 
      in the Kaggle environment, including data files and utility scripts.
    - Loading Inputs: The `load_inputs` function automatically loads CSV and JSON files into 
      Python variables (DataFrames for CSVs, dictionaries for JSONs), allowing for quick access 
      and manipulation within the notebook.
    - Checking Missing Files: The `check_missing_files` function ensures that all required files 
      are available in the Kaggle environment and provides feedback if any files are missing.
    - Creating Directory Structures: The `create_directories` function allows users to create 
      custom directory structures for organizing files, models, or outputs.
    - Moving and Copying Files: The `move_or_copy_files` function moves or copies files between 
      directories, making it easier to manage data or model outputs.
    - Compressing and Extracting ZIP Files: The `zip_files` function compresses multiple files into 
      a ZIP archive, while `unzip_file` extracts ZIP archives into a specified directory.

Usage:
    1. Import the module after adding it to your Kaggle notebook:
    
       `from kagutils import inventory_files, load_inputs, check_missing_files, 
       create_directories, move_or_copy_files, zip_files, unzip_file`
    
    2. Inventory available files in the Kaggle notebook:
    
       `inventory_files(parent_dir='/kaggle', max_files=5, max_depth=2, quiet=False)`
    
    3. Load input files into Python variables:
    
       `load_inputs(scope=globals(), quiet=False)`
    
    4. Check for missing required files:
    
       `check_missing_files(required_files=['train.csv', 'test.csv'], quiet=False)`
    
    5. Create custom directory structures:
    
       `create_directories({'/kaggle/working': ['models', 'output']}, quiet=False)`
    
    6. Move or copy files between directories:
    
       `move_or_copy_files(['file1.csv', 'file2.csv'], '/kaggle/input', '/kaggle/working', action='move', quiet=False)`
    
    7. Compress files into a ZIP archive:
    
       `zip_files(['file1.csv', 'file2.csv'], 'my_archive.zip', '/kaggle/working', quiet=False)`
    
    8. Extract a ZIP archive into a directory:
    
       `unzip_file('my_archive.zip', '/kaggle/working', quiet=False)`
    
This module is designed to be simple, intuitive, and compatible with Kaggle's environment, 
making it easier to manage files, directories, and data operations, so you can focus on analysis.

Author: ciioprf0
"""


import os 
import json
import pandas as pd
import shutil
import sqlite3
import zipfile
from typing import Any, Optional, Callable


def inventory_files(parent_dir: str = '/kaggle', max_files: int = 5, max_depth: int = 2, quiet: bool = False) -> None:
    """
    Inventory the files and directories starting from the parent directory, 
    printing up to max_files per directory. Optionally restrict the depth 
    of the directory tree traversal with max_depth.

    Args:
        parent_dir (str): The base directory to start the inventory from. Defaults to '/kaggle'.
        max_files (int): The maximum number of files to display per directory. Defaults to 5.
        max_depth (int): The maximum depth to walk into the directory structure. Defaults to 2.
        quiet (bool): If True, suppresses output. Defaults to False.
    """
    
    if not quiet:
        print(f"\n# Inventorying directory: {parent_dir}")
    
    parent_dir_depth = parent_dir.rstrip(os.sep).count(os.sep)

    # Walk the directory tree starting from the parent directory
    for root, dirs, files in os.walk(parent_dir):
        current_depth = root.count(os.sep) - parent_dir_depth

        if max_depth is not None and current_depth > max_depth:
            continue

        # Handle /kaggle/usr/lib/ directories as utility scripts
        if root == '/kaggle/usr/lib':
            if not quiet:
                print(f"Directory: {root}")
            for subdir in dirs[:max_files]:
                if not quiet:
                    print(f"  Local Library: {subdir}")
        else:
            if not quiet:
                print(f"Directory: {root}")
            for filename in files[:max_files]:
                if not quiet:
                    print(f"  Filename: {os.path.join(root, filename)}")


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


def load_inputs(input_dir: str = '/kaggle/input', scope: dict = None, quiet: bool = False) -> None:
    """
    Walk through the input directory and load files into separate variables in the provided scope.
    Appends '_df' to DataFrame variables for CSV files and '_dict' to dictionary variables for JSON files.
    If a ZIP archive is encountered, it is extracted, and the files inside are processed recursively.
    
    Provides feedback if no input files are found and reminds the user to add inputs via 'File' -> 'Add inputs'.
    
    :param input_dir: The base directory where input files are located.
    :param scope: The dictionary representing the calling scope (e.g., globals() or locals()).
    :param quiet: If True, suppresses output. Defaults to False.
    """
    if scope is None:
        scope = globals()  # Default to the global scope

    found_files = False  # Track if any files are found
    
    # Walk through the input directory
    for dirname, _, filenames in os.walk(input_dir):
        if filenames:
            found_files = True  # Files found, update the flag
        
        for filename in filenames:
            filepath = os.path.join(dirname, filename)
            data = None
            
            # Handle ZIP files by extracting and processing their contents recursively
            if filename.endswith('.zip'):
                extract_dir = load_zip(filepath)  # Extract the ZIP file
                load_inputs(extract_dir, scope, quiet=quiet)  # Recursively process extracted files
                
            else:
                data = load_file(filepath)  # Process regular files (CSV, JSON, etc.)
            
            if data is not None:
                # Create a variable name from the file name (without extension)
                file_key = os.path.splitext(filename)[0]
                
                # If the file is a DataFrame, prepend 'df_' to the variable name
                if isinstance(data, pd.DataFrame):
                    file_key = 'df_' + file_key 
                
                # If the file is a Dictionary (JSON), append '_dict' to the variable name
                elif isinstance(data, dict):
                    file_key += '_dict'
                
                # Inject the variable into the provided scope (global or local)
                scope[file_key] = data
                if not quiet:
                    print(f"Loaded '{file_key}' as a {type(data).__name__} from {filename}")

    # If no files were found, prompt the user
    if not found_files and not quiet:
        print("No input files found in the directory.")
        print("Did you forget to add inputs to this notebook?")
        print("To add inputs, go to the notebook menu bar and select 'File' -> 'Add inputs'.")
        
        
def check_missing_files(required_files: list[str], input_dir: str = '/kaggle/input', quiet: bool = False) -> list[str]:
    """
    Check if required files are present in the input directory.
    
    Args:
        required_files (list[str]): A list of required filenames (without paths).
        input_dir (str): The base directory to check for files. Defaults to '/kaggle/input'.
        quiet (bool): If True, suppresses output. Defaults to False.
    
    Returns:
        list[str]: A list of missing files.
    """
    found_files = []
    for dirname, _, filenames in os.walk(input_dir):
        found_files.extend(filenames)

    missing_files = [file for file in required_files if file not in found_files]
    
    if missing_files and not quiet:
        print(f"Missing files: {missing_files}")
    elif not missing_files and not quiet:
        print("All required files are present.")
    
    return missing_files

import shutil


def create_directories(dir_structure: dict[str, list[str]], quiet: bool = False) -> None:
    """
    Create a directory structure based on the provided dictionary.
    
    Args:
        dir_structure (dict): A dictionary where keys are parent directories and values are lists of subdirectories.
        quiet (bool): If True, suppresses output. Defaults to False.
    
    Example:
        dir_structure = {
            '/kaggle/working': ['models', 'output'],
            '/kaggle/input': ['data', 'temp']
        }
    """
    for parent_dir, sub_dirs in dir_structure.items():
        for sub_dir in sub_dirs:
            dir_path = os.path.join(parent_dir, sub_dir)
            if not os.path.exists(dir_path):
                os.makedirs(dir_path)
                if not quiet:
                    print(f"Created directory: {dir_path}")
            elif not quiet:
                print(f"Directory already exists: {dir_path}")



def move_or_copy_files(files: list[str], src_dir: str, dest_dir: str, action: str = 'move', quiet: bool = False) -> None:
    """
    Move or copy a list of files from the source directory to the destination directory.
    
    Args:
        files (list[str]): List of filenames to move or copy.
        src_dir (str): Source directory where the files are located.
        dest_dir (str): Destination directory to move or copy the files to.
        action (str): Either 'move' or 'copy'. Defaults to 'move'.
        quiet (bool): If True, suppresses output. Defaults to False.
    """
    if not os.path.exists(dest_dir):
        os.makedirs(dest_dir)
    
    for file in files:
        src_file = os.path.join(src_dir, file)
        dest_file = os.path.join(dest_dir, file)
        
        if os.path.exists(src_file):
            if action == 'move':
                shutil.move(src_file, dest_file)
                if not quiet:
                    print(f"Moved {file} to {dest_dir}")
            elif action == 'copy':
                shutil.copy(src_file, dest_file)
                if not quiet:
                    print(f"Copied {file} to {dest_dir}")
        else:
            if not quiet:
                print(f"File not found: {src_file}")
                
                
def zip_files(files: list[str], zip_name: str, src_dir: str = '/kaggle/working', quiet: bool = False) -> None:
    """
    Compress a list of files into a ZIP archive.
    
    Args:
        files (list[str]): List of filenames to zip.
        zip_name (str): The name of the output ZIP file.
        src_dir (str): Source directory where the files are located. Defaults to '/kaggle/working'.
        quiet (bool): If True, suppresses output. Defaults to False.
    """
    zip_path = os.path.join(src_dir, zip_name)
    with zipfile.ZipFile(zip_path, 'w') as zipf:
        for file in files:
            file_path = os.path.join(src_dir, file)
            if os.path.exists(file_path):
                zipf.write(file_path, file)
                if not quiet:
                    print(f"Added {file} to {zip_name}")
            elif not quiet:
                print(f"File not found: {file_path}")

def unzip_file(zip_file: str, extract_dir: str = '/kaggle/working', quiet: bool = False) -> None:
    """
    Extract a ZIP file into the specified directory.
    
    Args:
        zip_file (str): The ZIP file to extract.
        extract_dir (str): The directory to extract the contents. Defaults to '/kaggle/working'.
        quiet (bool): If True, suppresses output. Defaults to False.
    """
    with zipfile.ZipFile(zip_file, 'r') as zip_ref:
        zip_ref.extractall(extract_dir)
        if not quiet:
            print(f"Extracted {zip_file} to {extract_dir}")
