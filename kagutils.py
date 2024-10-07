# %% [code]
"""
Please upvote and comment if you find this Kaggle utility script useful.
GitHub: https://github.com/ciioprof0/kagutils
Kaggle: https://www.kaggle.com/code/mikeb3665/kagutils

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
       
    9. Finding kaggle.json: The `find_kaggle_json` function searches for the `kaggle.json` file in the
      default `~/.kaggle/` directory. If it is not found, it can copy it from a custom path, ensuring
      it is placed in the correct location.
      
    10. Checking kaggle.json Permissions: The `check_kaggle_json_permissions` function verifies the
      permissions of the `kaggle.json` file. On Unix-based systems, it sets the permissions to `chmod 600`.
      On Windows, it ensures the file is read-only.
      
    11. Validating kaggle.json: The `validate_kaggle_json` function ensures the `kaggle.json` file contains
      valid JSON data, specifically checking for the presence of the required keys: `username` and `key`.
      
    12. Running kaggle.json Utilities: The `kaggle_json_utils` function combines the operations of finding,
      checking permissions, and validating the `kaggle.json` file in a single call.
    
This module is designed to be simple, intuitive, and compatible with Kaggle's environment, 
making it easier to manage files, directories, and data operations, so you can focus on analysis.

Author: ciioprf0
"""


import os 
import json
import pandas as pd
import platform
import shutil
import sqlite3
import stat
import zipfile
from pathlib import Path
from typing import Any, Optional, Callable

# Constants
KAGGLE_DIR = Path.home() / ".kaggle"
KAGGLE_JSON = KAGGLE_DIR / "kaggle.json"


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
            

def find_kaggle_json(custom_path: str = None) -> bool:
    """
    Find the kaggle.json file in the default ~/.kaggle directory or the specified custom location.
    If it does not exist in ~/.kaggle, move it there from the custom_path.

    Args:
        custom_path (str): Optional custom path to check for kaggle.json.

    Returns:
        bool: True if kaggle.json is found or successfully moved, False otherwise.
    """
    # Check if kaggle.json exists in the default ~/.kaggle location
    if KAGGLE_JSON.exists():
        print(f"kaggle.json found at {KAGGLE_JSON}")
        return True

    # Check if a custom path is provided and kaggle.json exists there
    if custom_path:
        custom_json_path = Path(custom_path)
        if custom_json_path.exists() and custom_json_path.name == "kaggle.json":
            # Move kaggle.json to the default ~/.kaggle location
            KAGGLE_DIR.mkdir(exist_ok=True)
            shutil.copy(custom_json_path, KAGGLE_JSON)
            print(f"Copied kaggle.json to {KAGGLE_JSON}")
            return True
        else:
            print(f"kaggle.json not found in the custom path: {custom_json_path}")

    print("kaggle.json not found")
    return False


def check_kaggle_json_permissions() -> bool:
    """
    Check if kaggle.json has the correct permissions (chmod 600 on Linux/Mac, read-only on Windows).
    
    Returns:
        bool: True if permissions are correct, False otherwise.
    """
    if not KAGGLE_JSON.exists():
        print(f"{KAGGLE_JSON} does not exist.")
        return False

    os_type = platform.system()
    if os_type in ["Linux", "Darwin"]:  # Unix-based systems
        # Get file permissions
        st = os.stat(KAGGLE_JSON)
        permissions = stat.S_IMODE(st.st_mode)
        
        if permissions != 0o600:
            print(f"Incorrect permissions {oct(permissions)}. Setting to 600.")
            os.chmod(KAGGLE_JSON, 0o600)
        else:
            print("Correct permissions (600).")
    elif os_type == "Windows":  # Windows systems
        # Check if the file is read-only
        if not os.access(KAGGLE_JSON, os.R_OK) or os.access(KAGGLE_JSON, os.W_OK):
            print(f"Incorrect permissions. Setting read-only.")
            os.chmod(KAGGLE_JSON, stat.S_IREAD)
        else:
            print("Correct permissions (read-only).")
    else:
        print(f"Unsupported OS: {os_type}")
        return False
    
    return True


def validate_kaggle_json() -> bool:
    """
    Check if the kaggle.json file contains valid content (keys: 'username' and 'key').
    
    Returns:
        bool: True if the file contains valid JSON and required keys, False otherwise.
    """
    if not KAGGLE_JSON.exists():
        print(f"{KAGGLE_JSON} does not exist.")
        return False

    try:
        import json
        with open(KAGGLE_JSON, 'r') as f:
            data = json.load(f)

        if 'username' in data and 'key' in data:
            print("kaggle.json is valid.")
            return True
        else:
            print("Invalid kaggle.json. Missing 'username' or 'key'.")
            return False
    except json.JSONDecodeError:
        print("Error decoding kaggle.json. Ensure it is a valid JSON file.")
        return False


def kaggle_json_utils(custom_path: str = None):
    """
    Helper function to perform all kaggle.json-related checks and actions.

    Args:
        custom_path (str): Optional path to a custom location of kaggle.json.
    """
    if not find_kaggle_json(custom_path):
        return
    
    if not check_kaggle_json_permissions():
        return

    validate_kaggle_json()

