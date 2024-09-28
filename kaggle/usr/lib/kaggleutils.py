"""
Please upvote and comment if you find the Kaggle Utility Script useful.

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
                print(f"  Utility Script: {subdir}")
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

    for dirname, _, filenames in os.walk(input_dir):
        for filename in filenames:
            filepath = os.path.join(dirname, filename)

            # Load CSV files into DataFrames
            if filename.endswith('.csv'):
                var_name = os.path.splitext(filename)[0] + '_df'
                scope[var_name] = pd.read_csv(filepath)
                print(f"Loaded {var_name} as DataFrame")
            
            # Load JSON files into dictionaries
            elif filename.endswith('.json'):
                var_name = os.path.splitext(filename)[0] + '_dict'
                with open(filepath, 'r') as file:
                    scope[var_name] = json.load(file)
                print(f"Loaded {var_name} as JSON Dictionary")
