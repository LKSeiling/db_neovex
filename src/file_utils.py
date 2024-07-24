from glob import glob
from copy import deepcopy
import pandas as pd
import pickle
import os
import re
from json import load as jload

def get_extension(filepath):
    ext = filepath.split(".")[-1]
    return ext

def get_valid_filepaths(base_dir):
    allowed_filetypes = ['csv','xlsx','pkl','json']
    all_paths = [path for path in glob(f'{base_dir}**/*', recursive=True)]
    valid_file_paths = [path for path in all_paths if "$RECYCLE.BIN" not in path and "." in path and get_extension(path) in allowed_filetypes]
    return valid_file_paths

def get_df(filepath):
    file_type = filepath.split(".")[-1]
    if file_type == "csv":
        df = get_csv_df(filepath)
    elif file_type == "xlsx":
        df = get_xlsx_df(filepath)
    elif file_type == "pkl":
        df = get_pkl_df(filepath)
    elif file_type == "json":
        df = get_json_df(filepath)
    else:
        raise ValueError("The provided filetype is not supported.")
    return df

def get_encoding(filepath):
    with open(filepath) as f:
        enc = f.encoding
    return enc.lower()

def get_csv_df(filepath):
    filesize = get_filesize(filepath)
    if filesize < 500000:
        try:
            enc = get_encoding(filepath)
            df = pd.read_csv(filepath, encoding=enc, engine='python')
            return df 
        except Exception as e:
            print(f"Error processing {filepath}: {e}")
    else:
        print(f"{filepath} is too big and needs to be chunked.")   

def get_xlsx_df(filepath):
    df = pd.read_excel(filepath)
    return df

def get_pkl_df(filepath):
    filesize = get_filesize(filepath)
    if filesize > 100000:
        pass
    else:
        with open(filepath, 'rb') as f:
            df = pickle.load(f)
        return df

def get_json_df(filepath):
    with open(filepath) as f:
        json_str = jload(f)
    df = pd.read_json(json_str)
    return df

def get_filesize(filepath):
    return int(os.path.getsize(filepath)/1000)

def clean_table_cols(df, check_string=True):
    unwanted_str = "ï»¿"
    for col in df.columns:
        if not check_string or df[col].dtype == object:
            new_column_name = re.sub(r"[^0-9a-zA-Z.,-/_ ]", "", col)
            df.rename(columns={col: new_column_name}, inplace=True)
    return df

def add_to_log(logname, error_message):
    with open(f"./logs/{logname}.log", "a") as log_file:
        log_file.write(str(error_message))
