import pandas as pd
import numpy as np
import os
import timescaledb_model as tsdb
import logging
import bz2
from concurrent.futures import ProcessPoolExecutor, as_completed
import os
from tqdm import tqdm

import dask
import dask.dataframe as dd
from dask.diagnostics import ProgressBar
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

pool = ProcessPoolExecutor()

dask.config.set(pool=pool)

db = tsdb.TimescaleStockMarketModel('bourse', 'ricou', 'db', 'monmdp')        # inside docker
#db = tsdb.TimescaleStockMarketModel('bourse', 'ricou', 'localhost', 'monmdp') # outside docker

logging.basicConfig(level=logging.DEBUG)

def clean_c_s(df):
    df['last'] = df['last'].str.replace(r'\((c|s)\)$', '', regex=True)
    df['last'] = df['last'].str.replace(' ', '')
    df['last'] = df['last'].astype(float)
    return df

def clean_data(df):
    df1 = df.drop_duplicates()
    df2 = df1.dropna(subset=['last', 'volume'])
    del df1
    return clean_c_s(df2)

# Add the data to the companies table
def add_companies(df):
    logging.debug(f'In add_companies')

    unique_symbols_df = df.drop_duplicates(subset=['symbol']).reset_index()
    unique_symbols = set(unique_symbols_df['symbol'])
    
    # Fetch existing symbols from the database in chunks
    existing_symbols = set()
    for chunk in db.df_query("SELECT DISTINCT symbol FROM companies WHERE symbol IN %s", args=(tuple(unique_symbols),), chunksize=10000):
        existing_symbols.update(chunk['symbol'])

    unique_symbols_df = unique_symbols_df[~unique_symbols_df['symbol'].isin(existing_symbols)]

    comp_df = pd.DataFrame({
        "name": unique_symbols_df["name"].copy(),
        "mid": unique_symbols_df.index,
        "symbol": unique_symbols_df["symbol"].copy(),
        "symbol_nf": None,
        "isin": None,
        "reuters": None,
        "boursorama": None,
        "pea": None,
        "sector": None
    })

    db.df_write(comp_df, "companies", index=False, if_exists="append", commit=True)

    del unique_symbols_df

    return comp_df

# Add the data to the stocks table
def add_stocks(df, comp_dict):
    logging.debug(f'In add_stocks')

    df['id'] = df['symbol'].apply(lambda x: comp_dict.get(x))
    df['last'] = df['last'].str.replace(' ', '', regex=True)

    stocks_df = pd.DataFrame({
        "date": df["date"].copy(),
        "cid": df["id"].copy(),
        "value": df["last"].copy(),
        "volume": df["volume"].copy(),
    })

    db.df_write(stocks_df, "stocks", index=False, if_exists="append", commit=True)

    return stocks_df

# Add the data to the daystocks table
def add_daystocks(df, comp_dict):
    logging.debug(f'In add_daystocks')

    df['id'] = df['symbol'].apply(lambda x: comp_dict.get(x))
    df['last'] = df['last'].str.replace(' ', '', regex=True)

    daystocks_df = pd.DataFrame({
        "date": df["date"].copy(),
        "cid": df["id"].copy(),
        "open": df["last"].copy(),
        "close": df["last"].copy(),
        "high": df["last"].copy(),
        "low": df["last"].copy(),
        "volume": df["volume"].copy()
    })

    db.df_write(daystocks_df, "daystocks", index=False, if_exists="append", commit=True)

    return daystocks_df

# Add the data to the file_done table
def add_file_done(df):
    logging.debug(f'In add_file_done')

    filedone_df = pd.DataFrame({
        "name": df["filename"].unique()
    })

    db.df_write(filedone_df, "file_done", index=False, if_exists="append", commit=True)

    return filedone_df

def make_companies_dict(df):
    comp_dict = df.set_index('symbol')['mid'].to_dict()
    del df
    return comp_dict

def add_to_database(df):
    logging.debug(f'In add_to_database')
    for _, group in df.groupby(df.index // 1000):  # Regroupez les données par lots de 1000 fichiers
        comp_df = add_companies(group)
        comp_dict = make_companies_dict(comp_df)
        del comp_df

        daystocks_df = add_daystocks(group, comp_dict)
        del daystocks_df

        stocks_df = add_stocks(group, comp_dict)
        del stocks_df

        add_file_done(group)
        del group

def load_all_files():
    logging.debug(f'In load_all_files')

    folder_path = "/home/bourse/data/boursorama/"
    file_paths = []
    for year_folder in os.listdir(folder_path):
        year_path = os.path.join(folder_path, year_folder)
        if os.path.isdir(year_path):
            file_paths.extend([os.path.join(year_path, file_name) for file_name in os.listdir(year_path) if not db.is_file_done(file_name)])

    logging.debug(f"Total number of files to process: {len(file_paths)}")

    return file_paths

def extract_date_filename(filepath):
    filename = os.path.basename(filepath)
    # Supprimer 'amsterdam' du début de la chaîne
    date_str = filepath.split(" ")[1] + " " + filepath.split(" ")[2].split(".")[0]
    return pd.to_datetime(date_str, format='%Y-%m-%d %H:%M:%S'), filename

def load_and_clean_file(path):
    with bz2.BZ2File(path, 'rb') as file:
        df = pd.read_pickle(file)
        df.reset_index(drop=True, inplace=True)
        date, filename = extract_date_filename(path)
        df['date'] = date
        df['filename'] = filename
        return clean_data(df)

def process_file(path):
    df = load_and_clean_file(path)
    add_to_database(df)
    del df


def fill_database():
    file_paths = load_all_files()

    logging.debug("Starting to process files")
    max_workers = os.cpu_count() * 3
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(process_file, path) for path in file_paths]

        with tqdm(total=len(futures), desc="Processing files") as pbar:
            for future in as_completed(futures):
                future.result()
                pbar.update(1)

if __name__ == '__main__':
    logging.debug(f'In MAIN')

    fill_database()
    print("Done")