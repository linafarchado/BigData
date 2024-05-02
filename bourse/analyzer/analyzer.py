import pandas as pd
import numpy as np
import os
import timescaledb_model as tsdb
import logging
from multiprocessing import Pool, cpu_count
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
    grouped_df = df.groupby("name")

    for name, group_df in grouped_df:
        for index, row in group_df.iterrows():
            if "(c)" in str(row["last"]) or "(s)" in str(row["last"]):
                df.loc[index, "last"] = row["last"][:-3]

    return df

def clean_data(df):
    df1 = df.drop_duplicates()
    df2 = df1.dropna()

    return clean_c_s(df2)

# Add the data to the companies table
def add_companies(df):
    logging.debug(f'In add_companies')

    # Remove all duplicates in the name
    unique_names_df = df.drop_duplicates(subset=['symbol']).reset_index()

    comp_df = pd.DataFrame({
        "name": unique_names_df["name"].copy(),
        "mid": unique_names_df.index,
        "symbol": unique_names_df["symbol"].copy(),
        "symbol_nf": None,
        "isin": None,
        "reuters": None,
        "boursorama": None,
        "pea": None,
        "sector": None
    })

    db.execute("ALTER SEQUENCE company_id_seq RESTART WITH 1", commit=True)
    db.df_write(comp_df, "companies", index=False, if_exists="replace", commit=True)

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

    db.df_write(stocks_df, "stocks", index=False, if_exists="replace", commit=True)

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

    db.df_write(daystocks_df, "daystocks", index=False, if_exists="replace", commit=True)

    return daystocks_df

# Add the data to the file_done table
def add_file_done(df):
    logging.debug(f'In add_file_done')

    filedone_df = pd.DataFrame({
        "name": df["filename"].unique().copy()
    })

    db.df_write(filedone_df, "file_done", index=False, if_exists="replace", commit=True)

    return filedone_df

def make_companies_dict(df):
    comp_dict = df.set_index('symbol')['mid'].to_dict()

    return comp_dict

# Er add everyting to the database
def add_to_database(df):
    logging.debug(f'In add_to_database')

    comp_df = add_companies(df)

    comp_dict = make_companies_dict(comp_df)

    add_daystocks(df.copy(), comp_dict)
    add_stocks(df.copy(), comp_dict)
    add_file_done(df.copy())

def store_file(name):
    logging.debug(f"File Read: {name}")
    df = pd.read_pickle(name, compression='bz2')

    df['date'] = name.split(" ")[1] + " " + name.split(" ")[2].split(".")[0]
    df['filename'] = name

    df_final = clean_data(df)

    return df_final
    
def run_imap_unordered_multiprocessing(func, argument_list, num_processes):
    delayed_reads = []
    for path in argument_list:
        delayed_reads.append(dask.delayed(func)(path))

    with ProgressBar():
        result = dask.compute(*delayed_reads, n_workers=num_processes)

    return result

def load_all_files():
    logging.debug(f'In load_all_files')

    folder_path = "/home/bourse/data/boursorama/"
    file_paths = []
    for year_folder in os.listdir(folder_path):
        year_path = os.path.join(folder_path, year_folder)
        if os.path.isdir(year_path):
            file_paths.extend([os.path.join(year_path, file_name) for file_name in os.listdir(year_path) if not db.is_file_done(file_name)])

    logging.debug(f"Total number of files to process: {len(file_paths)}")

    dfs = []
    num_processes = cpu_count() // 2
    return run_imap_unordered_multiprocessing(store_file, file_paths, num_processes)

def fill_database():
    logging.debug("In fill_database")

    df = load_all_files()
    add_to_database(df)

if __name__ == '__main__':
    logging.debug(f'In MAIN')

    fill_database()
    print("Done")