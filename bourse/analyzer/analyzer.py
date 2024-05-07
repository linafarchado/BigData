import pandas as pd
import numpy as np
import os
import timescaledb_model as tsdb
import logging
import bz2
from concurrent.futures import ProcessPoolExecutor, as_completed
import os
from tqdm import tqdm
from datetime import datetime

db = tsdb.TimescaleStockMarketModel('bourse', 'ricou', 'db', 'monmdp')        # inside docker
#db = tsdb.TimescaleStockMarketModel('bourse', 'ricou', 'localhost', 'monmdp') # outside docker

logging.basicConfig(level=logging.DEBUG)
MAX_INT_VALUE = 2147483647

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

    existing_symbols = set()
    # Fetch existing symbols from the database in chunks
    for chunk in db.df_query("SELECT DISTINCT symbol FROM companies WHERE symbol IN %s", args=(tuple(unique_symbols),), chunksize=10000):
        existing_symbols.update(chunk['symbol'])
    index_total = next(db.df_query("SELECT count(*) FROM companies"))['count'][0]

    unique_symbols_df = unique_symbols_df[~unique_symbols_df['symbol'].isin(existing_symbols)].reset_index()
    unique_symbols_df.index = unique_symbols_df.index + index_total + 1

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

    stocks_df = pd.DataFrame({
        "date": df["date"].copy(),
        "cid": df['id'],
        "value": df["last"].copy(),
        "volume": df["volume"].copy(),
    })

    db.df_write(stocks_df, "stocks", index=False, if_exists="append", commit=True)

    return stocks_df

# Add the data to the daystocks table
def add_daystocks(df, comp_dict):
    logging.debug(f'In add_daystocks')

    daily_stats = df.resample('D', on='date').agg({
            'last': ['first', 'last', 'max', 'min'],
            'date': ['first'],
            'symbol': ['first'],
            'volume': ['sum']
        }).reset_index()
    
    daily_stats.columns = ["", 'open', 'close', 'high', 'low', 'date', 'symbol', 'volume']

    daily_stats['id'] = daily_stats['symbol'].apply(lambda x: comp_dict.get(x))
    daily_stats.dropna(subset=['date'], inplace=True)

    daystocks_df = pd.DataFrame({
        "date": daily_stats["date"].copy(),
        "cid": daily_stats["id"].copy(),
        "open": daily_stats["open"].copy(),
        "close": daily_stats["close"].copy(),
        "high": daily_stats["high"].copy(),
        "low": daily_stats["low"].copy(),
        "volume": daily_stats["volume"].copy()
    })

    daystocks_df.loc[daystocks_df['volume'] > MAX_INT_VALUE, 'volume'] = MAX_INT_VALUE
    # daystocks_df = daystocks_df[daystocks_df['volume'] <= MAX_INT_VALUE]

    db.df_write(daystocks_df, "daystocks", index=False, if_exists="append", commit=True)

    del daystocks_df
    del daily_stats

def add_tags(df):
    logging.debug(f'In add_tags')

    tags_df = pd.DataFrame({
        "name": df["name"].copy(),
        "value": df["last"].copy()
    })

    db.df_write(tags_df, "tags", index=False, if_exists="append", commit=True)

    return tags_df

# Add the data to the file_done table
def add_file_done(df):
    logging.debug(f'In add_file_done')

    filedone_df = pd.DataFrame({
        "name": df["filename"].unique()
    })

    db.df_write(filedone_df, "file_done", index=False, if_exists="append", commit=True)

    return filedone_df

comp_dict = {}

def make_companies_dict(df):
    logging.debug(f'In make_companies_dict')
    comp_dict.update(df.set_index('symbol')['mid'].to_dict())
    
def add_to_database(df):
    logging.debug(f'In add_to_database')

    for _, group in df.groupby('filename'):
        comp_df = add_companies(group)
        make_companies_dict(comp_df)
        logging.debug(f"here is the dict: {comp_dict}")

        stocks_df = add_stocks(group, comp_dict)
        del stocks_df
        del comp_df

        add_file_done(group)
        del group
    
    for _, group in df.groupby('symbol'):
        add_daystocks(group, comp_dict)


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
    if (len(path) > 0):
        df = pd.concat([load_and_clean_file(p) for p in path])
        add_to_database(df)
        del df

def load_all_files():
    logging.debug(f'In load_all_files')

    folder_path = "/home/bourse/data/boursorama/"
    file_paths_by_year_month = {}

    year_folder = "2020"
    if True:
    # year_folder in os.listdir(folder_path):
        year_path = os.path.join(folder_path, year_folder)
        for i in ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"]:
            key = year_folder + "-" + i
            file_paths_by_year_month[key] = []

        if os.path.isdir(year_path):
            for file_name in os.listdir(year_path):
                if not db.is_file_done(file_name):
                    year_month = "-".join(file_name.split()[1].split("-")[:2])  # Extract year-month from file name
                    file_paths_by_year_month[year_month].append(os.path.join(year_path, file_name))

    logging.debug(f"Total number of files to process: {sum(len(files) for files in file_paths_by_year_month.values())}")

    return file_paths_by_year_month

def init_comp_dict():
    df = list(db.df_query("SELECT * FROM companies"))
    df = pd.concat(df, ignore_index=True)
    print(df)
    comp_dict.update(df.set_index('symbol')['mid'].to_dict())


def fill_database():
    file_paths = load_all_files()

    logging.debug("Starting to process files")

    init_comp_dict()

    for key in tqdm(file_paths, total=len(file_paths), desc="Processing Months"):
        logging.debug(f"Month to process: {key}")
        process_file(file_paths[key])

    del file_paths

    # max_workers = os.cpu_count() * 3
    # with ProcessPoolExecutor(max_workers=max_workers) as executor:
    #     futures = [executor.submit(process_file, path) for path in file_paths]

    #     with tqdm(total=len(futures), desc="Processing files") as pbar:
    #         for future in as_completed(futures):
    #             future.result()
    #             pbar.update(1)

if __name__ == '__main__':
    logging.debug(f'In MAIN')

    fill_database()

    print("Done")