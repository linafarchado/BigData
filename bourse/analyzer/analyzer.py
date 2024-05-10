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

logging.basicConfig()
logging.getLogger('timescaledb_model').setLevel(logging.INFO)

MAX_INT_VALUE = 2147483647

comp_dict = {}
market_dict = {}
tags_dict = {}

def clean_c_s(df):
    df['last'] = df['last'].str.replace(r'\((c|s)\)$', '', regex=True)
    df['last'] = df['last'].str.replace(' ', '')
    df['last'] = df['last'].astype(float)
    return df

def clean_data(df):
    df1 = df.drop_duplicates()
    df2 = df1.dropna(subset=['last', 'volume'])
    del df1

    df2 = df2[df2['volume'] > 0]

    return clean_c_s(df2)


# Add the data to the companies table
def add_companies(df):
    # print(f'In add_companies')

    unique_symbols_df = df.drop_duplicates(subset=['key']).reset_index()
    unique_symbols = set(unique_symbols_df['symbol'])

    if len(unique_symbols) == 1:
        # If there's only one element, use it directly without converting to a tuple
        unique_symbols_str = f'({unique_symbols[0]})'
    else:
        # If there are multiple elements, convert the list to a tuple
        unique_symbols_tuple = tuple(unique_symbols)
        unique_symbols_str = str(unique_symbols_tuple)


    existing_symbols = set()
    # Fetch existing symbols from the database in chunks
    for chunk in db.df_query("SELECT DISTINCT symbol, mid FROM companies WHERE symbol IN %s", args=unique_symbols_str, chunksize=10000):
        chunk['key'] = chunk['symbol'] + " " + chunk['mid'].astype(str)
        existing_symbols.update(chunk['key'])
    
    index_total = next(db.df_query("SELECT count(*) FROM companies"))['count'][0]

    unique_symbols_df = unique_symbols_df[~unique_symbols_df['key'].isin(existing_symbols)].reset_index()

    unique_symbols_df['market_id'] = unique_symbols_df['market'].apply(lambda x: market_dict.get(x))

    comp_df = pd.DataFrame({
        "name": unique_symbols_df["name"].copy(),
        "mid": unique_symbols_df["market_id"].copy(),
        "symbol": unique_symbols_df["symbol"].copy(),
        "symbol_nf": None,
        "isin": None,
        "reuters": None,
        "boursorama": None,
        "pea": None,
        "sector": None
    })

    db.df_write(comp_df, "companies", index=False, if_exists="append", commit=True)

    comp_df['id'] = comp_df.index + index_total + 1

    del unique_symbols_df

    return comp_df

# Add the data to the stocks table
def add_stocks(df):
    # print(f'In add_stocks')

    df['id'] = df['key'].apply(lambda x: comp_dict.get(x))

    stocks_df = pd.DataFrame({
        "date": df["date"].copy(),
        "cid": df['id'],
        "value": df["last"].copy(),
        "volume": df["volume"].copy(),
    })

    # stocks_df.loc[stocks_df['volume'] > MAX_INT_VALUE, 'volume'] = MAX_INT_VALUE
    stocks_df = stocks_df[stocks_df['volume'] <= MAX_INT_VALUE]

    db.df_write(stocks_df, "stocks", index=False, if_exists="append", commit=True)

    del stocks_df

# Add the data to the daystocks table
def add_daystocks(df, key):
    # print(f'In add_daystocks')

    daily_stats = df.resample('D', on='date').agg({
            'last': ['first', 'last', 'max', 'min'],
            'date': ['first'],
            'symbol': ['first'],
            'volume': ['sum']
        }).reset_index()
    
    daily_stats.columns = ["", 'open', 'close', 'high', 'low', 'date', 'symbol', 'volume']
    daily_stats['key'] = key

    daily_stats['id'] = daily_stats['key'].apply(lambda x: comp_dict.get(x))
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

    # daystocks_df.loc[daystocks_df['volume'] > MAX_INT_VALUE, 'volume'] = MAX_INT_VALUE
    daystocks_df = daystocks_df[daystocks_df['volume'] <= MAX_INT_VALUE]

    db.df_write(daystocks_df, "daystocks", index=False, if_exists="append", commit=True)

    del daystocks_df
    del daily_stats

def add_tags(df):
    # print(f'In add_tags')
    counts = []
    for key in market_dict.keys():
        count = next(db.df_query(f"SELECT count(*) FROM companies WHERE mid = (SELECT id FROM markets where alias = '{key}')"))['count'][0]
        counts.append(count)

    tags_df = pd.DataFrame({
        "name": market_dict.keys(),
        "value": counts
    })

    db.df_write(tags_df, "tags", index=False, if_exists="replace", commit=True)

    del tags_df

def add_market(name):
    if not name in market_dict:
        # Determine the next available ID
        next_id = max(market_dict.values(), default=0) + 1
        market_dict[name] = next_id
        
        # Create a DataFrame for the new market record
        market_df = pd.DataFrame({
            "id": [next_id],
            "name": [name],
            "alias": [name]  # Assuming the alias is the same as the name for new markets
        })

        # Write the new market record to the 'markets' table
        db.df_write(market_df, "markets", index=False, if_exists="append", commit=True)
        
    return name


# Add the data to the file_done table
def add_file_done(df):
    # print(f'In add_file_done')

    filedone_df = pd.DataFrame({
        "name": df["filename"].unique()
    })

    db.df_write(filedone_df, "file_done", index=False, if_exists="append", commit=True)

    del filedone_df

def make_companies_dict(df):
    # print(f'In make_companies_dict')
    df['key'] = df['symbol'] + " " + df['mid'].astype(str)
    # print(df['key'])
    comp_dict.update(df.set_index('key')['id'].to_dict())
    
def add_to_database(df):
    print(f'In add_to_database')

    total_groups_filename = len(df.groupby('filename'))
    for _, group in tqdm(df.groupby('filename'), total=total_groups_filename, desc="Add Companies to DataBase"):
        add_tags(group)
        comp_df = add_companies(group)
        make_companies_dict(comp_df)
        del comp_df
        del group

    total_groups_symbol = len(df.groupby('symbol'))
    for _, group in tqdm(df.groupby('key'), total=total_groups_symbol, desc="Add Stocks to DataBase"):
        add_daystocks(group, group['key'].iloc[0])
        add_stocks(group)
        del group

    total_groups_filename = len(df.groupby('filename'))
    for _, group in tqdm(df.groupby('filename'), total=total_groups_filename, desc="Add Files to DataBase"):
        add_file_done(group)
        del group

def extract_date_filename_market(filepath):
    filename = os.path.basename(filepath)
    # Supprimer 'amsterdam' du début de la chaîne
    date_str = filepath.split(" ")[1] + " " + filepath.split(" ")[2].split(".")[0]
    market = filename.split(" ")[0]
    return pd.to_datetime(date_str, format='%Y-%m-%d %H:%M:%S'), filename, market

def load_and_clean_file(path):
    with bz2.BZ2File(path, 'rb') as file:
        df = pd.read_pickle(file)
        df.reset_index(drop=True, inplace=True)
        date, filename, market = extract_date_filename_market(path)
        df['date'] = date
        df['filename'] = filename
        df['market'] = market

        for name in df['market'].drop_duplicates():
            add_market(name)

        df['key'] = df['symbol'] + " " + df['market'].apply(lambda x: str(market_dict.get(x)))

        return clean_data(df)

def process_file(path, key):
    if (len(path) > 0):
        df = pd.concat([load_and_clean_file(p) for p in tqdm(path, total=len(path), desc=f"Load and clean {key}")])
        add_to_database(df)
        del df

def load_all_files():
    print(f'In load_all_files')

    folder_path = "/home/bourse/data/boursorama/"
    file_paths_by_year_month = {}

    # year_folder = "2023"
    # if True:
    for year_folder in os.listdir(folder_path):
        year_path = os.path.join(folder_path, year_folder)
        for i in ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"]:
            key = year_folder + "-" + i
            file_paths_by_year_month[key] = []

        if os.path.isdir(year_path):
            for file_name in os.listdir(year_path):
                if not db.is_file_done(file_name):
                    year_month = "-".join(file_name.split()[1].split("-")[:2])  # Extract year-month from file name
                    file_paths_by_year_month[year_month].append(os.path.join(year_path, file_name))

    print(f"Total number of files to process: {sum(len(files) for files in file_paths_by_year_month.values())}")

    return file_paths_by_year_month

def init_comp_dict():
    df = list(db.df_query("SELECT symbol, mid, id FROM companies"))
    df = pd.concat(df, ignore_index=True)
    df['key'] = df['symbol'] + " " + df['mid'].astype(str)
    comp_dict.update(df.set_index('key')['id'].to_dict())

def init_market_dict():
    df = list(db.df_query("SELECT * FROM markets"))
    df = pd.concat(df, ignore_index=True)
    market_dict.update(df.set_index('alias')['id'].to_dict())

def init_tags_dict():
    df = list(db.df_query("SELECT * FROM tags"))
    df = pd.concat(df, ignore_index=True)
    tags_dict.update(df.set_index('name')['value'].to_dict())

def fill_database():

    init_comp_dict()
    init_market_dict()
    init_tags_dict()

    file_paths = load_all_files()

    logging.info("Starting to process files")

    

    for key in tqdm(file_paths, total=len(file_paths), desc="Processing Months"):
        process_file(file_paths[key], key)

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