import pandas as pd
import numpy as np
import os
import timescaledb_model as tsdb
import logging
import bz2
from concurrent.futures import ProcessPoolExecutor, as_completed
import os
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
    df['last'] = df['last'].astype(str)
    df['last'] = df['last'].str.replace(r'\((c|s)\)$', '', regex=True)
    df['last'] = df['last'].str.replace(' ', '')
    df['last'] = df['last'].astype(float)
    return df

def clean_data(df):
    df = df.drop_duplicates().dropna(subset=['last', 'volume'])
    df = df[df['volume'] > 0]
    return clean_c_s(df)

# Add the data to the companies table
def add_companies(df):
    # print(f'In add_companies')

    unique_symbols_df = df.drop_duplicates(subset=['key']).reset_index()
    unique_symbols = set(unique_symbols_df['symbol'])

    if len(unique_symbols) == 1:
        # If there's only one element, use it directly without converting to a tuple
        unique_symbols_str = str(unique_symbols).replace('{', '(').replace('}', ')')
    else:
        # If there are multiple elements, convert the list to a tuple
        unique_symbols_tuple = tuple(unique_symbols)
        unique_symbols_str = str(unique_symbols_tuple)
    del unique_symbols

    existing_symbols = set()
    # Fetch existing symbols from the database in chunks
    for chunk in db.df_query("SELECT DISTINCT symbol, mid FROM companies WHERE symbol IN %s", args=unique_symbols_str, chunksize=10000):
        chunk['key'] = chunk['symbol'] + " " + chunk['mid'].astype(str)
        existing_symbols.update(chunk['key'])
    
    index_total = next(db.df_query("SELECT count(*) FROM companies"))['count'][0]

    unique_symbols_df = unique_symbols_df[~unique_symbols_df['key'].isin(existing_symbols)].reset_index()
    del existing_symbols

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

    db.dataframe_to_sql(comp_df, 'companies', columns=list(comp_df.columns.values))

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

    db.dataframe_to_sql(stocks_df, 'stocks', columns=list(stocks_df.columns.values))

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

    db.dataframe_to_sql(daystocks_df, 'daystocks', columns=list(daystocks_df.columns.values))

    del daystocks_df
    del daily_stats

def add_tags():
    # print(f'In add_tags')
    counts = []
    for key in market_dict.keys():
        count = next(db.df_query(f"SELECT count(*) FROM companies WHERE mid = (SELECT id FROM markets where alias = '{key}')"))['count'][0]
        counts.append(count)

    tags_df = pd.DataFrame({
        "name": market_dict.keys(),
        "value": counts
    })

    db.dataframe_to_sql(tags_df, 'tags', columns=list(tags_df.columns.values))

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
        db.dataframe_to_sql(market_df, 'markets', columns=list(market_df.columns.values))
        del market_df
        
    return name


# Add the data to the file_done table
def add_file_done(df):
    # print(f'In add_file_done')

    filedone_df = pd.DataFrame({
        "name": df["filename"].unique()
    })

    db.dataframe_to_sql(filedone_df, 'file_done', columns=list(filedone_df.columns.values))

    del filedone_df

def make_companies_dict(df):
    # print(f'In make_companies_dict')
    df['key'] = df['symbol'] + " " + df['mid'].astype(str)
    # print(df['key'])
    comp_dict.update(df.set_index('key')['id'].to_dict())
    
def add_to_database(df):
    print(f'In add_to_database')

    for _, group in df.groupby('filename'):
        comp_df = add_companies(group)
        make_companies_dict(comp_df)
        add_stocks(group)
        del comp_df
        del group

    for _, group in df.groupby('key'):
        add_daystocks(group, group['key'].iloc[0])
        del group
    add_tags()

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

        add_file_done(df)

        return clean_data(df)

def process_file(path, key):
    if (len(path) > 0):
        df = pd.concat([load_and_clean_file(p) for p in path])
        if not df.empty:
            add_to_database(df)
        del df

def load_all_files():
    print(f'In load_all_files')

    folder_path = "/home/bourse/data/boursorama/"
    file_paths_by_year_month = {}

    # year_folder = "2020"
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
    while sum(len(files) for files in file_paths.values()) != 0:
        logging.info("Starting to process files")

        try:
            for key in file_paths:
                process_file(file_paths[key], key)
        except Exception as e:
           print(f"There has been an error: {e}")

        file_paths = load_all_files()

if __name__ == '__main__':
    logging.debug(f'In MAIN')

    fill_database()

    print("Done")