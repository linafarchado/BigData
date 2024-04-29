import pandas as pd
import os
import timescaledb_model as tsdb
from multiprocessing import Pool, cpu_count
import logging
from functools import partial

# Configurez la journalisation
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s', handlers=[logging.FileHandler('process.log'), logging.StreamHandler()])

db = tsdb.TimescaleStockMarketModel('bourse', 'ricou', 'db', 'monmdp')        # inside docker
#db = tsdb.TimescaleStockMarketModel('bourse', 'ricou', 'localhost', 'monmdp') # outside docker

def clean_c_s(df):
    grouped_df = df.groupby("name")

    for name, group_df in grouped_df:
        for index, row in group_df.iterrows():
            if "(c)" in str(row["last"]) or "(s)" in str(row["last"]):
                df.loc[index, "last"] = row["last"][:-3]

    return df

def clean_data(df):
    logging.debug("Cleaning the data")
    df1 = df.drop_duplicates()
    df2 = df1.dropna()

    return clean_c_s(df2)

def preprocess_boursorama_data(df):
    # Groupe les données par date et symbole
    grouped_data = df.groupby(['date', 'symbol'])

    # Initialise une liste pour stocker les DataFrames traités
    processed_dfs = []

    # Itère sur les groupes
    for group_name, group_df in grouped_data:
        date, symbol = group_name

        # Sélectionne les valeurs uniques pour les colonnes open, high, low, close, volume
        group_df = group_df[['open', 'high', 'low', 'close', 'volume']].agg(['max', 'min'])
        group_df.columns = ['_'.join(col).strip('_') for col in group_df.columns.values]
        group_df = group_df.reset_index(drop=True)
        group_df['date'] = date
        group_df['symbol'] = symbol

        processed_dfs.append(group_df)

    # Concatène tous les DataFrames traités en un seul DataFrame
    processed_df = pd.concat(processed_dfs, ignore_index=True)

    return processed_df

# Add the data to the companies table
def add_companies(df):
    logging.debug("Adding data to the companies table")
    comp_df = pd.DataFrame({
        "name": df["name"].copy(),
        "mid": df.index,
        "symbol": df["symbol"].copy(),
        "symbol_nf": None,
        "isin": None,
        "reuters": None,
        "boursorama": None,
        "pea": None,
        "sector": None
    })

    db.df_write(comp_df, "companies", index=False, if_exists="replace", commit=True)

    return comp_df

# Add the data to the stocks table
def add_stocks(df, comp_dict):
    logging.debug("Adding data to the stocks table")
    df['id'] = df['symbol'].apply(lambda x: comp_dict.get(x))

    stocks_df = pd.DataFrame({
        "date": df["date"].copy(),
        "cid": df["id"].copy(),
        "value": df["last"].copy(),
        "volume": df["volume"].copy(),
    })

    db.df_write(stocks_df, "stocks", index=False, if_exists="replace", commit=True)

# Add the data to the daystocks table
def add_daystocks(df, comp_dict):
    logging.debug("Adding data to the daystocks table")
    df['id'] = df['symbol'].apply(lambda x: comp_dict.get(x))

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

# Add the data to the file_done table
def add_file_done(file_name):
    logging.debug(f"Adding {file_name} to the file_done table")
    filedone_df = pd.DataFrame({
        "name": [file_name]
    })

    db.df_write(filedone_df, "file_done", index=False, if_exists="replace", commit=True)

def make_companies_dict(df):
    comp_dict = df.set_index('symbol')['mid'].to_dict()

    return comp_dict

# Process a single file and add it to the database
def process_file(file_path):
    try:
        # If the file was already added to the Database, skip it
        if db.is_file_done(file_path):
            logging.debug(f"The file {file_path} has already been added before")
            return
        
        # Read and clean the file
        df = pd.read_pickle(file_path)
        df['date'] = file_path.split(" ")[1] + " " + file_path.split(" ")[2].split(".")[0] # We add the date
        df['filename'] = file_path # We add the file_name

        df_final = clean_data(df)
        df_processed = df_final # preprocess_boursorama_data(df_final)

        # Add the data to the database
        comp_df = add_companies(df_processed)
        comp_dict = make_companies_dict(comp_df)
        add_daystocks(df_processed, comp_dict)
        add_stocks(df_processed, comp_dict)
        add_file_done(file_path)

        logging.debug(f"File {file_path} processed successfully")
    except Exception as e:
        logging.error(f"Error processing file {file_path}: {e}")

def fill_database():
    
    folder_path = "/home/bourse/data/boursorama/"
    file_paths = []
    for year_folder in os.listdir(folder_path):
        year_path = os.path.join(folder_path, year_folder)
        if os.path.isdir(year_path):
            file_paths.extend([os.path.join(year_path, file_name) for file_name in os.listdir(year_path)])

    logging.debug(f"Total number of files to process: {len(file_paths)}")
    # Parallélisation avec multiprocessing
    num_processes = cpu_count() * 2
    with Pool(processes=num_processes) as pool:
        pool.map(process_file, file_paths)
        pool.close()
        pool.join()

    logging.debug("Done")

if __name__ == '__main__':
    fill_database()