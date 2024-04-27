import pandas as pd
import numpy as np
from fetch import preprocess_boursorama_data
import os

import timescaledb_model as tsdb

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
    df1 = df.drop_duplicates()
    df2 = df1.dropna()

    return clean_c_s(df2)

# Add the data to the companies table
def add_companies(df):
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
    db.df_write(comp_df, "companies", index=False, if_exists="replace")

    return comp_df

# Add the data to the stocks table
def add_stocks(df, comp_dict):
    df['id'] = df['symbol'].apply(lambda x: comp_dict.get(x))

    stocks_df = pd.DataFrame({
        "date": df["date"].copy(),
        "cid": df["id"].copy(),
        "value": df["last"].copy(),
        "volume": df["volume"].copy(),
    })

    db.df_write(stocks_df, "stocks", index=False, if_exists="replace")

    return stocks_df

# Add the data to the daystocks table
def add_daystocks(df, comp_dict):
    df['id'] = df['symbol'].apply(lambda x: comp_dict.get(x))

    daystocks_df = pd.DataFrame({
        "date": df["date"].copy(),
        "cid": df["id"].copy(),
        "open": df["open"].copy(),
        "close": df["close"].copy(),
        "high": df["high"].copy(),
        "low": df["low"].copy(),
        "volume": df["volume"].copy()
    })

    db.df_write(daystocks_df, "daystocks", index=False, if_exists="replace")

    return daystocks_df

# Add the data to the file_done table
def add_file_done(df):
    filedone_df = pd.DataFrame({
        "name": df["filename"].unique().copy()
    })

    db.df_write(filedone_df, "file_done", index=False, if_exists="replace")

    return filedone_df

def make_companies_dict(df):
    comp_dict = df.set_index('symbol')['mid'].to_dict()

    return comp_dict

# Er add everyting to the database
def add_to_database(df):
    comp_df = add_companies(df)

    comp_dict = make_companies_dict(comp_df)

    add_daystocks(df.copy(), comp_dict)
    add_stocks(df.copy(), comp_dict)
    add_file_done(df)

def store_file(name, website):
    # If the file was already added to the Database, skip it
    if db.is_file_done(name):
        print(f"The file {name} has already been added before")
        return None
    
    # Else we add read and clean it
    if website.lower() == "boursorama":
        df = pd.read_pickle(name)
        df['date'] = name.split(" ")[1] + " " + name.split(" ")[2].split(".")[0] # We add the date
        df['filename'] = name # We add the file_name

        df_final = clean_data(df)

        return df_final
    
def load_all_files_in_folder(folder_path):
    data_frames = []
    for filename in os.listdir(folder_path):
        file_path = os.path.join(folder_path, filename)
        # Read the CSV file into a DataFrame
        df = store_file(file_path, "boursorama")
        if df is not None:
            data_frames.append(df)
    
    # Combine all DataFrames into a single DataFrame
    if len(data_frames) == 0:
        return None
    return preprocess_boursorama_data(pd.concat(data_frames, ignore_index=True))

def fill_database():
    # Later will be changed to loading all the datas
    df = load_all_files_in_folder("/home/bourse/data/boursorama/2019/")
    
    if (df is not None):
        add_to_database(df)

if __name__ == '__main__':
    db.execute('DELETE FROM file_done')
    db.execute('DELETE FROM daystocks')
    db.execute('DELETE FROM stocks')
    db.execute('DELETE FROM companies')
    #fill_database()
    #print("Done")