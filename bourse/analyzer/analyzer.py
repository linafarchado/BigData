import pandas as pd
import numpy as np
import sklearn

import os

import timescaledb_model as tsdb

db = tsdb.TimescaleStockMarketModel('bourse', 'ricou', 'db', 'monmdp')        # inside docker
#db = tsdb.TimescaleStockMarketModel('bourse', 'ricou', 'localhost', 'monmdp') # outside docker

def clean_c(df):
    grouped_df = df.groupby("name")

    for name, group_df in grouped_df:
        
        for index, row in group_df.iterrows():
            if "(c)" in str(row["last"]):
                df.loc[index, "last"] = row["last"][:-3]

    return df

def clean_s(df):
    grouped_df = df.groupby("name")

    for name, group_df in grouped_df:
        
        for index, row in group_df.iterrows():
            if "(s)" in str(row["last"]):
                df.loc[index, "last"] = row["last"][:-3]

    return df

def clean_1r(df):
    grouped_df = df.groupby("name")

    for name, group_df in grouped_df:
        
        for index, row in group_df.iterrows():
            if "1r" in str(row["symbol"]):
                df.loc[index, "symbol"] = row["symbol"][2:]

    return df

def clean_data(df):
    df1 = df.drop_duplicates()
    df2 = df1.dropna()

    df3 = clean_c(df2)
    df4 = clean_s(df3)
    df5 = clean_1r(df4)

    return df5

# Add the data to the companies table
def add_companies(df):
    # Remove all duplicates in the name
    unique_names_df = df.drop_duplicates(subset=['name']).reset_index()

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

# Add the data to the stocks table
def add_stocks(df):
    stocks_df = pd.DataFrame({
        "date": df["date"].copy(),
        "cid": None,
        "value": df["last"].copy(),
        "volume": df["volume"].copy(),
    })

    db.df_write(stocks_df, "stocks", index=False, if_exists="replace")

# Add the data to the daystocks table
def add_daystocks(df):
    daystocks_df = pd.DataFrame({
        "date": df["date"].copy(),
        "cid": None,
        "open": df["last"].copy(),
        "close": df["last"].copy(),
        "high": df["last"].copy(),
        "low": df["last"].copy(),
        "volume": df["volume"].copy()
    })

    db.df_write(daystocks_df, "daystocks", index=False, if_exists="replace")

# Add the data to the file_done table
def add_file_done(df):
    filedone_df = pd.DataFrame({
        "name": df["filename"].unique().copy()
    })

    db.df_write(filedone_df, "file_done", index=False, if_exists="replace")

# Er add everyting to the database
def add_to_database(df):
    add_companies(df)
    add_daystocks(df)
    add_stocks(df)
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
    
def load_specific_date(spec):
    year = spec.split("-")[0]
    folder_path = "/home/bourse/data/boursorama/" + year + "/"

    file_paths = []

    # Check through all the folder
    for filename in os.listdir(folder_path):

    # Check if filename contains the desired date
        if spec in filename:

            # Construct the full path
            file_path = os.path.join(folder_path, filename)
            file_paths.append(file_path)

    data_frames = []
    for file_path in file_paths:

    # Read the CSV file into a DataFrame
        df = store_file(file_path, "boursorama")
        if df is not None:
            data_frames.append(df)

    # Combine all DataFrames into a single DataFrame (optional)
    if (len(data_frames) == 0):
        return None
    return pd.concat(data_frames, ignore_index=True)
    
def fill_database():
    # Later will be changed to loading all the datas
    df = load_specific_date("2020-01-01")

    # Add the dataframe to the Database
    if (df is not None):
        add_to_database(df)

if __name__ == '__main__':
    fill_database()
    print("Done")