import pandas as pd
import numpy as np
import sklearn

import os

import timescaledb_model as tsdb

db = tsdb.TimescaleStockMarketModel('bourse', 'ricou', 'db', 'monmdp')        # inside docker
#db = tsdb.TimescaleStockMarketModel('bourse', 'ricou', 'localhost', 'monmdp') # outside docker

def load_specific_date(spec):
    year = spec.split("-")[0]
    folder_path = "/home/bourse/data/boursorama/" + year + "/"

    file_paths = []

    for filename in os.listdir(folder_path):
    # Check if filename contains the desired date
        if spec in filename:
            # Construct the full path
            file_path = os.path.join(folder_path, filename)
            file_paths.append(file_path)

    data_frames = []
    for file_path in file_paths:
    # Read the CSV file into a DataFrame
        df = pd.read_pickle(file_path)
        data_frames.append(df)

    # Combine all DataFrames into a single DataFrame (optional)
    df_combined = pd.concat(data_frames, ignore_index=True)
    return df_combined

def first_clean(df_combined):
    grouped_df = df_combined.groupby("name")

    for name, group_df in grouped_df:
        
        for index, row in group_df.iterrows():
            if "(c)" in str(row["last"]):
                df_combined.loc[index, "last"] = row["last"][:-3]

def clean_data(df):
    df = df.drop_duplicates()
    df = df.dropna()

    return df

def store_file(name, website):
    if db.is_file_done(name + ".bz2"):
        print(db.is_file_done(name + ".bz2"))
        return
    if website.lower() == "boursorama":
        try:
            df = pd.read_pickle("/home/bourse/data/boursorama/" + name + ".bz2")  # is this dir ok for you ?
        except:
            year = name.split()[1].split("-")[0]
            df = pd.read_pickle("/home/bourse/data/boursorama/" + year + "/" + name + ".bz2")
        # to be finished
 
        df = clean_data(df)

        print(df)

if __name__ == '__main__':
    #store_file("amsterdam 2020-12-31 17:51:02.225763", "boursorama")
    df = load_specific_date("2022-12-01")
    df2 = first_clean(df)
    print(df2)
    print("Done")
