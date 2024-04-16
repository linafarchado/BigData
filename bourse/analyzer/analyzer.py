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
        df = store_file(file_path, "boursorama")

        if df is not None:
            data_frames.append(df)

    # Combine all DataFrames into a single DataFrame (optional)
    df_combined = pd.concat(data_frames, ignore_index=True)
    return df_combined

def first_clean(df):
    grouped_df = df.groupby("name")

    for name, group_df in grouped_df:
        
        for index, row in group_df.iterrows():
            if "(c)" in str(row["last"]):
                df.loc[index, "last"] = row["last"][:-3]

    return df

def second_clean(df):
    grouped_df = df.groupby("name")

    for name, group_df in grouped_df:
        
        for index, row in group_df.iterrows():
            if "(s)" in str(row["last"]):
                df.loc[index, "last"] = row["last"][:-3]

    return df

def third_clean(df):
    grouped_df = df.groupby("name")

    for name, group_df in grouped_df:
        
        for index, row in group_df.iterrows():
            if "1r" in str(row["symbol"]):
                df.loc[index, "symbol"] = row["symbol"][2:]

    return df

def add_to_database(df):
    #for row in df.itertuples():
        #last = float(str(row.last).replace(" ", ""))  # Convert last to float
        #volume = int(str(row.volume).replace(" ", ""))  # Convert volume to int

        #db.execute(f"INSERT INTO stocks (date, value, volume) VALUES ('{row.date}', {last}, {volume});")
        #if (not db.raw_query("SELECT EXISTS (SELECT * FROM file_done);")[0][0]):
            #db.execute(f"INSERT INTO file_done (name) VALUES ('{row.name}');")

    print("Starting to write")
    db.df_write(df[["filename"]], "file_done", if_exists="replace")
    print("Done writing")

    return None

def clean_data(df):
    df = df.drop_duplicates()
    df = df.dropna()

    return df

def store_file(name, website):
    if db.is_file_done(name):
        print(db.is_file_done(name))
        return None
    
    if website.lower() == "boursorama":
        #print(name)

        df = pd.read_pickle(name)
        df['date'] = name.split(" ")[1] + " " + name.split(" ")[2].split(".")[0]
        df['filename'] = name

        df2 = first_clean(df)
        df3 = second_clean(df2)
        df4 = third_clean(df3)
        df_final = clean_data(df4)

        return df_final

if __name__ == '__main__':
    # df = load_specific_date("2022-12-01")
    df = store_file("/home/bourse/data/boursorama/" + "2020" + "/" + "compA 2020-01-01 09:02:02.532411.bz2", "boursorama")

    print(df)
    add_to_database(df)

    # add all names of dataframe into 
    #db.df_write(df[["symbol"]], "companies", index=False)

    #print(df)
    print("Done")