import pandas as pd
import numpy as np
import sklearn

import timescaledb_model as tsdb

db = tsdb.TimescaleStockMarketModel('bourse', 'ricou', 'db', 'monmdp')        # inside docker
#db = tsdb.TimescaleStockMarketModel('bourse', 'ricou', 'localhost', 'monmdp') # outside docker

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
    store_file("amsterdam 2020-12-31 17:51:02.225763", "boursorama")
    print("Done")
