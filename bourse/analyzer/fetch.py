import pandas as pd
import yfinance as yf

def fetch_stock_data(symbol, date):
    
    date = date.apply(lambda x: x.split(" ")[0])
    try:
        data = yf.download(symbol, period='1d', start=date, end=date, ignore_tz=True)
        return data
    except Exception as e:
        print(f"Erreur lors de la récupération des données pour {symbol} le {date}: {e}")
        return pd.DataFrame()
    
def preprocess_boursorama_data(df):
    additional_data = df.apply(lambda row: fetch_stock_data(row['symbol'], row['date']), axis=1)

    additional_data = [data for data in additional_data.tolist() if not data.empty]
    additional_data = pd.concat(additional_data, keys=df.index)

    additional_data.index = additional_data.index.droplevel(1) 
    additional_data = additional_data.reset_index(drop=True)

    df[['open', 'close', 'high', 'low']] = additional_data[['Open', 'Close', 'High', 'Low']]
    return df
