import requests
import pandas as pd
import sqlite3
from datetime import datetime
import os

#configuration
API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY")  
SYMBOL = "AAPL"
DB_PATH = "/data/stock_data.db" 

#extract
def extract_stock_data(symbol, api_key):
    url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={api_key}&outputsize=compact"
    response = requests.get(url)
    data = response.json()
    
    if "Time Series (Daily)" not in data:
        raise ValueError("Error fetching data from API")
    
    # Convert to DataFrame
    df = pd.DataFrame(data["Time Series (Daily)"]).T
    df.reset_index(inplace=True)
    df.rename(columns={"index": "date", "1. open": "open", "2. high": "high", 
                       "3. low": "low", "4. close": "close", "5. volume": "volume"}, inplace=True)
    return df

#transform
def transform_data(df):
    df["date"] = pd.to_datetime(df["date"])
    df[["open", "high", "low", "close", "volume"]] = df[["open", "high", "low", "close", "volume"]].astype(float)
    
    df["ma7"] = df["close"].rolling(window=7).mean()
    
    df["symbol"] = SYMBOL
    df["updated_at"] = datetime.now()
    
    return df[["symbol", "date", "open", "high", "low", "close", "volume", "ma7", "updated_at"]]

#load
def load_data(df, db_path):
    conn = sqlite3.connect(db_path)
    df.to_sql("stock_prices", conn, if_exists="append", index=False)
    conn.close()

def run_etl():
    try:
        raw_data = extract_stock_data(SYMBOL, API_KEY)
        print("Data extracted successfully")
        
        transformed_data = transform_data(raw_data)
        print("Data transformed successfully")
        
        load_data(transformed_data, DB_PATH)
        print(f"Data loaded to {DB_PATH}")
        
    except Exception as e:
        print(f"Error in ETL pipeline: {e}")

if __name__ == "__main__":
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS stock_prices (
            symbol TEXT,
            date DATE,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume INTEGER,
            ma7 REAL,
            updated_at DATETIME
        )
    """)
    conn.commit()
    conn.close()
    
    run_etl()