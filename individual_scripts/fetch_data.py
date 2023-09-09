import yfinance as yf
from db_manager import create_tables, insert_stock_data
import psycopg2
import pandas as pd

DATABASE_URL = "postgresql://postgres:admin@localhost:5432/stock_data"


def fetch_data_from_db(ticker):
    with psycopg2.connect(DATABASE_URL) as conn:
        query = f"SELECT * FROM stocks WHERE ticker = '{ticker}' ORDER BY date"
        df = pd.read_sql(query, conn)
    return df


def fetch_historical_data(tickers, start_date, end_date):
    data_dict = {}

    for ticker in tickers:
        print(f"Fetching data for {ticker}...")
        stock = yf.Ticker(ticker)
        data = stock.history(period="1d", start=start_date, end=end_date)
        data_dict[ticker] = data
        print(
            f"Fetched {len(data)} records for {ticker} from {start_date} to {end_date}."
        )

    return data_dict


if __name__ == "__main__":
    tickers = ["AAPL", "TSLA", "AMD", "F", "NVDA", "INTC", "AMZN", "CSX"]

    print("Fetching historical stock data...")
    data_dict = fetch_historical_data(tickers, "2022-01-01", "2023-01-01")

    print("\nCreating tables in database...")
    create_tables()

    print("\nInserting data into the database...")
    for ticker, data in data_dict.items():
        print(f"Inserting data for {ticker}...")
        insert_stock_data(ticker, data)
        print(f"Data for {ticker} inserted successfully!")

    print("\nAll operations completed!")
