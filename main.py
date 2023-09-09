import yfinance as yf
import psycopg2
import numpy as np
import pandas as pd
from db_manager import (
    create_tables,
    insert_stock_data,
    session_scope,
    Stock,
    insert_signal_data,
)

DATABASE_URL = "postgresql://postgres:admin@localhost:5432/stock_data"


def fetch_data_from_db(ticker):
    with psycopg2.connect(DATABASE_URL) as conn:
        query = f"SELECT * FROM stocks WHERE ticker = '{ticker}' ORDER BY date"
        df = pd.read_sql(query, conn)
    return df


def calculate_metrics(df):
    # Calculate moving averages
    df["10_day_MA"] = df["close"].rolling(window=10).mean()
    df["50_day_MA"] = df["close"].rolling(window=50).mean()
    df["200_day_MA"] = df["close"].rolling(window=200).mean()

    # Calculate RSI
    delta = df["close"].diff()
    gain = (delta.where(delta > 0, 0)).fillna(0)
    loss = (-delta.where(delta < 0, 0)).fillna(0)
    avg_gain = gain.rolling(window=14).mean()
    avg_loss = loss.rolling(window=14).mean()
    rs = avg_gain / avg_loss
    df["RSI"] = 100 - (100 / (1 + rs))

    # Calculate MACD and Signal Line
    df["12_day_EMA"] = df["close"].ewm(span=12, adjust=False).mean()
    df["26_day_EMA"] = df["close"].ewm(span=26, adjust=False).mean()
    df["MACD"] = df["12_day_EMA"] - df["26_day_EMA"]
    df["Signal_Line"] = df["MACD"].ewm(span=9, adjust=False).mean()

    return df


def update_db_with_metrics(ticker, df):
    with session_scope() as session:
        for _, row in df.iterrows():
            stock = (
                session.query(Stock)
                .filter(Stock.ticker == ticker, Stock.date == row["date"])
                .first()
            )
            if stock:
                stock.ten_day_MA = row["10_day_MA"]
                stock.fifty_day_MA = row["50_day_MA"]
                stock.two_hundred_day_MA = row["200_day_MA"]
                stock.RSI = row["RSI"]
                stock.MACD = row["MACD"]
                stock.Signal_Line = row["Signal_Line"]


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


def detect_signals(data):
    signals = []
    data = data.dropna(
        subset=[
            "ten_day_MA",
            "fifty_day_MA",
            "two_hundred_day_MA",
            "RSI",
            "MACD",
            "Signal_Line",
        ]
    )
    data["short_long_MA_cross"] = np.where(
        data["ten_day_MA"] > data["fifty_day_MA"], 1, 0
    )

    # Identify potential buy/sell signals based on the derived metrics
    buy_signals = data[data["short_long_MA_cross"].diff() == 1].index
    for date in buy_signals:
        signals.append(("Buy", date))

    sell_signals = data[data["short_long_MA_cross"].diff() == -1].index
    for date in sell_signals:
        signals.append(("Sell", date))

    # RSI-based signals
    buy_rsi_signals = data[data["RSI"] < 30].index
    for date in buy_rsi_signals:
        signals.append(("Potential Buy (oversold)", date))

    sell_rsi_signals = data[data["RSI"] > 70].index
    for date in sell_rsi_signals:
        signals.append(("Potential Sell (overbought)", date))

    # MACD-based signals
    buy_macd_signals = data[data["MACD"] > data["Signal_Line"]].index
    for date in buy_macd_signals:
        signals.append(("Potential Buy (MACD)", date))

    sell_macd_signals = data[data["MACD"] < data["Signal_Line"]].index
    for date in sell_macd_signals:
        signals.append(("Potential Sell (MACD)", date))

    return signals


def save_signals_to_db(ticker, detected_signals, data):
    for signal_type, date_index in detected_signals:
        date = data.loc[date_index, "date"]

        stock_price = data.loc[date_index, "close"].item()
        volume = int(data.loc[date_index, "volume"].item())  # Convert to int

        stop_loss = stock_price * 0.97
        take_profit = stock_price * 1.03

        insert_signal_data(
            ticker, signal_type, date, stock_price, volume, stop_loss, take_profit
        )


def check_buy_sell_signals(ticker):
    data = fetch_data_from_db(ticker)
    detected_signals = detect_signals(data)
    save_signals_to_db(ticker, detected_signals, data)


if __name__ == "__main__":
    tickers = ["AAPL", "TSLA", "AMD", "F", "NVDA", "INTC", "AMZN", "CSX"]

    # Fetching Data
    print("Fetching historical stock data...")
    data_dict = fetch_historical_data(tickers, "2022-01-01", "2023-01-01")

    # Creating Tables
    print("\nCreating tables in database...")
    create_tables()

    # Inserting Data into Database
    print("\nInserting data into the database...")
    for ticker, data in data_dict.items():
        print(f"Inserting data for {ticker}...")
        insert_stock_data(ticker, data)
        print(f"Data for {ticker} inserted successfully!")

    # Calculating Metrics
    for ticker in tickers:
        print(f"Processing metrics for {ticker}")
        data = fetch_data_from_db(ticker)
        data_with_metrics = calculate_metrics(data)
        update_db_with_metrics(ticker, data_with_metrics)

    # Checking for Buy/Sell signals
    for ticker in tickers:
        print(f"Checking buy/sell signals for {ticker}...")
        check_buy_sell_signals(ticker)
        print(f"Signals for {ticker} saved to the database.")
        print("-" * 50)

    print("\nAll operations completed!")
