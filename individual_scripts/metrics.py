import pandas as pd
from db_manager import session_scope, Stock


def fetch_data_from_db(ticker):
    with session_scope() as session:
        data = (
            session.query(Stock)
            .filter(Stock.ticker == ticker)
            .order_by(Stock.date)
            .all()
        )
        df = pd.DataFrame([(d.date, d.close) for d in data], columns=["Date", "Close"])
    return df


def calculate_metrics(df):
    # Calculate moving averages
    df["10_day_MA"] = df["Close"].rolling(window=10).mean()
    df["50_day_MA"] = df["Close"].rolling(window=50).mean()
    df["200_day_MA"] = df["Close"].rolling(window=200).mean()

    # Calculate RSI
    delta = df["Close"].diff()
    gain = (delta.where(delta > 0, 0)).fillna(0)
    loss = (-delta.where(delta < 0, 0)).fillna(0)
    avg_gain = gain.rolling(window=14).mean()
    avg_loss = loss.rolling(window=14).mean()
    rs = avg_gain / avg_loss
    df["RSI"] = 100 - (100 / (1 + rs))

    # Calculate MACD and Signal Line
    df["12_day_EMA"] = df["Close"].ewm(span=12, adjust=False).mean()
    df["26_day_EMA"] = df["Close"].ewm(span=26, adjust=False).mean()
    df["MACD"] = df["12_day_EMA"] - df["26_day_EMA"]
    df["Signal_Line"] = df["MACD"].ewm(span=9, adjust=False).mean()

    return df


def update_db_with_metrics(ticker, df):
    with session_scope() as session:
        for _, row in df.iterrows():
            stock = (
                session.query(Stock)
                .filter(Stock.ticker == ticker, Stock.date == row["Date"])
                .first()
            )
            if stock:
                stock.ten_day_MA = row["10_day_MA"]
                stock.fifty_day_MA = row["50_day_MA"]
                stock.two_hundred_day_MA = row["200_day_MA"]
                stock.RSI = row["RSI"]
                stock.MACD = row["MACD"]
                stock.Signal_Line = row["Signal_Line"]

                # Add other indicators as needed


if __name__ == "__main__":
    tickers = ["AAPL", "TSLA", "AMD", "F", "NVDA", "INTC", "AMZN", "CSX"]

    for ticker in tickers:
        print(f"Processing metrics for {ticker}")
        data = fetch_data_from_db(ticker)
        data_with_metrics = calculate_metrics(data)
        update_db_with_metrics(ticker, data_with_metrics)

    print("\nAll operations completed!")
