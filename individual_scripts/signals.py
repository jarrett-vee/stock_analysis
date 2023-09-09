import numpy as np
import pandas as pd
from individual_scripts.fetch_data import fetch_data_from_db
from db_manager import insert_signal_data


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


if __name__ == "__main__":
    tickers = ["AAPL", "TSLA", "AMD", "F", "NVDA", "INTC", "AMZN", "CSX"]

    for ticker in tickers:
        data = fetch_data_from_db(ticker)
        detected_signals = detect_signals(data)

        save_signals_to_db(
            ticker, detected_signals, data
        )  # Pass the data to the function

        print(f"Signal for {ticker} saved to the database.")
        print("-" * 50)
