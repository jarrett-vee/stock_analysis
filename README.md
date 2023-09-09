# Stock Analysis Dashboard

This is a Python-based web application for analyzing and visualizing stock data. It fetches historical stock data, calculates various metrics (e.g., moving averages, RSI, MACD), and identifies buy/sell signals based on predefined strategies.


## Features

- **Data Fetching:** Fetch historical stock data from Yahoo Finance using the `yfinance` library.
- **Database Storage:** Store fetched stock data and detected signals in a PostgreSQL database.
- **Metrics Calculation:** Calculate moving averages (10-day, 50-day, 200-day), RSI, MACD, and Signal Line.
- **Signal Detection:** Identify buy and sell signals based on moving average crossovers, RSI, and MACD.
- **Interactive Dashboard:** Visualize stock data and signals using Dash.
- **Database Management:** Use SQLAlchemy to manage the database connection.

### Installation

1. Clone the repository:

   ```bash
   git clone https://github.com/jarrett-vee/stock-data-analysis-dashboard.git

2.  I kept in the individual scripts as personal preference but you can simply run main.py followed by app.py - the individual scripts are not needed.
