import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import plotly.graph_objs as go
import plotly.subplots as sp
import pandas as pd
from dash import dash_table
from sqlalchemy import create_engine

# Replace DATABASE_URL with your actual PostgreSQL database URL
DATABASE_URL = "postgresql://postgres:admin@localhost:5432/stock_data"

app = dash.Dash(__name__)

engine = create_engine(DATABASE_URL)


def fetch_data_from_db(ticker):
    # Use SQLAlchemy engine to connect to the database
    conn = engine.connect()

    # Use pandas read_sql to fetch data from the database
    query = f"SELECT * FROM stocks WHERE ticker = '{ticker}' ORDER BY date"
    df = pd.read_sql(query, conn)

    conn.close()

    return df


# Fetch signal data from the database
def fetch_signal_data():
    # Use SQLAlchemy engine to connect to the database
    conn = engine.connect()

    # Use pandas read_sql to fetch data from the database
    signal_query = "SELECT * FROM signals"
    signal_df = pd.read_sql(signal_query, conn)

    conn.close()

    return signal_df


app.layout = html.Div(
    [
        html.Div(
            [
                html.H1(
                    "Stock Data",
                    style={"textAlign": "center", "padding": "1em"},
                )
            ]
        ),
        html.Div(
            [
                dcc.Dropdown(
                    id="stock-dropdown",
                    options=[
                        {"label": ticker, "value": ticker}
                        for ticker in [
                            "AAPL",
                            "TSLA",
                            "AMD",
                            "F",
                            "NVDA",
                            "INTC",
                            "AMZN",
                            "CSX",
                        ]
                    ],
                    value=["AAPL"],
                    multi=True,
                    style={"width": "50%", "margin": "0 auto"},
                )
            ],
            style={"padding": "1em"},
        ),
        # Larger Graph
        dcc.Graph(
            id="stock-graph",
            config={"displayModeBar": False},
            style={"height": "800px", "width": "100%"},
        ),
        # Signal Table
        html.Div(
            [
                html.H2("Signal Data", style={"textAlign": "center"}),
                dash_table.DataTable(
                    id="signal-table",
                    columns=[
                        {"name": col, "id": col} for col in fetch_signal_data().columns
                    ],
                    data=fetch_signal_data().to_dict("records"),
                    style_table={
                        "overflowX": "scroll",
                        "width": "80%",
                        "margin": "0 auto",
                    },
                    style_cell={"textAlign": "center"},
                ),
            ]
        ),
        # Stock Table
        html.Div(
            [
                html.H2("Stock Data", style={"textAlign": "center"}),
                dash_table.DataTable(
                    id="stock-table",
                    columns=[
                        {"name": col, "id": col}
                        for col in fetch_data_from_db("AAPL").columns
                    ],
                    data=fetch_data_from_db("AAPL").to_dict("records"),
                    style_table={
                        "overflowX": "scroll",
                        "width": "80%",
                        "margin": "0 auto",
                    },
                    style_cell={"textAlign": "center"},
                ),
            ]
        ),
    ],
    style={"fontFamily": "Arial", "padding": "2em"},
)


@app.callback(Output("stock-graph", "figure"), [Input("stock-dropdown", "value")])
def update_graph(selected_tickers):
    fig = sp.make_subplots(
        rows=5,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.05,
        subplot_titles=(
            "Opening Price",
            "Closing Price",
            "Volume",
            "RSI",
            "MACD vs Signal Line",
        ),
    )

    for selected_ticker in selected_tickers:
        df = fetch_data_from_db(selected_ticker)

        fig.add_trace(
            go.Scatter(
                x=df["date"],
                y=df["open"],
                name=f"Open Price ({selected_ticker})",
                mode="lines",
            ),
            row=1,
            col=1,
        )
        fig.add_trace(
            go.Scatter(
                x=df["date"],
                y=df["close"],
                name=f"Close Price ({selected_ticker})",
                mode="lines",
            ),
            row=2,
            col=1,
        )
        fig.add_trace(
            go.Bar(x=df["date"], y=df["volume"], name=f"Volume ({selected_ticker})"),
            row=3,
            col=1,
        )
        fig.add_trace(
            go.Scatter(
                x=df["date"], y=df["RSI"], name=f"RSI ({selected_ticker})", mode="lines"
            ),
            row=4,
            col=1,
        )
        fig.add_trace(
            go.Scatter(
                x=df["date"],
                y=df["MACD"],
                name=f"MACD ({selected_ticker})",
                mode="lines",
            ),
            row=5,
            col=1,
        )
        fig.add_trace(
            go.Scatter(
                x=df["date"],
                y=df["Signal_Line"],
                name=f"Signal Line ({selected_ticker})",
                mode="lines",
            ),
            row=5,
            col=1,
        )

    return fig


if __name__ == "__main__":
    app.run_server(debug=True)
