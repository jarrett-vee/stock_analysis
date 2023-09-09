from sqlalchemy import create_engine, Column, Integer, String, Date, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from contextlib import contextmanager
import traceback

DATABASE_URL = "postgresql://postgres:admin@localhost:5432/stock_data"

Base = declarative_base()

engine = create_engine(DATABASE_URL, echo=True)
SessionLocal = sessionmaker(bind=engine)


class Signal(Base):
    __tablename__ = "signals"

    id = Column(Integer, primary_key=True, index=True)
    ticker = Column(String, index=True)
    signal_type = Column(String)
    date = Column(Date)
    stock_price = Column(Float)
    volume = Column(Float)
    stop_loss = Column(Float)
    take_profit = Column(Float)


class Stock(Base):
    __tablename__ = "stocks"

    id = Column(Integer, primary_key=True, index=True)
    ticker = Column(String, index=True)
    date = Column(Date)
    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float)
    volume = Column(Integer)
    dividends = Column(Float)
    stock_splits = Column(Float)
    ten_day_MA = Column(Float)
    fifty_day_MA = Column(Float)
    two_hundred_day_MA = Column(Float)
    RSI = Column(Float)
    MACD = Column(Float)
    Signal_Line = Column(Float)


@contextmanager
def session_scope():
    """Provide a transactional scope around a series of operations."""
    db_session = SessionLocal()
    try:
        yield db_session
        db_session.commit()
    except Exception as e:
        print("Error occurred:", str(e))
        print(traceback.format_exc())
        db_session.rollback()
    finally:
        db_session.close()


def create_tables():
    Base.metadata.create_all(bind=engine)


def fetch_signals(db_session):
    return db_session.query(Signal).order_by(Signal.date.asc()).all()


def insert_stock_data(ticker, data_frame):
    data_frame.reset_index(inplace=True)
    data_frame["ticker"] = ticker
    entries = []

    for _, row in data_frame.iterrows():
        stock_entry = Stock(
            ticker=row["ticker"],
            date=row["Date"].date(),
            open=row["Open"],
            high=row["High"],
            low=row["Low"],
            close=row["Close"],
            volume=int(row["Volume"]),
            dividends=row["Dividends"],
            stock_splits=row["Stock Splits"],
        )
        entries.append(stock_entry)

    with session_scope() as db_session:
        db_session.bulk_save_objects(entries)
        print(f"Inserted {len(entries)} entries for {ticker} successfully!")


def update_stock_metrics(ticker, date, ten_day_ma, fifty_day_ma, two_hundred_day_ma):
    with session_scope() as db_session:
        record = (
            db_session.query(Stock)
            .filter(Stock.ticker == ticker, Stock.date == date)
            .first()
        )
        if record:
            record.ten_day_MA = ten_day_ma
            record.fifty_day_MA = fifty_day_ma
            record.two_hundred_day_MA = two_hundred_day_ma
            db_session.add(record)
            print(f"Updated metrics for {ticker} on {date}")


def insert_signal_data(
    ticker, signal_type, date, stock_price, volume, stop_loss, take_profit, comment=None
):
    with session_scope() as db_session:
        signal_entry = Signal(
            ticker=ticker,
            signal_type=signal_type,
            date=date,
            stock_price=stock_price,
            volume=volume,
            stop_loss=stop_loss,
            take_profit=take_profit,
        )
        db_session.add(signal_entry)
        db_session.commit()
        print(f"Inserted signal '{signal_type}' for {ticker} on {date}")
