import os
import time
import datetime
import statistics
import requests
import pandas as pd
import yfinance as yf
import plotly.graph_objects as go
import plotly.subplots as sp
import plotly.express as px
from typing import List
from bs4 import BeautifulSoup
import hashlib
import pyarrow as pa
from ClassForm4 import Form4


class TradingData:
    def __init__(self, cik: str, start_date: str = None, end_date: str = None) -> None:
        self.cik = cik
        self.data = Form4(cik, start_date, end_date).data
        if len(self.data) > 0:
            self.parquet_path = 'trading-data'
            self.add_stock_data()
            self.record_data()
        else:
            print(f"No data to save for {self.cik}")

    def add_stock_data(self) -> None:
        """
        Adds stock data to the Form 4 data and updates the Form4 instance.
        """

        df = pd.DataFrame(self.data)
        df = df[df['ticker'].notnull()]
        df['transaction_date'] = pd.to_datetime(
            df['transaction_date'], format='%Y-%m-%d')
        min_max_dates = df.groupby('ticker').agg(min_date=('transaction_date', 'min'),
                                                 max_date=('transaction_date', 'max')).reset_index()
        # create a dictionary with Ticker as the key and min/max dates as the value
        stock_date_dict = dict(zip(min_max_dates['ticker'], zip(
            min_max_dates['min_date'], min_max_dates['max_date'])))
        # create a list of unique tickers in the dataframe
        tickers = df['ticker'].unique()
        ticker_history_pd = pd.DataFrame(
            columns=['Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume', 'stock_ticker'])
        good_ticker = []
        bad_ticker = []
        # loop over each ticker to get the stock prices data from yfinance and append it to the stock prices dataframe
        for ticker in tickers:
            min_date, max_date = stock_date_dict[ticker]
            ticker_history_n = yf.download(
                ticker, start=min_date, end=max_date)
            if not ticker_history_n.empty:
                good_ticker.append(ticker)
                ticker_history_n['stock_ticker'] = ticker
                ticker_history_pd = ticker_history_pd.append(ticker_history_n)
            else:
                bad_ticker.append(ticker)
        ticker_history_pd = ticker_history_pd.reset_index()
        ticker_history_pd = ticker_history_pd.rename(
            columns={c: c.replace(' ', '_').lower() for c in ticker_history_pd.columns})
        stock_prices_df = pd.DataFrame(ticker_history_pd)
        stock_prices_df['date'] = pd.to_datetime(
            stock_prices_df['index'], format='%Y-%m-%d')

        stock_prices_df = stock_prices_df.drop(['index'], axis=1)

        df = pd.merge(df, stock_prices_df, how='left', left_on=[
            'ticker', 'transaction_date'], right_on=['stock_ticker', 'date'])

        df = df.drop(['date', 'stock_ticker'], axis=1)

        df['daily_return'] = (df['close'].astype(
            float) - df['open'].astype(float)) / df['open'].astype(float)
        df['percent_change'] = df['daily_return'].astype(float) * 100
        df['range'] = df['high'].astype(float) - df['low'].astype(float)
        df['average_price'] = (df['high'].astype(
            float) + df['low'].astype(float)) / 2

        df['shares_value_usd'] = df['average_price'].astype(float) * \
            df['shares'].astype(float)
        for col_name, col_values in df.iteritems():
            if col_values.dtype == float:
                df[col_name] = col_values.apply(lambda x: round(x, 4))

        self.data = df.to_dict(orient='records')

    @ staticmethod
    def generate_hash(pd_df):

        # Remove the original index column from the DataFrame
        pd_df = pd_df.reset_index(drop=True)

        # Sort the columns in the DataFrame before computing the hash
        pd_df = pd_df.sort_index(axis=1)

        # Generate a new column with a concatenated string and a SHA-2 hash for each row
        pd_df['hash'] = pd_df.apply(lambda row: hashlib.sha256(
            str(row.values).encode('utf-8')).hexdigest(), axis=1)

        return pd_df

    def record_data(self):
        df = pd.DataFrame(self.data)
        # Define a dictionary with the data types for each column
        schema = {
            'cik': 'int',
            'parent_cik': 'int',
            'name': 'string',
            'ticker': 'string',
            'rptOwnerName': 'string',
            'rptOwnerCik': 'string',
            'isDirector': 'bool',
            'isOfficer': 'bool',
            'isTenPercentOwner': 'bool',
            'isOther': 'bool',
            'officerTitle': 'string',
            'security_title': 'string',
            'transaction_date': 'string',
            'form_type': 'int',
            'code': 'string',
            'equity_swap': 'float',
            'shares': 'float',
            'acquired_disposed_code': 'string',
            'shares_owned_following_transaction': 'float',
            'direct_or_indirect_ownership': 'string',
            'form4_link': 'string',
            'open': 'float',
            'high': 'float',
            'low': 'float',
            'close': 'float',
            'adj_close': 'float',
            'volume': 'float',
            'daily_return': 'float',
            'percent_change': 'float',
            'range': 'float',
            'average_price': 'float',
            'shares_value_usd': 'float',
        }

        # Loop over the columns in the dictionary and convert their data types
        for col, dtype in schema.items():
            if col in df.columns:
                df[col] = df[col].astype(dtype)

        # Call the generate_hash method on the class itself, not on an instance of the class
        df = TradingData.generate_hash(df)
        # Select only the unique rows based on the 'hash' column
        df = df.drop_duplicates(subset=['hash'])

        print(f"Incoming df: {len(df)}")
        # Check if the Parquet file already exists
        if os.path.isdir(self.parquet_path):

            pa_schema = pa.schema([
                pa.field('cik', pa.int64()),
                pa.field('parent_cik', pa.int64()),
                pa.field('name', pa.string()),
                pa.field('ticker', pa.string()),
                pa.field('rptOwnerName', pa.string()),
                pa.field('rptOwnerCik', pa.string()),
                pa.field('isDirector', pa.bool_()),
                pa.field('isOfficer', pa.bool_()),
                pa.field('isTenPercentOwner', pa.bool_()),
                pa.field('isOther', pa.bool_()),
                pa.field('officerTitle', pa.string()),
                pa.field('security_title', pa.string()),
                pa.field('transaction_date', pa.string()),
                pa.field('form_type', pa.int64()),
                pa.field('code', pa.string()),
                pa.field('equity_swap', pa.float64()),
                pa.field('shares', pa.float64()),
                pa.field('acquired_disposed_code', pa.string()),
                pa.field('shares_owned_following_transaction', pa.float64()),
                pa.field('direct_or_indirect_ownership', pa.string()),
                pa.field('form4_link', pa.string()),
                pa.field('open', pa.float64()),
                pa.field('high', pa.float64()),
                pa.field('low', pa.float64()),
                pa.field('close', pa.float64()),
                pa.field('adj_close', pa.float64()),
                pa.field('volume', pa.float64()),
                pa.field('daily_return', pa.float64()),
                pa.field('percent_change', pa.float64()),
                pa.field('range', pa.float64()),
                pa.field('average_price', pa.float64()),
                pa.field('shares_value_usd', pa.float64()),
                pa.field('hash', pa.string()),
            ])

            # Read the existing data from the Parquet file
            existing_df = pd.read_parquet(
                path=self.parquet_path, engine='pyarrow', schema=pa_schema)
            print(f"Existing df: {len(existing_df)}")
            df = df[~df['hash'].isin(existing_df['hash'])].dropna()

        # Write the DataFrame to a directory-based Parquet file
        if len(df) > 0:
            print(f"New df: {len(df)}")
            # Remove the original index column from the DataFrame
            df = df.reset_index(drop=True)
            df.to_parquet(self.parquet_path, partition_cols=[
                          'parent_cik'], engine='pyarrow')

            # Read the previous DataFrame from a Parquet file
            prev_df = pd.read_parquet(self.parquet_path)

            # Filter the DataFrame to keep only rows with a matching parent CIK
            prev_df = prev_df[prev_df['parent_cik'] == self.cik]

            # Convert the 'transaction_date' column to a datetime format
            prev_df['transaction_date'] = pd.to_datetime(
                prev_df['transaction_date'], format='%Y-%m-%d')

            # Filter the DataFrame to keep only rows within the specified date range
            mask = (prev_df['transaction_date'] >= self.start_date) & (
                prev_df['transaction_date'] <= self.end_date)
            prev_df = prev_df.loc[mask]

            # Reorder the columns of the previous DataFrame to match the current DataFrame
            prev_df = prev_df[df.columns]

            # Concatenate the current DataFrame with the filtered previous DataFrame
            df = pd.concat([df, prev_df], ignore_index=True)

            # Convert the resulting DataFrame to a list of dictionaries
            self.data = df.to_dict(orient='records')

        else:
            print(f"New df: {len(df)}")

    def inside_traiding_impact_plot(self) -> None:
        """
        Generates a plot of the inside trading impact over time.
        """
        df = pd.DataFrame(self.data)
        # Calculate the total inside trading volume for each day
        df = df.groupby("transaction_date").agg({"shares_value_usd": "sum"}).rename(
            columns={"shares_value_usd": "inside_trading_volume"})

        df = df.sort_values(by='transaction_date', ascending=True)

        # Create figure with secondary y-axis
        fig = sp.make_subplots(specs=[[{"secondary_y": True}]])

        # Add traces
        fig.add_trace(
            go.Scatter(x=df["transaction_date"],
                       y=df["inside_trading_volume"], name="Inside Trading Volume"),
            secondary_y=False,
        )

        fig.add_trace(
            go.Scatter(x=df["transaction_date"],
                       y=df["close"], name="Stock Closing Price"),
            secondary_y=True,
        )

        # Add figure title
        fig.update_layout(
            title_text=f"Inside Trading Volume and Stock Closing Price Over Time"
        )

        # Set x-axis title
        fig.update_xaxes(title_text="Date")

        # Set y-axes titles
        fig.update_yaxes(title_text="Inside Trading Volume", secondary_y=False)
        fig.update_yaxes(title_text="Stock Closing Price", secondary_y=True)

        fig.show()
