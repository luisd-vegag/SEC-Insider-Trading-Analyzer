import os
import pandas as pd
import yfinance as yf
import plotly.graph_objects as go
import plotly.subplots as sp
from typing import List
import pyarrow as pa
from ClassForm4 import Form4


class TradingData:
    def __init__(self, cik: str, start_date: str = None, end_date: str = None) -> None:
        self.cik = cik
        self.form4 = Form4(cik, start_date, end_date)
        self.data = self.form4.data
        pd.set_option('display.max_columns', None)
        pd.set_option('display.max_rows', None)

        if len(self.data) > 0:
            self.parquet_path = 'system/trading-data'
            self.add_stock_data()
            self.record_data()
        else:
            print(f"No data to save for {self.cik}")

    @ staticmethod
    def add_close_market_days(stock_prices_df):
        # loop over each stock ticker in the DataFrame
        filled_dfs = []
        fill_cols = ['open', 'high', 'low', 'close', 'adj_close']
        for ticker in stock_prices_df['stock_ticker'].unique():
            ticker_df = stock_prices_df[stock_prices_df['stock_ticker'] == ticker].copy(
            )

            # create a DataFrame with all dates between the first and last dates in the DataFrame
            date_range = pd.date_range(
                ticker_df['date'].iloc[0], ticker_df['date'].iloc[-1], freq='D')
            all_dates_df = pd.DataFrame({'date': date_range})

            # merge the all_dates_df with the ticker_df to fill in missing dates
            merged_df = pd.merge(all_dates_df, ticker_df,
                                 on='date', how='left')

            # fill in missing values for specified columns with the previous or next day's close price
            # !! NOT WORKING AS INTENDENT. THE VALUES ARE NOT USING THE CLOSE PRICE
            merged_df[fill_cols] = merged_df.groupby('stock_ticker')[fill_cols].fillna(
                method='ffill').fillna(method='bfill')

            merged_df['volume'] = 0
            merged_df['stock_ticker'] = ticker
            # append the filled DataFrame for this ticker to the list of filled DataFrames
            filled_dfs.append(merged_df)

        # concatenate the filled DataFrames for each ticker
        filled_stock_prices_df = pd.concat(filled_dfs)

        # sort the final DataFrame by stock ticker and date
        filled_stock_prices_df = filled_stock_prices_df.sort_values(
            ['stock_ticker', 'date'])

        return filled_stock_prices_df

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
        stock_prices_df = pd.DataFrame(
            columns=['Date', 'Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume', 'stock_ticker'])
        good_ticker = []
        bad_ticker = []
        # loop over each ticker to get the stock prices data from yfinance and append it to the stock prices dataframe
        for ticker in tickers:
            min_date, max_date = stock_date_dict[ticker]
            # Holidays and weekends missing
            ticker_history_n = yf.download(
                ticker, start=min_date, end=max_date)

            if not ticker_history_n.empty:
                good_ticker.append(ticker)
                ticker_history_n['stock_ticker'] = ticker
                ticker_history_n.reset_index(inplace=True)
                stock_prices_df = pd.concat(
                    [stock_prices_df, ticker_history_n], ignore_index=True)
            else:
                bad_ticker.append(ticker)

        if not stock_prices_df.empty:
            stock_prices_df = stock_prices_df.rename(
                columns={c: c.replace(' ', '_').lower() for c in stock_prices_df.columns})

            stock_prices_df['date'] = pd.to_datetime(
                stock_prices_df['date'], format='%Y-%m-%d')

            stock_prices_df = TradingData.add_close_market_days(
                stock_prices_df)

            df['transaction_date'] = pd.to_datetime(
                df['transaction_date'], format='%Y-%m-%d')

            df = pd.merge(df, stock_prices_df, how='left', left_on=[
                'ticker', 'transaction_date'], right_on=['stock_ticker', 'date'])

            df = df.drop(['date', 'stock_ticker'], axis=1)

            df['daily_return'] = (df['close'].astype(
                float) - df['open'].astype(float)) / df['open'].astype(float)
            df['percent_change'] = df['daily_return'].astype(float) * 100
            df['range'] = df['high'].astype(
                float) - df['low'].astype(float)
            df['average_price'] = (df['high'].astype(
                float) + df['low'].astype(float)) / 2

            df['shares_value_usd'] = df['average_price'].astype(float) * \
                df['shares'].astype(float)
            for col_name, col_values in df.items():
                if col_values.dtype == float:
                    df[col_name] = col_values.apply(lambda x: round(x, 4))

            self.data = df.to_dict(orient='records')

    def record_data(self):

        df = pd.DataFrame(self.data)
        # Define a dictionary with the data types for each column
        schema = {
            'cik': 'Int64',
            'parent_cik': 'Int64',
            'name': 'string',
            'ticker': 'string',
            'rptOwnerName': 'string',
            'rptOwnerCik': 'string',
            'isDirector': 'boolean',
            'isOfficer': 'boolean',
            'isTenPercentOwner': 'boolean',
            'isOther': 'boolean',
            'officerTitle': 'string',
            'security_title': 'string',
            'transaction_date': 'datetime64[ns]',
            'form_type': 'string',
            'code': 'string',
            'equity_swap': 'float64',
            'shares': 'float64',
            'acquired_disposed_code': 'string',
            'shares_owned_following_transaction': 'float64',
            'direct_or_indirect_ownership': 'string',
            'form4_link': 'string',
            'open': 'float64',
            'high': 'float64',
            'low': 'float64',
            'close': 'float64',
            'adj_close': 'float64',
            'volume': 'float64',
            'daily_return': 'float64',
            'percent_change': 'float64',
            'range': 'float64',
            'average_price': 'float64',
            'shares_value_usd': 'float64',
            'hash': 'string'
        }

        # Loop over the columns in the dictionary and convert their data types
        for col, dtype in schema.items():
            if col in df.columns:
                df[col] = df[col].astype(dtype)

        # Check if the Parquet file already exists
        if os.path.isdir(self.parquet_path + '/parent_cik=' + self.cik):
            pa_schema = pa.schema([
                pa.field('parent_cik', pa.int64()),
                pa.field('hash', pa.string()),
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
            ])

            # Read the existing data from the Parquet file
            existing_df = pd.read_parquet(
                path=self.parquet_path, engine='pyarrow', schema=pa_schema)
            print(f"Existing df: {len(existing_df)}")
            df = df[~df['hash'].isin(existing_df['hash'])].dropna()

        df = df[['parent_cik',
                'hash',
                 'open',
                 'high',
                 'low',
                 'close',
                 'adj_close',
                 'volume',
                 'daily_return',
                 'percent_change',
                 'range',
                 'average_price',
                 'shares_value_usd'
                 ]]

        df.to_parquet(self.parquet_path, partition_cols=[
            'parent_cik'], engine='pyarrow')

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
