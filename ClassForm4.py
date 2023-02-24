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


class Form4:
    # initial delay
    delay = 0
    # list to store response times
    response_times = []

    def __init__(self, cik: str, start_date: str = None, end_date: str = None) -> None:
        """
        Initializes a new instance of the Form4 class.

        Parameters:
        cik (str): The CIK number to search for.
        start_date (str, optional): The start date to filter the search results by. Must be in YYYY-MM-DD format. Defaults to None.
        end_date (str, optional): The end date to filter the search results by. Must be in YYYY-MM-DD format. Defaults to None.
        """
        base_url = "https://www.sec.gov"
        base_path = "/Archives/edgar/data/"
        self.base_url = base_url
        self.base_path = base_path
        self.cik = cik
        self.start_date = start_date
        self.end_date = end_date
        self.operation_ids = set()
        self.form4_links = set()
        self.data = []
        # set headers to simulate browser request
        self.headers = {
            "Connection": "close",
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "X-Requested-With": "XMLHttpRequest",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.163 Safari/537.36"
        }
        self.get_operation_ids()
        self.scrape_form4()

    def get_operation_ids(self) -> None:
        """
        Gets the operation IDs for the search results and saves them to the Form4 instance.
        """
        url = self.base_url + self.base_path + self.cik + '/'
        response1 = requests.get(url, headers=self.headers)
        soup1 = BeautifulSoup(response1.text, "html.parser")
        title = soup1.find('title').text.strip()
        id_try = True
        while id_try == True:
            if 'SEC.gov | Request Rate Threshold Exceeded' in title:
                print(
                    f"CIK: '{self.cik}'| Fail to scrape form 4 due to SEC.gov Request Rate Threshold Exceeded. Retrying in 60 seg.")
                time.sleep(60)
            else:
                id_try = False
            # extract operation id
            summary_text = f"Directory Listing for {self.base_path}{self.cik}"
            summary_tag = soup1.find("table", {"summary": summary_text})
            table = summary_tag.find_all("tr") if summary_tag else ""

            for row in table:
                cols = row.find_all("td")
                if len(cols) >= 2:
                    if (self.start_date != None and self.end_date != None):
                        date = cols[2].text
                        date = datetime.datetime.strptime(
                            str(date)[0:10], "%Y-%m-%d").date()
                        start_date = datetime.datetime.strptime(
                            self.start_date, "%Y-%m-%d").date()
                        end_date = datetime.datetime.strptime(
                            self.end_date, "%Y-%m-%d").date()
                        if (date >= start_date and date <= end_date):
                            ref = cols[0].find("a", href=True)
                        else:
                            ref = False
                    else:
                        ref = cols[0].find("a", href=True)
                    if ref:
                        self.operation_ids.add(ref["href"].split("/")[-1])
        print(f"CIK: '{self.cik}'| Found {len(self.operation_ids)} operations.")

    def scrape_form4(self) -> None:
        """
        Scrapes the Form 4 data for each operation ID and saves it to the Form4 instance.
        """
        delay = 0
        response_times = [0]
        progress_base = len(self.operation_ids)
        progress_i = 0
        for operation_id in self.operation_ids:
            start_time = time.time()
            progress_i += 1
            print(
                f"CIK: '{self.cik}'| Scraping progress {round((progress_i/progress_base)*100)}%")
            id_try = True
            while id_try == True:
                url = self.base_url + self.base_path + self.cik + '/' + operation_id
                # get the index page for the filing
                response2 = requests.get(url, headers=self.headers)
                soup2 = BeautifulSoup(response2.text, "html.parser")
                title = soup2.find('title').text.strip()

                if 'SEC.gov | Request Rate Threshold Exceeded' in title:
                    print(
                        f"CIK: '{self.cik}'| Fail to scrape form 4 due to SEC.gov Request Rate Threshold Exceeded. Retrying in 60 seg.")
                    time.sleep(60)
                else:
                    id_try = False

                # find the link to the filing's primary document (ends with "-index.html")
                index_link = None
                summary_text = f"Directory Listing for {self.base_path}{self.cik}/{operation_id}"
                table = soup2.find("table", {"summary": summary_text})
                if table:
                    for a in table.find_all("a", href=True):
                        if a["href"].endswith("-index.html"):
                            index_link = a["href"]
                            break

                if not index_link:
                    break

                # get the primary document page
                response3 = requests.get(
                    self.base_url + index_link, headers=self.headers)
                soup3 = BeautifulSoup(response3.text, "html.parser")

                # find the link to the FORM 4 document
                form4_link = None
                table = soup3.find(
                    "table", {"class": "tableFile", "summary": "Document Format Files"})
                if table:
                    for row in table.find_all("tr"):
                        cols = row.find_all("td")
                        if len(cols) >= 2 and "4" in cols[3].text:
                            for a in cols[2].find_all("a", href=True):
                                if a["href"].endswith(".xml"):
                                    form4_link = self.base_url + self.base_path + self.cik + \
                                        '/' + operation_id + '/' + \
                                        a["href"].split("/")[-1]
                                    self.get_form4_data(form4_link)
                                    break
                end_time = time.time()

                # adjust delay based on operation time variance
                response_times.append(end_time - start_time)
                if (len(response_times) > 3):
                    variance = statistics.variance(response_times)
                    response_times = response_times[-3:]
                else:
                    variance = statistics.variance(response_times)

                if variance > 0.5:
                    print(f"CIK: '{self.cik}'| Increase delay by 1 sec")
                    delay += 1
                elif variance < 0.4 and delay > 0:
                    print(f"CIK: '{self.cik}'| Decrease delay by 1 sec")
                    delay -= 1

                time.sleep(delay)

    def get_form4_data(self, form4_link: str) -> List[dict]:
        """
        Parses the Form 4 data and returns it as a list of dictionaries.

        Parameters:
        form4_link (str): The URL to the Form 4 filing.

        Returns:
        List[dict]: A list of dictionaries containing the Form 4 data.
        """
        response4 = requests.get(form4_link, headers=self.headers)
        soup4 = BeautifulSoup(response4.text, "lxml-xml")

        cik_file_tag = soup4.find("issuerCik")
        cik_file = cik_file_tag.text if cik_file_tag else ""

        name_tag = soup4.find("issuerName")
        name = name_tag.text if name_tag else ""

        ticker_tag = soup4.find("issuerTradingSymbol")
        ticker = ticker_tag.text if ticker_tag else ""

        rptOwnerName_tag = soup4.find("rptOwnerName")
        rptOwnerName = rptOwnerName_tag.text if rptOwnerName_tag else ""

        rptOwnerCik_tag = soup4.find("rptOwnerCik")
        rptOwnerCik = rptOwnerCik_tag.text if rptOwnerCik_tag else ""

        isDirector_tag = soup4.find("isDirector")
        isDirector = isDirector_tag.text if isDirector_tag else ""

        isOfficer_tag = soup4.find("isOfficer")
        isOfficer = isOfficer_tag.text if isOfficer_tag else ""

        isTenPercentOwner_tag = soup4.find("isTenPercentOwner")
        isTenPercentOwner = isTenPercentOwner_tag.text if isTenPercentOwner_tag else ""

        isOther_tag = soup4.find("isOther")
        isOther = isOther_tag.text if isOther_tag else ""

        officerTitle_tag = soup4.find("officerTitle")
        officerTitle = officerTitle_tag.text if officerTitle_tag else ""

        for transaction in soup4.find_all("derivativeTransaction"):
            security_title_tag = transaction.find(
                "securityTitle").find("value")
            security_title = security_title_tag.text if security_title_tag else ""

            date_tag = transaction.find("transactionDate").find("value")
            date = date_tag.text if date_tag else ""

            form_type_tag = transaction.find(
                "transactionCoding").find("transactionFormType")
            form_type = form_type_tag.text if form_type_tag else ""

            code_tag = transaction.find(
                "transactionCoding").find("transactionCode")
            code = code_tag.text if code_tag else ""

            equity_swap_tag = transaction.find(
                "transactionCoding").find("equitySwapInvolved")
            equity_swap = equity_swap_tag.text if equity_swap_tag else ""

            shares_tag = transaction.find("transactionAmounts")
            shares_tag = shares_tag.find(
                "transactionShares") if shares_tag else ""
            shares_tag = shares_tag.find("value") if shares_tag else ""
            shares = shares_tag.text if shares_tag else ""
            shares = shares if shares != "" else 0

            acquired_disposed_code_tag = transaction.find("transactionAmounts")
            acquired_disposed_code_tag = acquired_disposed_code_tag.find(
                "transactionAcquiredDisposedCode") if acquired_disposed_code_tag else ""
            acquired_disposed_code_tag = acquired_disposed_code_tag.find(
                "value") if acquired_disposed_code_tag else ""
            acquired_disposed_code = acquired_disposed_code_tag.text if acquired_disposed_code_tag else ""

            shares_owned_following_transaction_tag = transaction.find(
                "postTransactionAmounts")
            shares_owned_following_transaction_tag = shares_owned_following_transaction_tag.find(
                "sharesOwnedFollowingTransaction") if shares_owned_following_transaction_tag else ""
            shares_owned_following_transaction_tag = shares_owned_following_transaction_tag.find(
                "value") if shares_owned_following_transaction_tag else ""
            shares_owned_following_transaction = shares_owned_following_transaction_tag.text if shares_owned_following_transaction_tag else ""
            shares_owned_following_transaction = shares_owned_following_transaction if shares_owned_following_transaction != "" else 0

            direct_or_indirect_ownership_tag = transaction.find(
                "ownershipNature")
            direct_or_indirect_ownership_tag = direct_or_indirect_ownership_tag.find(
                "directOrIndirectOwnership") if direct_or_indirect_ownership_tag else ""
            direct_or_indirect_ownership_tag = direct_or_indirect_ownership_tag.find(
                "value") if direct_or_indirect_ownership_tag else ""
            direct_or_indirect_ownership = direct_or_indirect_ownership_tag.text if direct_or_indirect_ownership_tag else ""

            self.data.append({
                "cik": cik_file,
                "parent_cik": self.cik,
                "name": name,
                "ticker": ticker,
                "rptOwnerName": rptOwnerName,
                "rptOwnerCik": rptOwnerCik,
                "isDirector": isDirector,
                "isOfficer": isOfficer,
                "isTenPercentOwner": isTenPercentOwner,
                "isOther": isOther,
                "officerTitle": officerTitle,
                "security_title": security_title,
                "transaction_date": date,
                "form_type": form_type,
                "code": code,
                "equity_swap": equity_swap,
                "shares": shares,
                "acquired_disposed_code": acquired_disposed_code,
                "shares_owned_following_transaction": shares_owned_following_transaction,
                "direct_or_indirect_ownership": direct_or_indirect_ownership,
                "form4_link": form4_link
            })

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
        # stock_prices_df = stock_prices_df.reset_index()
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
                df[col_name] = col_values.apply(lambda x: round(x, 2))

        self.data = df.to_dict(orient='records')

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

    def save_to_csv(self, path: str) -> None:
        """
        Saves the Form 4 data to a CSV file.

        Parameters:
        path (str): The path and filename to save the CSV file to.
        """
        if (len(self.data) > 0):
            directory_index = path.rfind("/")
            if directory_index != -1:
                directory = path[:directory_index]
                # Check if directory exists and create it if it doesn't
                if not os.path.exists(directory):
                    os.makedirs(directory)

            form4_df = pd.DataFrame(self.data)
            form4_df.to_csv(path, sep='|', index=False)
            print(f"CIK: '{self.cik}'| Saved Form 4 data.")
        else:
            print(f"CIK: '{self.cik}'| There is not Form 4 data.")
