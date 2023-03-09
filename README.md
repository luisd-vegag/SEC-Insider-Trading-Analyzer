# SEC-Insider-Trading-Analyzer

This project is a Python package for scraping and extracting data from SEC Form 4 filings, adding stock data for each transacction, and visualization tools. The package provides the clases `Form4`, and `TradingData` that can be used to work with Form 4 filings transaction data for a given company's CIK code. The packege also include a `main.py` file to enble parallel data extraction for both classes.


## Table of Contents

- [Installation](#installation)
- [Usage](#usage)
- [References](#references)
- [Contributing](#contributing)
- [License](#license)


## Prerequisites

- Docker installed on your machine

## Setup

To use the packege to extract data, follow these steps:

1. Clone this repository to your local machine.

2. Navigate to the project directory in your terminal.

3. Build the Docker image by running the following command:

    `docker build -t sec-app .`

4. Once the image is built, start a Docker container and access the python envoirment inside the container with the following command:

    `docker run -it -v $(pwd):/app sec-app`

5. When you're finished, exit the container by typing `exit()`.
 
## References

### ClassForm4

#### Instance Parameters
- `cik: str`

    With or without leading zeros.

- `start_date: str = None`

    Format 'yyyy-MM-dd'

- `end_date: str = None`

    Format 'yyyy-MM-dd'

- `days_range: int = 0`

#### Instance Attributes
- `self.form4`
    Returns the form4 filings data from the given date range as a list of dictionaries.

#### Methods
- `save_to_csv(self, path: str = 'data/saved_form4_date.csv') -> None:`
    Saves the object's `form4.data` attribute list to a CSV file.

#### Example Usage
Run `python`
```python
from ClassForm4 import Form4

cik = '1318605'
start_date = '2020-01-01'
end_date = '2022-12-31'
days_range = 0

tesla = Form4(cik, start_date=start_date, end_date=end_date, days_range =days_range)

tesla.save_to_csv('tesla_data.csv')
```


### ClassTradingData

#### Instance Parameters
- `cik: str`

    With or without leading zeros.

- `start_date: str = None`

    Format 'yyyy-MM-dd'

- `end_date: str = None`

    Format 'yyyy-MM-dd'

- `days_range: int = 0`

#### Instance Attributes
- `data`
    Returns the form4 filings data and stock data from the given date range as a list of dictionaries.

#### Methods
- `stacked_bar_acquired_disposed_by_insider: self`
    Generate a stacked bar chart showing the total number of shares acquired (A) and disposed (D) by each insider.
- `stacked_bar_insider_ownership: self`
    Generates a stacked bar chart showing the direct and indirect ownership of insiders in a company over a given period of time.
- `plot_inside_trading_impact: self`
    Generates a plot of the inside trading impact over time showing the total number of shares acquired (A) and disposed (D) and the closing shares price.


#### Example Usage
Run `python`
```python
from ClassTradingData import TradingData

cik = '1318605'
start_date = '2020-01-01'
end_date = '2022-12-31'
days_range = 0

tesla = TradingData(cik, start_date=start_date, end_date=end_date, days_range=days_range)
tesla.stacked_bar_acquired_disposed_by_insider()
tesla.stacked_bar_insider_ownership()
tesla.plot_inside_trading_impact()

```

## License
This project is licensed under the [MIT License](https://opensource.org/license/mit/).
