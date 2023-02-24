# SEC-Insider-Trading-Analyzer

This project is a Python package for scraping and extracting data from SEC Form 4 filings. The package provides a `Form4` class that can be used to scrape Form 4 filings for a given company's CIK code.

## Table of Contents

- [Installation](#installation)
- [Usage](#usage)
- [Reference](#reference)
- [Contributing](#contributing)
- [License](#license)


## Prerequisites

- Docker installed on your machine

## Setup

To use the Form4 Scraper, follow these steps:

1. Clone this repository to your local machine.

2. Navigate to the project directory in your terminal.

3. Build the Docker image by running the following command:

    `docker build -t form4-scraper .`

4. Once the image is built, start a Docker container and access the bash terminal inside the container with the following command:

    `docker run -it form4-scraper /bin/bash`

5. You are now inside the Docker container and can interact with the Form4 class. You can run your Python script or open the Python REPL to experiment with the class methods.

6. When you're finished, exit the container by typing `exit`.
 
## Reference

### ClassForm4

#### Methods

- `get_operation_ids(self) -> None:`
    Scrapes the operation IDs for the search results and saves them to the Form4 instance.
- `get_form4_links(self) -> List[str]:`
    Retrieves a list of links to Form 4 filings for the specified CIK and time period.
- `get_form4_data(self, form4_link: str) -> List[dict]:`
    Scrapes the data from a single Form 4 filing and returns a list of dictionaries.
- `add_stock_data(self) -> None:`
    Adds stock data (e.g. closing prices) to each Form 4 filing in the object's `form4_data` list.
- `inside_traiding_impact_plot(self) -> None:`
    Generates a plot of the inside trading impact over time.   
- `save_to_csv(self, filename: str) -> None:`
    Saves the object's `form4_data` list to a CSV file.

## Example Usage

```python
from ClassForm4 import Form4

cik = '123456789'
start_date = '2020-01-01'
end_date = '2022-12-31'

form4 = Form4(cik, start_date=start_date, end_date=end_date)
form4.add_stock_data()
form4.save_to_csv('form4_data.csv')
```
## License
This project is licensed under the [MIT License](https://opensource.org/license/mit/).