FROM python:3.8

# set the working directory
WORKDIR /app

# copy requirements file
COPY requirements.txt .

# install dependencies
RUN pip install -r requirements.txt

# copy the entire directory to /app
COPY . /app

# start a Python terminal and import modules
CMD ["python", "-i", "-c", "import os; from ClassTradingData import TradingData; from ClassForm4 import Form4"]
