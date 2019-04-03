from selenium import webdriver
from bs4 import BeautifulSoup
import pandas as pd
from requests import get
import time
from time import sleep
from random import randint
from time import time
from IPython.core.display import clear_output
from warnings import warn
import datetime


# Preparing the monitoring of the loop
start_time = time()
requests = 0

klse_yahoo_codes = pd.read_csv("klse-main-market-stocks-code.csv")
klse_yahoo_codes.shape
klse_yahoo_codes.size

stock_codes = []
company_name = []

for index, row in klse_yahoo_codes.iterrows():
    # print row[0]

    # launch url
    url = 'https://finance.yahoo.com/quote/' + row[0] + '?p=' + row[0]
    print url

    # create a new Firefox session
    driver = webdriver.Chrome(executable_path='/usr/local/bin/chromedriver')
    driver.implicitly_wait(10)
    driver.get(url)

    response = get(url)

    # Pause the loop
    sleep(randint(2, 6))

    # Monitor the requests
    requests += 1
    elapsed_time = time() - start_time
    print('Request:{}; Frequency: {} requests/s'.format(requests, requests / elapsed_time))
    clear_output(wait=True)

    # Throw a warning for non-200 status codes
    if response.status_code != 200:
        warn('Request: {}; Status code: {}'.format(requests, response.status_code))

    # Break the loop if the number of requests is greater than expected
    if requests > klse_yahoo_codes.size:
        warn('Number of requests was greater than expected.')
        break

    # Parse the content of the request with BeautifulSoup
    soup_level1 = BeautifulSoup(driver.page_source, 'lxml')

    # Beautiful Soup finds all Job Title links on the agency page and the loop begins
    for link in soup_level1.find_all('div'):
        if link.find(class_="D(ib) Fz(18px)"):
            comName = link.h1.text
            stock_codes.append(row[0])
            company_name.append(comName)
            print row[0]
            print comName
            break

    driver.close()