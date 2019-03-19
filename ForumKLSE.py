from selenium import webdriver
from bs4 import BeautifulSoup
import re
import pandas as pd
from tabulate import tabulate
import os
from requests import get
from time import time
import time
from time import sleep
from random import randint
from time import time
# from IPython.core.display import clear_output
from warnings import warn
from IPython.core.display import clear_output
warn("Warning Simulation")

# create a new Firefox session
driver = webdriver.Chrome(executable_path='/usr/local/bin/chromedriver')
driver.implicitly_wait(30)
driver.get("https://klse.i3investor.com/servlets/forum/800003369.jsp?fp=46")

link_list_df = pd.read_csv('forum03_14_2019.csv')


stocks_name = []
comIds = []
comDts = []
comments = []


# Preparing the monitoring of the loop
start_time = time()
requests = 0

for forum_link in link_list_df.forum_links:
    if str(forum_link).startswith('/'):
        print 'https://klse.i3investor.com/' + str(forum_link)
        # Make a get request
        response = get('https://klse.i3investor.com' + str(forum_link))

        # Pause the loop
        sleep(randint(2, 6))

        requests += 1
        elapsed_time = time() - start_time
        print('Request:{}; Frequency: {} requests/s'.format(requests, requests / elapsed_time))
        clear_output(wait=True)

        # Throw a warning for non-200 status codes
        if response.status_code != 200:
            warn('Request: {}; Status code: {}'.format(requests, response.status_code))

        # Break the loop if the number of requests is greater than expected
        if requests > 100:
            warn('Number of requests was greater than expected.')
            break

        # Parse the content of the request with BeautifulSoup
        page_html = BeautifulSoup(driver.page_source, 'lxml')

        # Select all the 50 movie containers from a single page
        trs = page_html.find_all('tr')

        for tr in trs:
            # if the tr tag have forum
            if tr.find(class_='postcount') is not None:
                print forum_link
                print tr.find('span', class_='comuid').text
                print tr.find('span', class_='comdt').text
                print tr.find('span', class_='autolink').text

                stocks_name.append(str(forum_link))
                comIds.append(str(tr.find('span', class_='comuid').text))
                comDts.append(str(tr.find('span', class_='comdt').text))
                comments.append(str(tr.find('span', class_='autolink').text))
        print 'next line \n'

    # else:
    #     print 'no' + forum_link
