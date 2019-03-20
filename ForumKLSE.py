from selenium import webdriver
from bs4 import BeautifulSoup
import re
import sys
import pandas as pd
from requests import get
import time
from time import sleep
from random import randint
from time import time
from IPython.core.display import clear_output
from warnings import warn
import datetime


pages = [str(i) for i in range(1,50)]
stock_names = []
forum_links = []
posts = []
forum_1_cmt = []
last_posts = []

# Preparing the monitoring of the loop
start_time = time()
requests = 0

# For pages
for page in pages:
    # Make a get request


    # launch url
    url = 'https://klse.i3investor.com/jsp/scl/forum.jsp?fp=' + page + '&c=1'

    # create a new Firefox session
    driver = webdriver.Chrome(executable_path='/usr/local/bin/chromedriver')
    driver.implicitly_wait(30)
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
    if requests > 50:
        warn('Number of requests was greater than expected.')
        break

    # Parse the content of the request with BeautifulSoup
    page_html = BeautifulSoup(driver.page_source, 'lxml')

    # Select all the 50 movie containers from a single page
    trs = page_html.find_all('tr')

    for tr in trs:
        # if the tr tag have forum
        if tr.find(class_='ficon ftst') is not None and tr.find(class_="forumtopic"):

            # Scrape the stock
            stock_name = tr.p.a.text
            print stock_name
            stock_names.append(stock_name)

            # Scrape href
            forum_link = tr.p.a['href']
            print forum_link
            forum_links.append(forum_link)

            # Scrape 1st comment
            comment = tr.find('span', class_='comtextindex autolink').text
            print comment
            forum_1_cmt.append(comment)

            # Scrape number of posts
            post = tr.find('td', class_='highlight').text
            print post
            posts.append(post)

            # Scrape last post
            last_post = tr.find('td', class_='graydate').text
            # thisDate = last_post.replace(',', ' 2019')
            # date_time_obj = datetime.datetime.strptime(thisDate, '%b %d %Y %I:%M%p')
            # print date_time_obj.date() == datetime.datetime.now().date()
            last_posts.append(last_post)

    # driver.implicitly_wait(30)
    driver.close()

# test data frame
forum_df = pd.DataFrame({'stock_names': stock_names,
                        'forum_links': forum_links,
                        'first_cmt': forum_1_cmt,
                         'posts': posts,
                         'last_posts': last_posts})

print forum_df.info()
forum_df.head(10)

from datetime import datetime
now = datetime.now()
date_time = now.strftime("%m_%d_%Y")
print("date and time:",date_time)

forum_df.to_csv('forum' + str(date_time) + '.csv', index=False, encoding='utf-8')



# date_time_str = 'Mar 20 2019 10:41PM'
# date_time_obj = datetime.datetime.strptime(date_time_str, '%b %d %Y %I:%M%p')
#
# print date_time_obj.date()
#
# print('Date:', date_time_obj.date())
# print('Time:', date_time_obj.time())
# print('Date-time:', date_time_obj)
#
# now = datetime.datetime.now()
# print now.date()
#
# now < date_time_obj
# date_time_obj < now
