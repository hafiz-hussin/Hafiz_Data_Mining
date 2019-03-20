# Author by Peter
# Hafiz -- add Postgrest

import requests
from xml.etree import ElementTree as ET
import time
from unidecode import unidecode

# postgrest
from sqlalchemy import create_engine
from sqlalchemy import Column, String, Integer, Sequence, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.engine.url import URL
import logging
import settings

feeddataList = []

class NewsFeedsData:
    def __init__(self, title, link, description, pubDate):
        self.title = title
        self.link = link
        self.description = description
        self.pubDate = pubDate

    def toString(self):
        return '"' + str(self.title) + '","'+ str(self.link) + '","'+ str(self.description) + '","'+ str(self.pubDate)

class NewsFeedCrawler:
    def __init__(self, starting_url, depth):
        self.starting_url = starting_url
        self.depth = depth
        self.apps = []

    def crawl(self):
        self.get_app_from_link(self.starting_url)
        return

    def reformatStr(self, inputStr):
        return unidecode(inputStr).replace('\n', '').replace('\r', '')
        # return str(source, encoding='utf-8', errors='ignore')

    def get_app_from_link(self, link):

            start_page = requests.get(link)

            try:
                root = ET.fromstring(start_page.text.decode('utf-8'))
                # root = ElementTree.parse(start_page.text)
                levels = root.findall('.//item')
            except UnicodeEncodeError:
                print "wrong root"
                print start_page.text

            #print(levels)

            for level in levels:

                try:
                    title = self.reformatStr(level.find('title').text)
                    link = self.reformatStr(level.find('link').text)
                    description = self.reformatStr(level.find('description').text)
                    pubDate = self.reformatStr(level.find('pubDate').text)
                    print("====================================")
                    print("1)" + title)
                    print("2)" + link)
                    print("3)" + description)
                    print("4)" + pubDate)
                    feeddata = NewsFeedsData(title, link, description, pubDate)
                    feeddataList.append(feeddata)
                except UnicodeEncodeError:
                    print "wrong encoder inside loop"

# Db handling
DeclarativeBase = declarative_base()

class NewsDB(DeclarativeBase):
    __tablename__ = "news"
    id = Column(Integer, Sequence('trigger_id_seq'), primary_key=True)
    title = Column('title', String)
    link = Column('link', String)
    description = Column('description', String)
    pubDate = Column('pubDate', DateTime)

class DBHandling:

    def __init__(self):
        db_url = URL(**settings.DATABASE)
        logging.info("Creating as SQLdatabase connection at URL at URL '{db_url}'".format(db_url=db_url))

        db = create_engine(db_url)
        Session = sessionmaker(db)
        self.session = Session()
        DeclarativeBase.metadata.create_all(db)

    def insertIntoDB(self, data):
        psql = NewsDB(
            title=data.title,
            link=data.link,
            description=data.description,
            pubDate=data.pubDate
        )
        self.session.add(psql)
        self.session.commit()


links = ['https://www.theedgemarkets.com/mytopstories.rss', 'https://www.malaysiakini.com/en/news.rss']

for strlink in links:
    newsFeedCrawler = NewsFeedCrawler(strlink, 0)
    newsFeedCrawler.crawl()


# Prepare s file name as exported csv file
timeStr = time.strftime("%Y%m%d_%H%M%S", time.gmtime())
newsdataFile = open('/Users/mhafizhussin/Documents/UM_MASTER_PROGRAM/SEMESTER\ 3/WQD7005_DATA_MINING/GROUP_CODES/Hafiz_Data_Mining/data/newsdata_' + timeStr + '.csv', 'w')

newsdataFile.write("title, link, description, pubDate\n")

dbIns = DBHandling()
for fdata in feeddataList:
    print(fdata.toString())
    newsdataFile.write(fdata.toString() + '\n')
    dbIns.insertIntoDB(fdata)

