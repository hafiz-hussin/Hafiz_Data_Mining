# author: Peter
# edited: Hafiz
# add postgresql

# Import the Twython class
from twython import Twython
import json
import pandas as pd
import mysql.connector
import time
from unidecode import unidecode

tweetList = []


class Tweets:
    def __init__(self, user, date, text, favorite_count):
        self.user = user
        self.date = date
        self.text = text
        self.favorite_count = favorite_count

    def toString(self):
        return '"' + str(self.user) + '","' + str(self.date) + '","' + str(self.text) + '","' + str(
            self.favorite_count) + '"'


class TwitterApp:
    def __init__(self):
        # Load credentials from json file
        # Must have the Tweeter developers' key for accessing the twitter API
        with open("twitter_credentials.json", "r") as file:
            creds = json.load(file)

        # Instantiate an object
        self.python_tweets = Twython(creds['CONSUMER_KEY'], creds['CONSUMER_SECRET'])

    def reformatStr(self, inputStr):
        return unidecode(inputStr).replace('\n', '').replace('\r', '')

    def crawlTweets(self, tweetQuery):
        # Create our query
        query = {'q': tweetQuery
            , 'src': 'typd'
                 # 'result_type': 'popular',
                 # #'count': 50,
            , 'lang': 'en',
                 }

        # Search tweets
        dict_ = {'user': [], 'date': [], 'text': [], 'favorite_count': []}
        for status in self.python_tweets.search(**query)['statuses']:
            dict_['user'].append(self.reformatStr(status['user']['screen_name']))
            dict_['date'].append(self.reformatStr(status['created_at']))
            dict_['text'].append(self.reformatStr(status['text']))
            dict_['favorite_count'].append(status['favorite_count'])

            TweetsData = Tweets(
                self.reformatStr(status['user']['screen_name']),
                self.reformatStr(status['created_at']),
                self.reformatStr(status['text']),
                status['favorite_count'])

            tweetList.append(TweetsData)

        pd.set_option('display.max_columns', None)
        pd.set_option('display.expand_frame_repr', False)
        pd.set_option('max_colwidth', -1)

        # Structure data in a pandas DataFrame for easier manipulation
        df = pd.DataFrame(dict_)
        # df.sort_values(by='favorite_count', inplace=True, ascending=False)
        # print(df.head(5))
        # print(df)


# Db handling
class DBHandling:

    def __init__(self):
        self.mydb = mysql.connector.connect(
            host='localhost',
            user='root',
            passwd='root',
            database='mdsklse'
        )

    def insertIntoDB(self, data):
        mycursor = self.mydb.cursor()

        sql = " INSERT INTO tweetsdata " \
              " ( user, post_date, post_text, favorite_count ) VALUES (%s, %s, %s, %s )"

        val = (str(data.user), str(data.date), str(data.text), str(data.favorite_count))

        mycursor.execute(sql, val)
        self.mydb.commit()


twitterApp = TwitterApp()
toQuery = ['KLSE', 'Malaysia', 'stock', 'bursa saham']
for qStr in toQuery:
    twitterApp.crawlTweets(qStr)

dbInst = DBHandling()

# Prepare s file name as exported csv file
timeStr = time.strftime("%Y%m%d_%H%M%S", time.gmtime())
tweetOutputFile = open('csvdata/tweetsdata_' + timeStr + '.csv', 'w')
tweetOutputFile.write("user,post_date,post_text,favorite_count\n")

for tweetData in tweetList:
    print(tweetData.toString())
    dbInst.insertIntoDB(tweetData)
    tweetOutputFile.write(tweetData.toString() + '\n')
