import tweepy #https://github.com/tweepy/tweepy
import csv
import json

from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener


#Twitter API credentials
CONSUMER_KEY = "rCSg2Rkgh67cxxGagoGySIJ9V"
CONSUMER_SECRET = "yB23Cy8wRWKOsTuRQap3putUBdCdrHpq0TrUVWOXH6moWAT0ms"
ACCESS_KEY = "535487768-RSzeE9PRRPj72hq6QYS96nzKln6EP4QOJsY0W90m"
ACCESS_sECRET = "Ynln1X324tfTdwoZsEd8fzSSCrQ89TP6mnaBGnVAN8jKa"

count = 0;

class MyListener(StreamListener):
    def on_data(self, data):
        jsonData=json.loads(data)
        text1=jsonData['text']
        text2=jsonData['entities']['hashtags']
        #Change the file name if different files are needed.
        f1=open('./tweetFile1.txt', 'ab')
        a = csv.writer(f1, delimiter=',')
        print(text1)
        a.writerow([str(text1.encode('utf-8'))])
        return StreamListener.on_data(self, data)

    def on_error(self, status):
        #print(status)
        return True

def Collect_Tweets():

    AUTH = OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    AUTH.set_access_token(ACCESS_KEY, ACCESS_sECRET)
    twitter_stream = Stream(AUTH, MyListener())
    twitter_stream.filter(track=["trump"])


Collect_Tweets()
