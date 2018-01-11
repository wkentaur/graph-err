#!/usr/bin/python
# -*- coding: utf-8 -*-

from UudisKratt import UudisKratt
import time
from wordcloud import WordCloud
import requests
import os.path, json, logging

from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy import API

logging.basicConfig(filename='log/tweet.log',level=logging.INFO)
logging.basicConfig(format='%(asctime)s %(message)s')

homedir = os.path.expanduser('~')
confFile = os.path.join(homedir, '.graph-err.cnf')
with open(confFile) as json_data_file:
	myConf = json.load(json_data_file)

auth = OAuthHandler(myConf['twitter']['consumer_key'], myConf['twitter']['consumer_secret'])
auth.set_access_token(myConf['twitter']['access_token'], myConf['twitter']['access_token_secret'])
api = API(auth)

filename = "img/wordcloud.png"

actor = UudisKratt()

time_now = int(time.time()*1000)
results = actor.graph.data(
"CALL ga.timetree.events.single({time: {inTime}}) YIELD node "
"MATCH(t:Term)<--(w:LocalWord)<-[sw]-(s:Sentence)<--(node) "
"WHERE NOT t.text IN ['ETV', 'ERR'] "
"RETURN DISTINCT t.text, count(sw) as sen_count "
"ORDER BY sen_count DESC "
"LIMIT 50 "
, {'inTime': time_now}
)

top_terms = {};
for row in results:
	top_terms[row['t.text']] = row['sen_count']


# Generate a word cloud image
wordcloud = WordCloud(width=600, height=300).fit_words(top_terms)
wordcloud.to_file(filename)

# and tweet it
api.update_with_media(filename, status="#täna")

logging.info ("Posted tweet.")
