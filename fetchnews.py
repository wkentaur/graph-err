#!/usr/bin/python
# -*- coding: utf-8 -*-

from UudisKratt import UudisKratt
import urllib.request
from bs4 import BeautifulSoup
import time
import logging
import filelock

startTime = time.time()
logging.basicConfig(filename='log/fetchnews.log',level=logging.INFO)
logging.basicConfig(format='%(asctime)s %(message)s')
lock = filelock.FileLock(UudisKratt.LOCK_FILE)

rss_url = 'http://uudised.err.ee/rss'
excl_cats = ['Viipekeelsed uudised', 'ETV uudistesaated', 'Ilm', 'Tele/raadio']

with lock:
	actor = UudisKratt()

	f = urllib.request.urlopen(rss_url)
	rss_data = f.read()
	soup = BeautifulSoup(rss_data, "xml")
	news_count = 0

	for rss_item in soup.find_all('item'):

		if (rss_item.category.string.strip() not in excl_cats):
			latest_link = rss_item.link.string
			newsNode = actor.getNewsNode(latest_link)
			if (not newsNode):
				if ( actor.fetchArticle(latest_link) ):
					news_count += 1
		runTime = time.time() - startTime
		if (runTime > (15*60) ):
			logging.error("Runtime limit exeeded, stopping...")
			break

runTime = time.time() - startTime
logging.info ("Fetched %d news in %f seconds." % (news_count, runTime))
