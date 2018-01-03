#!/usr/bin/python
# -*- coding: utf-8 -*-

from UudisKratt import UudisKratt
import logging


logging.basicConfig(filename='log/genterms.log',level=logging.INFO)
logging.basicConfig(format='%(asctime)s %(message)s')

actor = UudisKratt()

results = actor.graph.data(
"MATCH (n:Nstory)--()--(w:LocalWord) "
"WHERE NOT (w)-[:IS]->() "
"RETURN DISTINCT n.url "
"LIMIT 1000 "
)


news_count = 0

for row in results:
	actor.genNewsTerms(row['n.url'])
	news_count += 1

logging.info ("Generated terms for %d news." % (news_count, ))