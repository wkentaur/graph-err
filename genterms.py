#!/usr/bin/python
# -*- coding: utf-8 -*-

from UudisKratt import UudisKratt
import filelock
import logging


logging.basicConfig(filename='log/genterms.log',level=logging.INFO)
logging.basicConfig(format='%(asctime)s %(message)s')
lock = filelock.FileLock(UudisKratt.LOCK_FILE)

with lock:
	actor = UudisKratt()
	added_terms = set()

	results = actor.graph.data(
	"MATCH (w:LocalWord) "
	"WHERE NOT (w)-[:IS]->() "
	"WITH w "
	"MATCH (w)<--()<--(n:Nstory) "
	"RETURN DISTINCT n.url "
	"LIMIT 1000 "
	)

	news_count = 0

	for row in results:
		added_terms.update( actor.genTerms(row['n.url']) )
		news_count += 1

	#update incoming links count
	for term_id in added_terms:
		results = actor.graph.run(
		"MATCH (t:Term {id: {termId}})-[r]-(:LocalWord) "
		"WITH t, count(r) AS in_count "
		"SET t.incoming = in_count "
		, {'termId': term_id}
	)

logging.info ("Generated terms for %d news." % (news_count, ))
