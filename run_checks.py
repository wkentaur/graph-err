#!/usr/bin/python
# -*- coding: utf-8 -*-

from UudisKratt import UudisKratt
import filelock
import logging

logging.basicConfig(filename='log/run_checks.log',level=logging.ERROR)
logging.basicConfig(format='%(asctime)s %(message)s')
lock = filelock.FileLock(UudisKratt.LOCK_FILE)

###### DAY

# Day has MAX 250 Nstories
def checkDayNstory():
	MAX_NSTORIES = 250
	error_count = 0
	results = actor.graph.data(
	"MATCH (d:Day)--(n:Nstory) "
	"RETURN distinct d, count(n) as ncount "
	"ORDER BY ncount desc "
	"LIMIT 1 "
	)
	if (len(results)>0):
		if (results[0]["ncount"] > MAX_NSTORIES):
			logging.error("Day has more than %d Nstories" % (MAX_NSTORIES) )
			error_count = 1
	return error_count

###### LOCALWORD

# LocalWord has only one Nstory parent
def checkLocalWordNstory():
	error_count = 0
	results = actor.graph.data(
	"MATCH (w:LocalWord)--(:Sentence)--(n:Nstory) "
	"RETURN w, length(collect(distinct n)) as ncount "
	"ORDER BY ncount desc "
	"LIMIT 1 "
	)
	if (len(results)>0):
		if (results[0]["ncount"] != 1):
			logging.error("LocalWord is connected to more than one distinct Nstory")
			error_count = 1
	return error_count


# LocalWord has MAX 120 Sentence-s
def checkLocalWordSentence():
	MAX_SENTENCES = 120
	error_count = 0
	results = actor.graph.data(
	"MATCH (w:LocalWord)--(s:Sentence) "
	"RETURN distinct w, count(s) as scount "
	"ORDER BY scount desc "
	"LIMIT 1 "
	)
	if (len(results)>0):
		if (results[0]["scount"] > MAX_SENTENCES):
			logging.error("Day has more than %d Nstories" % (MAX_SENTENCES) )
			error_count = 1
	return error_count

# LocalWord is connected to MAX one Term
def checkLocalWordTerm():
	error_count = 0
	results = actor.graph.data(
	"MATCH (w:LocalWord)--(t:Term) "
	"RETURN distinct w, count(t) as tcount "
	"ORDER BY tcount desc "
	"LIMIT 1 "
	)
	if (len(results)>0):
		if (results[0]["tcount"] != 1):
			logging.error("LocalWord is connected to more than one Term")
			error_count = 1
	return error_count

# Term and LocalWord have same type property value
def checkLocalWordType():
	error_count = 0
	results = actor.graph.data(
	"MATCH (w:LocalWord)--(t:Term) "
	"WHERE w.type <> t.type "
	"RETURN w "
	"LIMIT 1 "
	)
	if (len(results)>0):
		logging.error("LocalWord and Term have different types")
		error_count = 1
	return error_count

###### NSTORY

# Nstory has MAX 6 Editor-s
def checkNstoryEditor():
	MAX_EDITORS = 6
	error_count = 0
	results = actor.graph.data(
	"MATCH (n:Nstory)--(e:Editor) "
	"RETURN distinct n, count(e) as ecount "
	"ORDER BY ecount desc "
	"LIMIT 1 "
	)
	if (len(results)>0):
		if (results[0]["ecount"] > MAX_EDITORS):
			logging.error("Nstory has more than %d Editors" % (MAX_EDITORS) )
			error_count = 1
	return error_count

# Nstory has MAX 800 distinct LocalWord-s
def checkNstoryLocalWord():
	MAX_LOCALWORDS = 800
	error_count = 0
	results = actor.graph.data(
	"MATCH (w:LocalWord)--(:Sentence)--(n:Nstory) "
	"RETURN n, length(collect(distinct w)) as wcount "
	"ORDER BY wcount desc "
	"LIMIT 1 "
	)
	if (len(results)>0):
		if (results[0]["wcount"] > MAX_LOCALWORDS):
			logging.error("Nstory has more than %d distinct LocalWords" % (MAX_LOCALWORDS) )
			error_count = 1
	return error_count

# Nstory has MAX 300 Sentence-s
def checkNstorySentence():
	MAX_SENTENCES = 300
	error_count = 0
	results = actor.graph.data(
	"MATCH (n:Nstory)--(s:Sentence) "
	"RETURN distinct n, count(s) as scount "
	"ORDER BY scount desc "
	"LIMIT 1 "
	)
	if (len(results)>0):
		if (results[0]["scount"] > MAX_SENTENCES):
			logging.error("Nstory has more than %d Sentences" % (MAX_SENTENCES) )
			error_count = 1
	return error_count


###### SENTENCE

# Sentence has MAX 200 LocalWord-s
def checkSentenceLocalWord():
	MAX_LOCALWORDS = 200
	error_count = 0
	results = actor.graph.data(
	"MATCH (s:Sentence)--(w:LocalWord) "
	"RETURN distinct s, count(w) as wcount " 
	"ORDER BY wcount desc "
	"LIMIT 1 "
	)
	if (len(results)>0):
		if (results[0]["wcount"] > MAX_LOCALWORDS):
			logging.error("Sentence has more than %d LocalWords" % (MAX_LOCALWORDS) )
			error_count = 1
	return error_count


# Sentence is connected only to one Nstory
def checkSentenceNstory():
	error_count = 0
	results = actor.graph.data(
	"MATCH (n:Nstory)--(s:Sentence) "
	"RETURN distinct s, count(n) as ncount "
	"ORDER BY ncount desc "
	"LIMIT 1 "
	)
	if (len(results)>0):
		if (results[0]["ncount"] != 1):
			logging.error("Sentence is connected to more than one Nstory")
			error_count = 1
	return error_count

#main
 
with lock:
	actor = UudisKratt()

	error_count = 0

	error_count += checkDayNstory()
	error_count += checkLocalWordNstory()
	error_count += checkLocalWordSentence()
	error_count += checkLocalWordTerm()
	error_count += checkLocalWordType()
	error_count += checkNstoryEditor()
	error_count += checkNstoryLocalWord()
	error_count += checkNstorySentence()
	error_count += checkSentenceLocalWord()
	error_count += checkSentenceNstory()

	if (error_count > 0):
		raise AssertionError('%d data checks failed!' % (error_count) )
	else:
		logging.error("All checks passed.")
