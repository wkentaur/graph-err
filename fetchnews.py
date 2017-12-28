#!/usr/bin/python
# -*- coding: utf-8 -*-

import os.path, json
import urllib.request
from bs4 import BeautifulSoup
from estnltk import Text
from py2neo import Graph
import re, logging
import time, datetime

def fetchArticle(article_url):

	out_text = ''

	if (article_url):
		f = urllib.request.urlopen(article_url)
		html_data = f.read()

		soup = BeautifulSoup(html_data, "lxml")
		##<meta property="article:modified_time" content="2016-12-14T10:49:25+02:00" />
		mod_date = soup.find("meta",  property="article:modified_time")

		pub_date = ''
		if (mod_date):

			matchDate = re.search("^(\d+-\d+-\d+)T(\d+:\d+:\d+)", mod_date["content"])
			if matchDate:
				pub_date = "%s %s" % (matchDate.group(1), matchDate.group(2))

		#title
		title = ''
		m_title = soup.find("meta",  property="og:title")
		if (m_title):
			title = m_title["content"]

		art_text = soup.find("article") 

		timezone = "GMT+02:00"
		insertNews(article_url, title, pub_date, timezone)

		for row in art_text.find_all("p") :
			out_text = "%s%s" % (out_text, row.text)

		analyzeText(out_text, article_url)
		return True

	return False

def insertNews(url, title, published, timezone):
	if (url):
		timePart = published.split(" ")[1]
		pubDaySec = getSec(timePart)
		pubTimestamp = int(time.mktime(datetime.datetime.strptime(published, "%Y-%m-%d %H:%M:%S").timetuple()))*1000
		graph.run(
		"CREATE (ns:Nstory {url: {inUrl}, title: {inTitle}, pubDaySec: toInteger({inPubDaySec})}) "
		"WITH ns "
		"CALL ga.timetree.events.attach({node: ns, time: {inTimestamp}, timezone: '{inTz}', relationshipType: 'PUBLISHED_ON'}) "
		"YIELD node RETURN node ", 
		{'inUrl': url, 'inTitle': title, 'inPubDaySec': pubDaySec, 'inTimestamp': pubTimestamp, 'inTz': timezone}
		)

	return

def getSec(time_str):
	h, m, s = time_str.split(':')
	return int(h) * 3600 + int(m) * 60 + int(s)

def analyzeText(in_text, article_url):

	text = Text(in_text)
	
	sentence_count = 0
	count = 0
	prev_sen_num = -1
	for named_entity in text.named_entities:
		ne_words = named_entity.split()
		orig_words = text.named_entity_texts[count].split()
		orig_text = text.named_entity_texts[count]
		
		word_count = 0
		out_entity = u''
		for ne_word in ne_words:
			if (word_count > len(orig_words)-1 ):
				break
			if (word_count):
				out_entity = "%s " % (out_entity)
			#last word  
			if (word_count == (len( ne_words )-1) ):
				new_word = ne_word
				if ( orig_words[word_count].isupper() ):
					new_word = new_word.upper()
				elif ( len(orig_words[word_count])>1 and orig_words[word_count][1].isupper() ):
					new_word = new_word.upper()
				elif ( orig_words[word_count][0].isupper() ):
					new_word = new_word.title()
				#Jevgeni Ossinovsk|Ossinovski
				if (out_entity and new_word.find('|') > 0 ):
					word_start = out_entity
					out_ent2 = ''
					for word_part in new_word.split('|'):
						if (out_ent2):
							out_ent2 = "%s|" % (out_ent2)
						out_ent2 = "%s%s%s" % (out_ent2, word_start, word_part)
					out_entity = out_ent2
				else:
					out_entity = "%s%s" % (out_entity, new_word)
			
			else:
				out_entity = "%s%s" % (out_entity, orig_words[word_count])
			
			word_count += 1
		
		ne_endpos = text.named_entity_spans[count][1]
		while (ne_endpos > text.sentence_ends[sentence_count]):
			sentence_count += 1
		
		## Rupert Colville'i
		if ( out_entity.find("'") > 0 ):
			out_entity = re.sub(u"^(.+?)\'\w*", u"\\1", out_entity)
		w_type = text.named_entity_labels[count]
		
		if (sentence_count != prev_sen_num):
			insertSentence(article_url, sentence_count)
			prev_sen_num = sentence_count
		
		insertWord(article_url, sentence_count, out_entity, w_type, orig_text)


		count += 1

def insertSentence(article_url, sentence_num):

	results = graph.data(
	"MATCH (nstory:Nstory {url: {inUrl} })-[]-(sentence:Sentence {numInNstory: toInteger({inNum})}) RETURN sentence LIMIT 1 ", 
	{'inUrl': article_url, 'inNum': sentence_num} 
	)

	if (len(results)==0):
		graph.run(
		"MATCH (nstory:Nstory {url: {inUrl} }) "
		"CREATE (sentence:Sentence {numInNstory: toInteger({inNum})}) "
		"MERGE (nstory)-[:HAS]->(sentence)", {'inUrl': article_url, 'inNum': sentence_num}
		)

def isInDb(inUrl):

	results = graph.data(
	"MATCH (a:Nstory {url: {inUrl} }) RETURN a.url LIMIT 1 ", {'inUrl': inUrl} 
	)

	return (len(results) > 0)
    

def insertWord(n_url, sen_num, w_text, w_type, orig_text):
	if (w_text):
		
		if (w_text.find('|') > 0):
			graph.run(
			"MATCH (nstory:Nstory {url: {inUrl}})-[]-(sentence:Sentence {numInNstory: toInteger({inNum})}) "
			"MERGE (word:Word {text: {inText}}) "
			"ON CREATE SET word.type = {inType}, word.origtext = {inOrigtext} "
			"MERGE (sentence)-[senword:HAS]->(word) "
			"ON CREATE SET senword.count = 1 "
			"ON MATCH SET senword.count = senword.count + 1", {'inUrl': n_url, 'inNum': sen_num, 'inText': w_text, 'inType': w_type, 'inOrigtext': orig_text}
			)
		else:
			graph.run(
			"MATCH (nstory:Nstory {url: {inUrl}})-[]-(sentence:Sentence {numInNstory: toInteger({inNum})}) "
			"MERGE (word:Word {text: {inText}}) "
			"ON CREATE SET word.type = {inType} "
			"MERGE (sentence)-[senword:HAS]->(word) "
			"ON CREATE SET senword.count = 1 "
			"ON MATCH SET senword.count = senword.count + 1", {'inUrl': n_url, 'inNum': sen_num, 'inText': w_text, 'inType': w_type}
			)
	return 


##
## main
##

logging.basicConfig(filename='fetchnews.log',level=logging.ERROR)
logging.basicConfig(format='%(asctime)s %(message)s')

homedir = os.path.expanduser('~')
confFile = os.path.join(homedir, '.graph-err.cnf')
with open(confFile) as json_data_file:
    myConf = json.load(json_data_file)

graph = Graph(user=myConf['neo4j']['user'], password=myConf['neo4j']['password']) 

rss_url = 'http://uudised.err.ee/rss'
excl_cats = ['Viipekeelsed uudised', 'ETV uudistesaated', 'Ilm']

f = urllib.request.urlopen(rss_url)
rss_data = f.read()
soup = BeautifulSoup(rss_data, "xml")
news_count = 0

for rss_item in soup.find_all('item'):

	if (rss_item.category.string.strip() not in excl_cats):
		latest_link = rss_item.link.string
		in_db = isInDb(latest_link)
		if (not in_db):
			if ( fetchArticle(latest_link) ):
				news_count += 1

logging.info ("Fetched %d news." % (news_count, ))
