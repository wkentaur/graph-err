#!/usr/bin/python
# -*- coding: utf-8 -*-

import os.path, json
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError
from bs4 import BeautifulSoup
from estnltk import Text
from py2neo import Graph
import re
import time, datetime
import logging

class UudisKratt():

	VERSION = "3"
	MAX_TEXT_LEN = 90000

	def __init__(self):

		self.throttle_delay = 4   # sec
		self.last_request_time = None
		homedir = os.path.expanduser('~')
		confFile = os.path.join(homedir, '.graph-err.cnf')
		with open(confFile) as json_data_file:
			my_conf = json.load(json_data_file)
			self.graph = Graph(user=my_conf['neo4j']['user'], password=my_conf['neo4j']['password']) 

	def fetchArticle(self, article_url):

		out_text = ''

		if (article_url):
			current_time = time.time()
			if (self.last_request_time and
			(current_time - self.last_request_time < self.throttle_delay)):
				throttle_time = (self.throttle_delay -
								(current_time - self.last_request_time))
				logging.info("sleeping for %f seconds" % (throttle_time))
				time.sleep(throttle_time)
			self.last_request_time = time.time()
			req = Request(article_url)
			try:
				response = urlopen(req)
			except HTTPError as e:
				logging.error('HTTPError: ', e.code)
			except URLError as e:
				logging.error('URLError: ', e.reason)
			else:
				req_url = response.geturl()
				if (req_url != article_url):
					self.updateNewsUrl(article_url, req_url)
					article_url = req_url

				html_data = response.read()

				soup = BeautifulSoup(html_data, "lxml")
				##<meta property="article:modified_time" content="2016-12-14T10:49:25+02:00" />
				mod_date = soup.find("meta",  property="article:modified_time")
				cat_match = soup.find("meta",  property="article:section")
				category = ''
				if (cat_match):
					category = cat_match["content"]

				pub_date = ''
				if (mod_date):
					match_date = re.search("^(\d+-\d+-\d+)T(\d+:\d+:\d+)", mod_date["content"])
					if match_date:
						pub_date = "%s %s" % (match_date.group(1), match_date.group(2))

				#title
				title = ''
				m_title = soup.find("meta",  property="og:title")
				if (m_title):
					title = m_title["content"]

				art_text = soup.find("article") 
				timezone = "GMT+02:00"

				if (art_text and len(pub_date) > 0):
					self.insertNews(article_url, title, pub_date, timezone, category)

					editor_txt = art_text.find("p", {'class': 'editor'})
					if (editor_txt):
						editor_txt = editor_txt.find("span", {'class': 'name'})
					if (editor_txt and len(editor_txt) > 0):
						if (editor_txt.text.find(',') > 0):
							for editor in editor_txt.text.split(','):
								self.insertEditor( article_url, editor.strip() )
						else:
							self.insertEditor( article_url, editor_txt.text.strip() )

					for html_break in art_text.find_all('br'):
						html_break.replace_with('; ')
					for row in art_text.find_all("p", {'class': None}):
						row_text = row.get_text(separator=u' ')
						out_text = "%s %s" % (out_text, row_text)

					retval = self.analyzeText(out_text, article_url)
					return retval
				else:
					logging.error("Malformed content at url: %s" % (article_url))

		return False

	def insertNews(self, url, title, published, timezone, category):

		if (url and published.find(" ") > 0):
			time_part = published.split(" ")[1]
			pub_day_sec = self.getSec(time_part)
			pub_timestamp = int(time.mktime(datetime.datetime.strptime(published, "%Y-%m-%d %H:%M:%S").timetuple()))*1000
			if (category):
				self.graph.run(
				"MERGE (ns:Nstory {url: {inUrl}}) "
				"SET ns.title = {inTitle}, ns.pubDaySec = toInteger({inPubDaySec}), ns.category = {inCat}, ns.ver = {inVer} "
				"WITH ns "
				"CALL ga.timetree.events.attach({node: ns, time: {inTimestamp}, timezone: '{inTz}', relationshipType: 'PUBLISHED_ON'}) "
				"YIELD node RETURN node ", 
				{'inUrl': url, 'inTitle': title, 'inPubDaySec': pub_day_sec, 'inCat': category, 'inVer': UudisKratt.VERSION , 'inTimestamp': pub_timestamp, 'inTz': timezone}
				)
			else:
				self.graph.run(
				"MERGE (ns:Nstory {url: {inUrl}}) "
				"SET ns.title = {inTitle}, ns.pubDaySec = toInteger({inPubDaySec}), ns.ver = {inVer} "
				"WITH ns "
				"CALL ga.timetree.events.attach({node: ns, time: {inTimestamp}, timezone: '{inTz}', relationshipType: 'PUBLISHED_ON'}) "
				"YIELD node RETURN node ", 
				{'inUrl': url, 'inTitle': title, 'inPubDaySec': pub_day_sec, 'inVer': UudisKratt.VERSION , 'inTimestamp': pub_timestamp, 'inTz': timezone}
				)
			#FIXME if new PUBLISHED_ON rel is added, remove old PUBLISHED_ON relation
		return

	def insertEditor(self, url, editor):
		if (url and editor):
			self.graph.run(
			"MATCH (ns:Nstory {url: {inUrl}}) "
			"MERGE (editor:Editor {name: {inEditor}}) "
			"MERGE (ns)-[:EDITED_BY]->(editor)"
			, {'inUrl': url, 'inEditor': editor }
			)
		return

	def getSec(self, time_str):
		h, m, s = time_str.split(':')
		return int(h) * 3600 + int(m) * 60 + int(s)

	def analyzeText(self, in_text, article_url):

		if (len(in_text) < UudisKratt.MAX_TEXT_LEN ):
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
				## Birsbane’is
				if ( out_entity.find("'") > 0 or out_entity.find("’") > 0 ):
					out_entity = re.sub(u"^(.+?)[\'\’]\w*", u"\\1", out_entity)
				w_type = text.named_entity_labels[count]
				
				if (sentence_count != prev_sen_num):
					self.insertSentence(article_url, sentence_count)
					prev_sen_num = sentence_count
				
				self.insertWord(article_url, sentence_count, out_entity, w_type, orig_text)

				count += 1
			return True
		else:
			logging.error("text size exceeds limit! url: %s" % (article_url) )
			return False

	def insertSentence(self, article_url, sentence_num):

		results = self.graph.data(
		"MATCH (nstory:Nstory {url: {inUrl} })--(sentence:Sentence {numInNstory: toInteger({inNum})}) RETURN sentence LIMIT 1 ", 
		{'inUrl': article_url, 'inNum': sentence_num} 
		)

		if (len(results)==0):
			self.graph.run(
			"MATCH (nstory:Nstory {url: {inUrl} }) "
			"CREATE (sentence:Sentence {numInNstory: toInteger({inNum})}) "
			"MERGE (nstory)-[:HAS]->(sentence)", {'inUrl': article_url, 'inNum': sentence_num}
			)


	def getNewsNode(self, url):
		return self.graph.find_one('Nstory', property_key='url', property_value=url)  


	def insertWord(self, n_url, sen_num, w_text, w_type, orig_text):

		results = self.graph.data(
		"MATCH (:Nstory {url: {inUrl} })--(:Sentence)--(word:LocalWord {text: {inText}, type: {inType} }) "
		"RETURN word LIMIT 1 ", 
		{'inUrl': n_url, 'inText': w_text, 'inType': w_type} 
		)
		
		if (len(results)==0):
			
			if (w_text.find('|') > 0):
				self.graph.run(
				"MATCH (nstory:Nstory {url: {inUrl}})--(sentence:Sentence {numInNstory: toInteger({inNum})}) "
				"CREATE (word:LocalWord {text: {inText}}) "
				"SET word.type = {inType}, word.origtext = {inOrigtext} "
				"MERGE (sentence)-[senword:HAS]->(word) "
				"ON CREATE SET senword.count = 1 "
				, {'inUrl': n_url, 'inNum': sen_num, 'inText': w_text, 'inType': w_type, 'inOrigtext': orig_text}
				)
			else:
				self.graph.run(
				"MATCH (nstory:Nstory {url: {inUrl}})--(sentence:Sentence {numInNstory: toInteger({inNum})}) "
				"CREATE (word:LocalWord {text: {inText}}) "
				"SET word.type = {inType} "
				"MERGE (sentence)-[senword:HAS]->(word) "
				"ON CREATE SET senword.count = 1 "
				, {'inUrl': n_url, 'inNum': sen_num, 'inText': w_text, 'inType': w_type}
				)
		else:
				#FIXME return LocalWord on first connection match
				self.graph.run(
				"MATCH (nstory:Nstory {url: {inUrl}})--(sentence:Sentence {numInNstory: toInteger({inNum})}) "
				"MATCH (nstory:Nstory {url: {inUrl} })--()--(word:LocalWord {text: {inText}, type: {inType} }) "
				"WITH DISTINCT sentence, word "
				"MERGE (sentence)-[senword:HAS]->(word) "
				"ON CREATE SET senword.count = 1 "
				"ON MATCH SET senword.count = senword.count + 1 "
				, {'inUrl': n_url, 'inNum': sen_num, 'inText': w_text, 'inType': w_type}
				)
		return 

	def genNewsTerms(self, url):

		results = self.graph.data(
		"MATCH (nstory:Nstory {url: {inUrl} })--(sentence:Sentence)--(word:LocalWord) "
		"RETURN DISTINCT word.text as text, word.type as type, id(word) as id "
		"ORDER BY type " 
		, {'inUrl': url} 
		)
		persons = []
		for wordDict in results:
			if (wordDict['type'] == 'LOC' or wordDict['type'] == 'ORG'):
				self.insertTerm(wordDict['text'], wordDict['type'], wordDict['id'])
			elif (wordDict['type'] == 'PER'):
				if (wordDict['text'].find(' ') > 0):
					wordDict['surname'] = wordDict['text'].split(' ')[-1]
				else:
					wordDict['surname'] = ''
				persons.append(wordDict)
			else:
				logging.error("unknown LocalWord type: %s, word id(): %d " % (wordDict['type'], wordDict['id']))

		for person in persons:
			if (person['text'].find(' ') > 0):
				if (person['text'].find('|') > 0):
					useName = person['text']
					for fullname in person['text'].split('|'):
						match = next((item for item in persons if item["text"] == fullname), None)
						if (match) :
							useName = fullname
							person['surname'] = fullname.split(' ')[-1]
							break
					self.insertTerm(useName, person['type'], person['id'])
				else:
					self.insertTerm(person['text'], person['type'], person['id'])
			else:
				#lookup if short name is surname
				if (person['text'].find('|') > 0):
					useName = person['text']
					for name in person['text'].split('|'):
						match = next((item for item in persons if item["surname"] == name), None)
						if (match) :
							useName = match['text']
							break
					self.insertTerm(useName, person['type'], person['id'])
				else:
					useName = person['text']
					match = next((item for item in persons if item["surname"] == person['text']), None)
					if (match) :
						useName = match['text']
					self.insertTerm(useName, person['type'], person['id'])
		return

	def checkForLocalWords(self, url):

		newsNode = self.getNewsNode(url)
		sen_count = 0
		for rel in self.graph.match(start_node=newsNode, rel_type="HAS"):
			sentence = rel.end_node()
			sen_count += 1
			sWordRels = self.graph.match(start_node=sentence, rel_type="HAS")
			if (next(sWordRels, None)  == None):
				logging.info("dead end sentence for url: %s ...fetching article" % (url, ))
				self.fetchArticle(url)
				return
		if (sen_count == 0):
			logging.info("no sentences for url: %s ...fetching article" % (url, ))
			self.fetchArticle(url)
		return

	def insertTerm(self, w_text, w_type, w_id):

		term_id = None
		if (w_text.find('|') > 0):
			if (w_type == 'PER'):
				if (w_text.find(' ') > 0):
					term_id = self.getTermByWord(w_text, w_type)
			else:
				term_id = self.getTermByWord(w_text, w_type)

		if (not term_id):
			term_id = "%s|%s" % (w_text, w_type)
		if (w_text.find('|') > 0):
			self.graph.run(
			"MERGE (term:Term {id: {termId}}) "
			"ON CREATE SET term.text = {wText}, term.type = {wType}, term.fuzzy = 'true'"
			"WITH term "
			"MATCH (word:LocalWord) "
			"WHERE id(word) = {wId} "
			"MERGE (word)-[:IS]->(term) "
			, {'termId': term_id, 'wText': w_text, 'wType': w_type, 'wId': w_id}
			)
		else:
			self.graph.run(
			"MERGE (term:Term {id: {termId}}) "
			"ON CREATE SET term.text = {wText}, term.type = {wType} "
			"WITH term "
			"MATCH (word:LocalWord) "
			"WHERE id(word) = {wId} "
			"MERGE (word)-[:IS]->(term) "
			, {'termId': term_id, 'wText': w_text, 'wType': w_type, 'wId': w_id}
			)
		return

	def updateNewsUrl(self, old_url, new_url):
		if (self.getNewsNode(new_url)):
			# FIXME delete old Nstory node ?
			logging.info("node with url %s is duplicate" % (old_url) )
			return new_url
		logging.info("url %s redirected, updating news node" % (old_url) )
		results = self.graph.data(
		"MATCH (n:Nstory {url: {oldUrl} }) "
		"SET n.url = {newUrl} "
		"RETURN n.url "
		"LIMIT 1 " 
		, {'oldUrl': old_url, 'newUrl': new_url} 
		)
		if (len(results) > 0):
			return results[0]['n.url']
		return None

	def getTermByWord(self, w_text, w_type):
		results = self.graph.data(
		"MATCH (word:LocalWord {text: {wText}, type: {wType} })--(term:Term) "
		"RETURN term.id "
		"LIMIT 1 " 
		, {'wText': w_text, 'wType': w_type} 
		)
		if (len(results) > 0):
			return results[0]['term.id']
		else:
			return None

	def mergeTermInto(self, firstTerm, targetTerm):

		if( len(firstTerm)>0 and len(targetTerm)>0 ):
			res_cursor = self.graph.run(
			"MATCH (first:Term {id: {firstId} })--(w:LocalWord) "
			"MATCH (target:Term {id: {targetId} }) "
			"MERGE (w)-[:IS]->(target) "
			"RETURN w.text "
			, {'firstId': firstTerm, 'targetId': targetTerm} 
			)
			
			if res_cursor.forward():
				del_cursor = self.graph.run(
				"MATCH (first:Term {id: {firstId} }) "
				"DETACH DELETE(first) "
				, {'firstId': firstTerm} 
				)
				
				results = self.graph.run(
				"MATCH (t:Term {id: {targetId} })-[r]-(:LocalWord) "
				"WITH t, count(r) AS in_count "
				"SET t.incoming = in_count "
				, {'targetId': targetTerm}
				)
				return True
		return False
	
###########UudisKratt.py
