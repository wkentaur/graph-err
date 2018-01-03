#!/usr/bin/python
# -*- coding: utf-8 -*-

import os.path, json
import urllib.request
from bs4 import BeautifulSoup
from estnltk import Text
from py2neo import Graph
import re
import time, datetime
import logging

class UudisKratt():

	def __init__(self):
		homedir = os.path.expanduser('~')
		confFile = os.path.join(homedir, '.graph-err.cnf')
		with open(confFile) as json_data_file:
			myConf = json.load(json_data_file)

			self.graph = Graph(user=myConf['neo4j']['user'], password=myConf['neo4j']['password']) 

	def fetchArticle(self, article_url):

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
			self.insertNews(article_url, title, pub_date, timezone)

			editor_txt = art_text.find("p", {'class': 'editor'})
			if (editor_txt):
				editor_txt = editor_txt.find("span", {'class': 'name'})
			if (editor_txt and len(editor_txt) > 0):
				self.insertEditor(article_url, editor_txt.text)

			for row in art_text.find_all("p", {'class': None}) :
				out_text = "%s%s" % (out_text, row.text)

			self.analyzeText(out_text, article_url)
			return True

		return False

	def insertNews(self, url, title, published, timezone):

		if ( url and not self.getNewsNode(url) ):
			timePart = published.split(" ")[1]
			pubDaySec = self.getSec(timePart)
			pubTimestamp = int(time.mktime(datetime.datetime.strptime(published, "%Y-%m-%d %H:%M:%S").timetuple()))*1000
			self.graph.run(
			"CREATE (ns:Nstory {url: {inUrl}, title: {inTitle}, pubDaySec: toInteger({inPubDaySec})}) "
			"WITH ns "
			"CALL ga.timetree.events.attach({node: ns, time: {inTimestamp}, timezone: '{inTz}', relationshipType: 'PUBLISHED_ON'}) "
			"YIELD node RETURN node ", 
			{'inUrl': url, 'inTitle': title, 'inPubDaySec': pubDaySec, 'inTimestamp': pubTimestamp, 'inTz': timezone}
			)
		return

	def insertEditor(self, inUrl, inEditor):
		if (inUrl and inEditor):
			self.graph.run(
			"MATCH (ns:Nstory {url: {inUrl}}) "
			"MERGE (editor:Editor {name: {inEditor}}) "
			"MERGE (ns)-[:EDITED_BY]->(editor)"
			, {'inUrl': inUrl, 'inEditor': inEditor }
			)
		return

	def getSec(self, time_str):
		h, m, s = time_str.split(':')
		return int(h) * 3600 + int(m) * 60 + int(s)

	def analyzeText(self, in_text, article_url):

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

	def insertSentence(self, article_url, sentence_num):

		results = self.graph.data(
		"MATCH (nstory:Nstory {url: {inUrl} })-[]-(sentence:Sentence {numInNstory: toInteger({inNum})}) RETURN sentence LIMIT 1 ", 
		{'inUrl': article_url, 'inNum': sentence_num} 
		)

		if (len(results)==0):
			self.graph.run(
			"MATCH (nstory:Nstory {url: {inUrl} }) "
			"CREATE (sentence:Sentence {numInNstory: toInteger({inNum})}) "
			"MERGE (nstory)-[:HAS]->(sentence)", {'inUrl': article_url, 'inNum': sentence_num}
			)


	def getNewsNode(self, inUrl):
		return self.graph.find_one('Nstory', property_key='url', property_value=inUrl)  


	def insertWord(self, n_url, sen_num, w_text, w_type, orig_text):
		
		results = self.graph.data(
		"MATCH (:Nstory {url: {inUrl} })--(:Sentence)--(word:LocalWord {text: {inText}}) "
		"RETURN word LIMIT 1 ", 
		{'inUrl': n_url, 'inText': w_text} 
		)
		
		if (len(results)==0):
			
			if (w_text.find('|') > 0):
				self.graph.run(
				"MATCH (nstory:Nstory {url: {inUrl}})-[]-(sentence:Sentence {numInNstory: toInteger({inNum})}) "
				"CREATE (word:LocalWord {text: {inText}}) "
				"SET word.type = {inType}, word.origtext = {inOrigtext} "
				"MERGE (sentence)-[senword:HAS]->(word) "
				"ON CREATE SET senword.count = 1 "
				, {'inUrl': n_url, 'inNum': sen_num, 'inText': w_text, 'inType': w_type, 'inOrigtext': orig_text}
				)
			else:
				self.graph.run(
				"MATCH (nstory:Nstory {url: {inUrl}})-[]-(sentence:Sentence {numInNstory: toInteger({inNum})}) "
				"CREATE (word:LocalWord {text: {inText}}) "
				"SET word.type = {inType} "
				"MERGE (sentence)-[senword:HAS]->(word) "
				"ON CREATE SET senword.count = 1 "
				, {'inUrl': n_url, 'inNum': sen_num, 'inText': w_text, 'inType': w_type}
				)
		else:
				self.graph.run(
				"MATCH (nstory:Nstory {url: {inUrl}})-[]-(sentence:Sentence {numInNstory: toInteger({inNum})}) "
				"MATCH (nstory:Nstory {url: {inUrl} })--()--(word:LocalWord {text: {inText}}) "
				"MERGE (sentence)-[senword:HAS]->(word) "
				"ON CREATE SET senword.count = 1 "
				"ON MATCH SET senword.count = senword.count + 1 "
				, {'inUrl': n_url, 'inNum': sen_num, 'inText': w_text}
				)
		return 

	def genNewsTerms(self, inUrl):

		results = self.graph.data(
		"MATCH (nstory:Nstory {url: {inUrl} })--(sentence:Sentence)--(word:LocalWord) "
		"RETURN DISTINCT word.text as text, word.type as type, id(word) as id "
		"ORDER BY type " 
		, {'inUrl': inUrl} 
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

	def checkForLocalWords(self, inUrl):

		newsNode = self.getNewsNode(inUrl)
		for rel in self.graph.match(start_node=newsNode, rel_type="HAS"):
			sentence = rel.end_node()
			sWordRels = self.graph.match(start_node=sentence, rel_type="HAS")
			if (next(sWordRels, None)  == None):
				logging.info("dead end sentence for url: %s ...fetching article" % (inUrl, ))
				self.fetchArticle(inUrl)
				return
		return

	def insertTerm(self, wText, wType, wId):

		termId = None
		if (wText.find('|') > 0):
			if (wType == 'PER'):
				if (wText.find(' ') > 0):
					termId = self.getTermByWord(wText, wType)
			else:
				termId = self.getTermByWord(wText, wType)

		if (not termId):
			termId = "%s|%s" % (wText, wType)
		if (wText.find('|') > 0):
			self.graph.run(
			"MERGE (term:Term {id: {termId}}) "
			"ON CREATE SET term.text = {wText}, term.type = {wType}, term.fuzzy = 'true'"
			"WITH term "
			"MATCH (word:LocalWord) "
			"WHERE id(word) = {wId} "
			"MERGE (word)-[:IS]->(term) "
			, {'termId': termId, 'wText': wText, 'wType': wType, 'wId': wId}
			)
		else:
			self.graph.run(
			"MERGE (term:Term {id: {termId}}) "
			"ON CREATE SET term.text = {wText}, term.type = {wType} "
			"WITH term "
			"MATCH (word:LocalWord) "
			"WHERE id(word) = {wId} "
			"MERGE (word)-[:IS]->(term) "
			, {'termId': termId, 'wText': wText, 'wType': wType, 'wId': wId}
			)
		return

	def getTermByWord(self, wText, wType):
		results = self.graph.data(
		"MATCH (word:LocalWord {text: {wText}, type: {wType} })--(term:Term) "
		"RETURN term.id "
		"LIMIT 1 " 
		, {'wText': wText, 'wType': wType} 
		)
		if (len(results) > 0):
			return results[0]['term.id']
		else:
			return None


