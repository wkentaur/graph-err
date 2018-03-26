#!/usr/bin/python
# -*- coding: utf-8 -*-

import os.path, json
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError
from bs4 import BeautifulSoup
from estnltk import Text
from py2neo import Graph
from py2neo.ogm import GraphObject, Property, RelatedTo, RelatedFrom
from hashlib import md5
import re
import time, datetime
import logging

            
class Nstory(GraphObject):
	__primarykey__ = "url"

	url = Property()
	title = Property()
	category = Property()
	hash = Property()
	pub_day_sec = Property("pubDaySec")		#int
	ver = Property()

	sentences = RelatedTo("Sentence", "HAS")
	editors = RelatedTo("Editor", "EDITED_BY")

	def __init__(self):
		self.sentences_dict = {}
		self.words_dict = {}

	def pullLocalGraph(self, graph, pullNstory=True, resetWordCount=False):
		if (pullNstory):	graph.pull(self)
		for sen in self.sentences:
			self.sentences_dict[sen.num] = sen
			for word in sen.words:
				if (word.type not in self.words_dict):
					self.words_dict[word.type] = {}
				self.words_dict[word.type][word.text] = word
				if (resetWordCount):
					props = {}
					props['count'] = 0
					sen.words.update(word, props)

	def pushLocalGraph(self, graph):

		for sen in self.sentences_dict.values():
			if (sen not in self.sentences):
				self.sentences.add(sen)
			logging.info("Pushing sentence: %d with %d words. " % (sen.num, len(sen.words) ) )
			graph.push(sen)
		logging.info("Pushing nstory")
		graph.push(self)
		if ( len(self.sentences) !=  len(self.sentences_dict) ):
			raise AssertionError('Sentence counts dont match for url: %s' % (self.url) )

	def attachTimetree(self, graph, pub_date_time, pub_timezone):
		if ( pub_date_time.find(' ') > 0 ):
			prev_date = self.getPubDate(graph)
			pub_date = pub_date_time.split(' ')[0]
			if (prev_date):
				if (prev_date < pub_date):
					graph.run(
					"MATCH (ns:Nstory {url: {inUrl}})-[rel]->(:Day) "
					"WITH rel LIMIT 1 " 
					"DELETE rel ", 
					{'inUrl': self.url}
					)
				elif(prev_date > pub_date):
					raise AssertionError("Previous pub_date is bigger than new pub_date, url %s" % (self.url))
					return
			pub_timestamp = int(time.mktime(datetime.datetime.strptime(pub_date_time, "%Y-%m-%d %H:%M:%S").timetuple()))*1000
			graph.run(
			"MATCH (ns:Nstory {url: {inUrl}}) "
			"CALL ga.timetree.events.attach({node: ns, time: {inTimestamp}, timezone: '{inTz}', relationshipType: 'PUBLISHED_ON'}) "
			"YIELD node RETURN node ", 
			{'inUrl': self.url, 'inTimestamp': pub_timestamp, 'inTz': pub_timezone}
			)
		
	def getPubDate(self, graph):
		results = graph.data(
		"MATCH (n:Nstory {url: {inUrl} })-->(d:Day)<--(m:Month)<--(y:Year) "
		"RETURN d.value, m.value, y.value "
		"LIMIT 1 "
		, {'inUrl': self.url} 
		)
		if (len(results) > 0):
			date_str = "%d-%02d-%02d" % (results[0]['y.value'], results[0]['m.value'], results[0]['d.value'])
			return date_str
		else:
			return None

	def insertSentence(self, graph, sentence_num):

		if (not sentence_num in self.sentences_dict):
			new_sentence = Sentence()
			new_sentence.num = sentence_num
			##ogm class can't create temp id-s, with merge it gets internal ID from db
			graph.merge(new_sentence)
			self.sentences_dict[sentence_num] = new_sentence
			return new_sentence
		return None

	def insertWord(self, graph, sen_num, w_text, w_type, w_orig_text):

		created_newword = False
		if (sen_num in self.sentences_dict):
			sentence = self.sentences_dict[sen_num]
			if (w_type in self.words_dict and w_text in self.words_dict[w_type]):
				word = self.words_dict[w_type][w_text]
			else:
				word = LocalWord()
				if (w_type not in self.words_dict):
					self.words_dict[w_type] = {}
				self.words_dict[w_type][w_text] = word
				created_newword = True

			word.text = w_text
			word.type = w_type
			if (w_text.find('|') > 0):
				word.origtext = w_orig_text
			if (created_newword):
				##ogm class can't create temp id-s, with merge it gets internal ID from db
				graph.merge(word)
			props = {}
			props['count'] = sentence.words.get(word, 'count',0) + 1
			logging.info("sen: %d -> %s|%s" % (sen_num, word.text, word.type))
			sentence.words.update(word, props)
			return word
		else:
			raise ValueError('Sentence object doesnt exist.')
		return None

class Sentence(GraphObject):

	num = Property("numInNstory")	#int

	in_nstories = RelatedFrom("Nstory", "HAS")
	words = RelatedTo("LocalWord", "HAS")

class LocalWord(GraphObject):

	text = Property()
	type = Property()	#values: 'LOC', 'ORG', 'PER'
	origtext = Property()

	in_sentences = RelatedFrom("Sentence", "HAS")
	terms = RelatedTo("Term", "IS")

class Term(GraphObject):
	__primarykey__ = "id"

	text = Property()
	type = Property()			#values: 'LOC', 'ORG', 'PER'
	fuzzy = Property()			#values: "true"
	incoming = Property()		#int

	in_words = RelatedFrom("LocalWord", "IS")

class Editor(GraphObject):
	__primarykey__ = "name"

	name = Property()

	in_nstories = RelatedFrom("Nstory", "EDITED_BY")


class UudisKratt():

	VERSION = "4"
	MAX_TEXT_LEN = 110000
	MAX_FIELD_LEN = 150
	LOCK_FILE = "graph-err.lock"

	def __init__(self):

		self.throttle_delay = 8   # sec
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
				self.setErrorNstory(article_url)
				logging.error('HTTPError: %d, setting ErrorNstory' % (e.code) )
			except URLError as e:
				logging.error('URLError: %s' % (e.reason) )
			else:
				req_url = response.geturl()
				if (req_url != article_url):
					self.updateNstoryUrl(article_url, req_url)
					article_url = req_url

				nstory = Nstory()
				nstory.url = article_url
				html_data = response.read()

				soup = BeautifulSoup(html_data, "lxml")
				cat_match = soup.find("meta",  property="article:section")
				if (cat_match):
					nstory.category = cat_match["content"][:UudisKratt.MAX_FIELD_LEN]
				nstory.ver = UudisKratt.VERSION

				pub_date = ''
				pub_timezone = ''
				##<meta property="article:modified_time" content="2016-12-14T10:49:25+02:00" />
				mod_date = soup.find("meta",  property="article:modified_time")
				if (mod_date):
					match_date = re.search("^(\d+-\d+-\d+)T(\d+:\d+:\d+)\+(\d+:\d+)", mod_date["content"])
					if match_date:
						nstory.pub_day_sec = self.getSec(match_date.group(2))
						pub_date = "%s %s" % (match_date.group(1), match_date.group(2))
						pub_timezone = "GMT+%s" % (match_date.group(3) )

				#title
				m_title = soup.find("meta",  property="og:title")
				if (m_title):
					nstory.title = m_title["content"][:UudisKratt.MAX_FIELD_LEN]

				art_text = soup.find("article") 

				if (art_text and pub_date and pub_timezone):

					for html_break in art_text.find_all('br'):
						html_break.replace_with('; ')
					for row in art_text.find_all("p", {'class': None}):
						row_text = row.get_text(separator=u' ')
						out_text = "%s %s" % (out_text, row_text)

					logging.info("Updating Nstory: %s" % (article_url) )
					self.graph.merge(nstory)
					nstory.attachTimetree(self.graph, pub_date, pub_timezone)

					editor_txt = art_text.find("p", {'class': 'editor'})
					if (editor_txt):
						editor_txt = editor_txt.find("span", {'class': 'name'})
						if (len(editor_txt) > 0):
							for editor_str in editor_txt.text.split(','):
								editor = Editor()
								editor.name = editor_str.strip()[:UudisKratt.MAX_FIELD_LEN]
								nstory.editors.add(editor)

					retval = self.analyzeText(out_text, nstory)
					return retval
				else:
					logging.error("Malformed content at url, setting as ErrorNstory: %s" % (article_url))
					self.setErrorNstory(article_url)
		return False

	def texthash(self, text):
		return md5(text.encode('utf-8')).hexdigest()

	def getSec(self, time_str):
		h, m, s = time_str.split(':')
		return int(h) * 3600 + int(m) * 60 + int(s)

	def analyzeText(self, in_text, nstory):

		if (len(in_text) < UudisKratt.MAX_TEXT_LEN ):
			pullNstory = False
			resetWordCount = True
			nstory.pullLocalGraph(self.graph, pullNstory, resetWordCount)
			text = Text(in_text)
			
			sentence_count = 0
			count = 0
			prev_sen_num = -1
			logging.info("%s named entities: %d " % (nstory.url, len(text.named_entities) ) )
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
					out_entity = re.sub(u"^(.+?)[\'\’]\w{1,2}$", u"\\1", out_entity)
				w_type = text.named_entity_labels[count]
				
				if (sentence_count != prev_sen_num):
					nstory.insertSentence(self.graph, sentence_count)
					prev_sen_num = sentence_count
				
				nstory.insertWord(self.graph, sentence_count, out_entity, w_type, orig_text)

				count += 1
			nstory.hash = self.texthash(in_text)
			nstory.pushLocalGraph(self.graph)
			return True
		else:
			logging.error("text size exceeds limit! url: %s" % (article_url) )
			return False

	def getNstory(self, url):
		return Nstory.select(self.graph, url).first()

	def genTerms(self, url):

		added_terms = set()

		results = self.graph.data(
		"MATCH (nstory:Nstory {url: {inUrl} })--(sentence:Sentence)--(word:LocalWord) "
		"RETURN DISTINCT word.text as text, word.type as type, id(word) as id "
		"ORDER BY type " 
		, {'inUrl': url} 
		)
		persons = []
		for wordDict in results:
			if (wordDict['type'] == 'LOC' or wordDict['type'] == 'ORG'):
				added_terms.add( self.insertTerm(wordDict['text'], wordDict['type'], wordDict['id']) )
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
					added_terms.add( self.insertTerm(useName, person['type'], person['id']) )
				else:
					added_terms.add( self.insertTerm(person['text'], person['type'], person['id']) )
			else:
				#lookup if short name is surname
				if (person['text'].find('|') > 0):
					useName = person['text']
					for name in person['text'].split('|'):
						match = next((item for item in persons if item["surname"] == name), None)
						if (match) :
							useName = match['text']
							break
					added_terms.add( self.insertTerm(useName, person['type'], person['id']) )
				else:
					useName = person['text']
					match = next((item for item in persons if item["surname"] == person['text']), None)
					if (match) :
						useName = match['text']
					added_terms.add( self.insertTerm(useName, person['type'], person['id']) )
		return added_terms

	def checkForLocalWords(self, url):

		newsNode = self.graph.find_one('Nstory', property_key='url', property_value=url)
		sen_count = 0
		for rel in self.graph.match(start_node=newsNode, rel_type="HAS"):
			sentence = rel.end_node()
			sen_count += 1
			sWordRels = self.graph.match(start_node=sentence, rel_type="HAS")
			if (next(sWordRels, None)  == None):
				logging.info("dead end sentence [%d] for url: %s ...fetching article" % (sentence['numInNstory'], url))
				self.delDeadEndSentences(url)
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
			"ON CREATE SET term.text = {wText}, term.type = {wType}, term.incoming = 0, term.fuzzy = 'true' "
			"WITH term "
			"MATCH (word:LocalWord) "
			"WHERE id(word) = {wId} "
			"MERGE (word)-[:IS]->(term) "
			, {'termId': term_id, 'wText': w_text, 'wType': w_type, 'wId': w_id}
			)
		else:
			self.graph.run(
			"MERGE (term:Term {id: {termId}}) "
			"ON CREATE SET term.text = {wText}, term.type = {wType}, term.incoming = 0 "
			"WITH term "
			"MATCH (word:LocalWord) "
			"WHERE id(word) = {wId} "
			"MERGE (word)-[:IS]->(term) "
			, {'termId': term_id, 'wText': w_text, 'wType': w_type, 'wId': w_id}
			)
		return term_id

	def updateNstoryUrl(self, old_url, new_url):
		if (self.getNstory(new_url)):
			logging.info("Deleting duplicate Nstory with url %s " % (old_url) )
			dupeNstory = self.getNstory(old_url)
			if (dupeNstory):
				self.graph.delete(dupeNstory)
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

	def setErrorNstory(self, url):

		results = self.graph.run(
		"MATCH (n:Nstory {url: {inUrl} }) "
		"REMOVE n:Nstory "
		"SET n:ErrorNstory "
		, {'inUrl': url} 
		)

	def delDeadEndSentences(self, url):
		results = self.graph.data(
		"MATCH (n:Nstory {url: {inUrl} })-->(s:Sentence) "
		"WHERE NOT (s)-->(:LocalWord) "
		"WITH DISTINCT s LIMIT 50 "
		"DETACH DELETE(s) "
		"RETURN count(*) as del_count "
		, {'inUrl': url} 
		)
		if (len(results) > 0):
			return results[0]['del_count']
		else:
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
