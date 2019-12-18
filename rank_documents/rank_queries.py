import pickle
import duckdb
import numpy as np
import time
from threading import Thread
from typing import List
import tensorflow as tf
from queue import Queue
import nboost.model.bert_model as bm
from nboost.model.bert_model import modeling, tokenization
from nboost.model.base import BaseModel
from nboost.types import Choice
import json
import pymonetdb
import statistics

def preprocess(content):
	return content[20:200]

def get_queries():
	#Get the preprocessed queries
	queries = []
	with open("topics.data", 'rb') as filehandle:
    	queries = pickle.load(filehandle)
    return queries

def get_content(raw_docs):
	keys = []
	values = []
	contents = {}
	with open(raw_docs) as json_file:
		contents = json_file.read()
		json_data = json.loads(contents)
		contents = {item['id']:item for item in json_data}
    return contents

def BM25(input_query,c):
	query_words= ""
	for word in input_query.split():
			query_words = query_words + "'" + word.lower() + "',"
	query_words = query_words[:-1]
	c.execute("SELECT termid FROM dict WHERE term IN (" + query_words + ")")
	id_list = c.fetchall()
	query_ids= ""
	for ids in id_list:
			query_ids = query_ids + str(ids[0]) + ","
	query_ids = query_ids[:-1]
	BM = """ WITH qterms AS (SELECT termid, docid, count as df FROM terms							 
		WHERE termid IN ("""+ query_ids +""")),										  
		subscores AS (SELECT docs.docid, length, term_tf.termid,						 
		tf, df, (log((528155.000000-df+0.5)/(df+0.5))*((term_tf.tf*(1.2+1)/						  
		(term_tf.tf+1.2*(1-0.75+0.75*(length/188.33)))))) AS subscore							
		FROM (SELECT termid, docid, df AS tf FROM qterms) AS term_tf				  
		JOIN (SELECT docid FROM qterms												
		GROUP BY docid )						   
		AS cdocs ON term_tf.docid = cdocs.docid									 
		JOIN docs ON term_tf.docid=docs.docid										 
		JOIN dict ON term_tf.termid=dict.termid)									  
		SELECT scores.docid, ROUND(score,6) FROM (SELECT docid, sum(subscore) AS score		   
		FROM subscores GROUP BY docid) AS scores JOIN docs ON						 
		scores.docid=docs.docid ORDER BY ROUND(score,6) DESC, scores.docid ASC LIMIT 100; """
	c.execute(BM)
	docids =str(list(c.fetchnumpy()['docid']))[1:-1]
	c.execute("SELECT name FROM docs WHERE docid IN (" + docids + ")")
	results = c.fetchnumpy()['name']
	return results


if __name__ == '__main__':

	raw_docs = sys.argv[1]
	db_name = sys.argv[2]
	option = int(sys.argv[3])
	

	queries = get_queries()
	times = []
	results = open('results.txt','w')

	#DuckDB with Bert
	if option == 1:
		contents = get_content(raw_docs)
		bert_model = bm.BertModel()
		con = duckdb.connect(db_name)
		c = con.cursor()
		for item in queries:
			query_no, query= item['number'],item["title"]
			results.write("Query: " + str(query_no) + "\n")
			start_time = time.time() 
			candidate_docs= BM25(query,c)
			choices = []
			for i in range(len(candidate_docs)):
				content  = contents[candidate_docs[i] + '.000000']['contents']
				content = preprocess(content)
				choices.append(Choice(i,content.encode('utf-8')))
			ranked = bert_model.rank(query.encode('utf-8'),choices)
			end_time = time.time()
			for i in range(10):
				results.write(str(i+1) + ") " + candidate_docs[ranked[i]]+"\n")
			times.append(end_time - start_time)
			
	else:
		con = None
		#DuckDB
		if option == 2:
			con = duckdb.connect(db_name)
		#MonetDB
		else: 
			con = pymonetdb.connect(username='monetdb',password='monetdb',hostname='localhost', database=db_name)
		c = con.cursor()
		for item in queries:
			query_no, query= item['number'],item["title"]
			start_time = time.time() 
			bm25_results = BM25(query,c)
			end_time = time.time()
			for i in range(10):
				results.write(str(i+1) + ") " + bm25_results[i]+"\n")
			times.append(end_time - start_time)

	print("Max: " + str(max(times)))
	print("Min: " + str(min(times)))
	print("Average: " + str(sum(times)/len(times)))
	print("Standard Deviation: " + str(statistics.stdev(times)))
	print("Total: " + str(sum(times)))
	print("Number of queries: " + str(len(times)))

	results.close()
	c.close()
	con.close()
