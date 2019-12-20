import pickle
import duckdb
import numpy as np
import time
from threading import Thread
from typing import List
import tensorflow as tf
from queue import Queue
import sys
sys.path.append(".")
import bert as bm
from transformers import AutoModelForSequenceClassification, AutoTokenizer
from typing import List
import numpy as np
import torch.nn
import torch
from base import BaseModel
import json
import pymonetdb
import statistics
import sys


def preprocess(content):
	#Preprocess the content
	return content[20:200]


def get_queries():
	#Get the preprocessed queries
	queries = []
	with open("topics.data", 'rb') as filehandle:
		queries = pickle.load(filehandle)
	return queries

def get_content(raw_docs):
	#Get the content of documents
	keys = []
	values = []
	contents = {}
	with open(raw_docs) as json_file:
		contents = json_file.read()
		json_data = json.loads(contents)
		contents = {item['id']:item for item in json_data}
	return contents

def BM25(input_query,c,opt):
	#Rank documens using BM25, return up to 100 results.

	#tokenize the query
	query_words= ""
	for word in input_query.split():
			query_words = query_words + "'" + word.lower() + "',"
	query_words = query_words[:-1]

	#get the ids of the terms in the query
	c.execute("SELECT termid FROM dict WHERE term IN (" + query_words + ")")
	id_list = c.fetchall()

	#calculate BM25 scores using the database
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

	#Get the name of the document for each result
	if opt == 3:
		docids =str([i[0] for i in c.fetchall()])[1:-1]
		c.execute("SELECT name FROM docs WHERE docid IN (" + docids + ")")
		results = [i[0] for i in c.fetchall()]
	#If DuckDB is used, fetchnumpy() is more efficient than fetchall()
	else:
		docids =str(list(c.fetchnumpy()['docid']))[1:-1]
		c.execute("SELECT name FROM docs WHERE docid IN (" + docids + ")")
		results = c.fetchnumpy()['name']
	return results


if __name__ == '__main__':
 
 	#input from user
	raw_docs = sys.argv[1]
	db_name = sys.argv[2]
	option = int(sys.argv[3])
	bert_directory = sys.argv[4]

	#Get the queries
	queries = get_queries()
	#Store execution time for each query
	times = []
	#Write the results to results.txt
	results = open('results.txt','w')

	#DuckDB with Bert
	if option == 1:
		#Get the contents of the documents
		contents = get_content(raw_docs)
		#Inialize a BertModel instance
		bert_model = bm.PtBertModel(bert_directory)
		#connect to the database
		con = duckdb.connect(db_name)
		c = con.cursor()
		end_time = 0
		#For each query
		for item in queries:
			count = count + 1 ###
			query_no, query= item['number'],item["title"]
			results.write("Query: " + str(query_no) + "\n")
			#Start timing
			start_time = time.time() 
			#Get the candidate documents (max. 100) using BM25
			candidate_docs= BM25(query,c,1)
			num_results = len(candidate_docs)
			#Run BertModel if you have more than 10 documents returned
			if num_results > 10:
				choices = []
				for i in range(len(candidate_docs)):
					#Only get some part of the document and preprocess
					content  = contents[candidate_docs[i] + '.000000']['contents']
					content = preprocess(content)
					#Create a Choice object for each candidate document and store in a list
					choices.append(content)
				#Rerank the candidate documents using the BertModel
				ranked = bert_model.rank(query,choices)
				#End timing
				end_time = time.time()
				#Write top 10 results to results.txt
				for i in range(10):
					results.write(str(i+1) + ") " + candidate_docs[ranked[i]]+"\n")
			#If there are less than 10 documents returned, write them to results.txt without running Bert
			else:
				end_time = time.time()
				for i in range(num_results):
					results.write(str(i+1) + ") " + candidate_docs[i]+"\n")
			times.append(end_time - start_time)


	
	#If BertModel will not be used		
	else:
		con = None
		#Use DuckDB
		if option == 2:
			#Connect to database
			con = duckdb.connect(db_name)
			c = con.cursor()
			#For each query
			for item in queries:
				query_no, query= item['number'],item["title"]
				results.write("Query: " + str(query_no) + "\n")
				#Start timing
				start_time = time.time() 
				#Execute BM25
				bm25_results = BM25(query,c,2)
				#End timing
				end_time = time.time()
				#Write results to file
				num_results = 10 if len(bm25_results) > 10 else len(bm25_results)
				for i in range(num_results):
					results.write(str(i+1) + ") " + bm25_results[i]+"\n")
				times.append(end_time - start_time)
		#Use MonetDB
		else: 
			#Connect to database
			con = pymonetdb.connect(username='monetdb',password='monetdb',hostname='localhost', database=db_name)
			c = con.cursor()
			#For each query
			for item in queries:
				query_no, query= item['number'],item["title"]
				results.write("Query: " + str(query_no) + "\n")
				#Start timing
				start_time = time.time() 
				#Execute BM25
				bm25_results = BM25(query,c,3)
				#End timing
				end_time = time.time()
				#Write results to file
				num_results = 10 if len(bm25_results) > 10 else len(bm25_results)
				for i in range(num_results):
					results.write(str(i+1) + ") " + bm25_results[i]+"\n")
				times.append(end_time - start_time)

	#Print the timing information
	print("Max: " + str(max(times)))
	print("Min: " + str(min(times)))
	print("Average: " + str(sum(times)/len(times)))
	print("Standard Deviation: " + str(statistics.stdev(times)))
	print("Total: " + str(sum(times)))
	print("Number of queries: " + str(len(times)))

	#Close resuts.txt and the connection
	results.close()
	c.close()
	con.close()
