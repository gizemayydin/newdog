import pandas as pd
import numpy as py
import duckdb
import pymonetdb
import sys

def split_terms(path_to_csvs):
	#Since "terms" table is too large for DuckDB, we split it to chunks.
	df2 = pd.read_csv(path_to_csvs+"/terms.csv",sep='|',header = None)
	size_of_terms = df2.shape[0] + 100000
	indices = []
	for i in range(0,size_of_terms,100000):
		indices.append(i)
	for i in range(1,len(indices)):
		df2[indices[i-1]:indices[i]].to_csv(path_to_csvs+"/terms" + str(i) + ".csv",sep='|',index=False, header=None)
		print("created between " + str(indices[i-1]) + " and " + str(indices[i]))
	return size_of_terms


def create_table(c):
	c.execute("CREATE TABLE dict (termid int NOT NULL, term varchar(100), df int, PRIMARY KEY (termid));")
	c.execute("CREATE TABLE terms (termid int,docid int, count int);")
	c.execute("CREATE TABLE docs (name varchar(50), docid int NOT NULL, length int, PRIMARY KEY (docid));")

def create_index(c):
	c.execute("CREATE INDEX dict_index ON dict (termid);")
	c.execute("CREATE INDEX docs_index ON docs (docid);")
	c.execute("CREATE INDEX terms_index ON terms (termid);")

#In the version olddog currently uses, there are four columns in "docs" table.
#Since the fourth one is not used anymore, we drop the column.
def read_docs():
	docs = pd.read_csv(path_to_csvs+"/docs.csv",sep='|',header = None)
	docs.drop([3],axis=1,inplace=True)
	docs.to_csv(path_to_csvs+"/docs_modified.csv",sep='|',index=False, header=None)




#inputs from user (absolute path)
#path_to_csvs = "/home/gizem/olddog/createDB"
#1:Duck,2:DuckInd,3:Monet,4:MonetInd
#option_number = 3
#db name
#db_name = "trialdb"

if __name__ == '__main__':
	path_to_csvs = str(sys.argv[1])
	option_number = int(sys.argv[2])
	db_name = str(sys.argv[3])

	read_docs()

	#initialize c and con to dummy 
	c = 0
	con = 0
	
	#Create the Database
	if option_number == 1 or option_number == 2 :

		size_of_terms = split_terms(path_to_csvs)

		con = duckdb.connect(db_name) #stores db info in filename
		c = con.cursor()

		#create database
		create_table(c)

		#insert data
		c.execute("COPY dict FROM '"+ path_to_csvs +"/dict.csv'  DELIMITER '|'")
		c.execute("COPY docs FROM '"+ path_to_csvs +"/docs_modified.csv' DELIMITER '|'")
		for i in range(1,int(size_of_terms/100000) + 1):
			query = "COPY terms FROM '"+ path_to_csvs +"/terms" + str(i) + ".csv' DELIMITER '|'"
			c.execute(query)
			print("Completed " + str(i) + " of " + str(size_of_terms/100000))

		if option_number == 2:
			create_index(c)

	elif option_number == 3 or option_number == 4:
		con = pymonetdb.connect(username='monetdb',password='monetdb',hostname='localhost', database=db_name)
		c = con.cursor()

		#create database
		create_table(c)

		#insert data
		c.execute("COPY INTO dict FROM '"+ path_to_csvs +"/dict.csv' DELIMITERS '|';")
		c.execute("COPY INTO docs FROM '"+ path_to_csvs +"/docs_modified.csv' DELIMITERS '|';")
		c.execute("COPY INTO terms FROM '"+ path_to_csvs +"/terms.csv' DELIMITERS '|';")

		if option_number == 4:
			create_index(c)

	c.close()
	con.close()








