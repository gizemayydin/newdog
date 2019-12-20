# NewDog

TODO: introduction

## Preliminaries

* Build [OldDog](https://github.com/Chriskamphuis/olddog) using Maven:
```
mvn clean package appassembler:assemble
```
* OldDog takes a Lucene index as input, for example as created by the [Anserini](https://github.com/castorini/Anserini) project. Below, there is an example command to create index for Robust04 collection using Anserini.

```
nohup sh target/appassembler/bin/IndexCollection -collection TrecCollection -input /path/to/collecion  -index /lucene-index.robust04.pos+docvectors+rawdocs -generator JsoupGenerator -threads 1  -storePositions -optimize -storeDocvectors -storeRawDocs >& log.robust04.pos+docvectors+rawdocs &
```

* Both [MonetDB](https://www.monetdb.org/) and [DuckDB](https://www.duckdb.org/) is used. To install both, use (for Ubuntu):
```
pip install duckdb
```
```
echo "deb https://dev.monetdb.org/downloads/deb/ $(lsb_release -cs) monetdb" >> /etc/apt/sources.list.d/monetdb.list
echo "deb-src https://dev.monetdb.org/downloads/deb/ $(lsb_release -cs) monetdb" >> /etc/apt/sources.list.d/monetdb.list
wget --output-document=- https://www.monetdb.org/downloads/MonetDB-GPG-KEY | sudo apt-key add -
sudo apt update
sudo apt install -y  monetdb5-sql monetdb-client
sudo systemctl enable monetdbd
sudo systemctl start monetdbd
sudo usermod -a -G monetdb $USER
sudo rm /usr/lib/x86_64-linux-gnu/monetdb5/createdb/72_fits.sql
sudo rm /usr/lib/x86_64-linux-gnu/monetdb5/fits.mal
```
* [NBoost](https://github.com/koursaros-ai/nboost) is used to rerank the documents ranked by BM25. The files "base.py", "logger.py" and "bert.py" are obtained directly from [NBoost](https://github.com/koursaros-ai/nboost) repository [1]. To run the BERT model, please download and unzip the specifications from this link: https://storage.googleapis.com/koursaros/pt-bert-base-uncased-msmarco.tar.gz [1]

In case of errors, check alternative ways to install the packages from their respective GitHub repositories. 

## Setup
* Create the CSV files representing the databases using OldDog:
```
nohup target/appassembler/bin/nl.ru.convert.Convert -index path/to/index -docs /tmp/docs.csv -dict /tmp/dict.csv -terms /tmp/terms.csv
```
* If you are using MonetDB, first create the database manually:
```
sudo monetdbd create /var/monetdb5/dbfarm
sudo monetdbd start /var/monetdb5/dbfarm
monetdb create db_name
monetdb release db_name
```
* To create and fill the tables:
```
python create_db/create_db.py path/to/csvs option db_name
```
Where ```option``` is: 1:DuckDB 2:DuckDB+index 3:MonetDB 4:MonetDB+index
 
* If BERT model of NBoost will be used, follow these steps:
  * Create JSON collection of your documents using Anserini. [Example](https://github.com/castorini/anserini/blob/master/src/main/python/passage_retrieval/example/robust04.md).
  * Run:
  ```
  python rank_documents/mergejson.py output_filename path/to/raw/documents
  ```
## Ranking

* Preprocess the queries to be executed and store it in a file:
```
python rank_documents/preprocess_topics.py /path/to/topics 
```
* Ranking the documents using BM25:
```
python rank_documents/rank_queries.py output_filename db_name option bert_directory
```
```output_filename``` is the one used in Setup to creae JSON collection of the documents. If you are not planning to use BERT, you may leave it empty.
```db_name``` is the name of the database you want to connect.
```option``` is 1:DuckDB + Bert, 2:DuckDB, 3:MonetDB
```bert_directory``` is the absolute path of the file you downloaded and unzipped from https://storage.googleapis.com/koursaros/pt-bert-base-uncased-msmarco.tar.gz [1]

# References

[1] Cole Thienes and Jack Pertschuk. 2019. NBoost: Neural Boosting Search Results.https://github.com/koursaros-ai/nboost.

