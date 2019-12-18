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
sudo apt update
sudo apt install -y  monetdb5-sql monetdb-client
sudo systemctl enable monetdbd
sudo systemctl start monetdbd
sudo usermod -a -G monetdb $USER
sudo rm /usr/lib/x86_64-linux-gnu/monetdb5/createdb/72_fits.sql
sudo rm /usr/lib/x86_64-linux-gnu/monetdb5/fits.mal
```
* [NBoost](https://github.com/koursaros-ai/nboost) is used to rerank the documents ranked by BM25. To install, use:
```
pip install nboost
```
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
  python mergejson.py output_filename path/to/raw/documents
  ```

