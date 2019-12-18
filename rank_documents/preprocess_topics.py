import re
import subprocess
import duckdb
import numpy as np
import pickle
import time
import sys
sys.path.append("..")
from src.main.python.topic_reader import TopicReader



if __name__ == "__main__" :
	path_to_topics = sys.argv[1]
	tr = TopicReader(path_to_topics)
	topics = tr.return_topics()
	with open('topics.data', 'wb') as filehandle:
	    pickle.dump(topics, filehandle)
