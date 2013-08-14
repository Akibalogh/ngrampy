"""
	This downloads from google all of the files matching a pattern on the Google Books Ngram Download page. 
"""

import httplib2
from BeautifulSoup import BeautifulSoup, SoupStrainer
import re
import os
import urllib

# Use httplib2 and BeautifulSoup to scrape the links from the google index page:
http = httplib2.Http()
status, response = http.request('http://storage.googleapis.com/books/ngrams/books/datasetsv2.html')

for link in BeautifulSoup(response, parseOnlyThese=SoupStrainer('a')):
	if link.has_key('href'):
		url = link['href']
		
		# IF we match 1-3-grams with the selected dataset
		#if re.search("[123]gram.+20120701", url):
		if re.search("[12]gram.+20120701", url):
			# Decode this
			m = re.search(r"googlebooks-([\w\-]+)-(\d+)gram.+",url)
			language, n_gram = m.groups(None)
	
			# Only download some language
			if language not in set(["eng-us-all"]): continue
			#if language not in set(["eng-us-all","eng-gb-all", "fre-all", "ger-all", "heb-all", "ita-all", "rus-all", "spa-all" ]): continue

			# Only download some n-grams
			#if n_gram not in set(["1", "2", "3"]): continue
			if n_gram not in set(["1", "2"]): continue

			filename = re.split(r"/", url)[-1] # last item on filename split
			
			# Make the directory if it does not exist
			if not os.path.exists(language):       os.mkdir(language)
			if not os.path.exists(language+"/"+n_gram): os.mkdir(language+"/"+n_gram)
			
			if not os.path.exists(language+"/"+n_gram+"/"+filename):
				#pass
				print "# Downloading %s to %s" % (url, language+"/"+n_gram+"/"+filename)
				urllib.urlretrieve(url, language+"/"+n_gram+"/"+filename )
