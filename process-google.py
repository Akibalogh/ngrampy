"""
	Note: This appears to be quicker than using "import gzip", even if we copy
	the gziped file to a SSD before reading. It's also faster than trying to change buffering on stdin
"""

import os
import re
import datetime
from datetime import datetime
import sys
import codecs
sys.stdin = codecs.getreader(sys.stdin.encoding)(sys.stdin)
import redis
import argparse
parser = argparse.ArgumentParser(description='MCMC for magic box!')
parser.add_argument('--out', dest='out-path', type=str, default="./tmp/", nargs="?", help='The file name for output (year appended)')
parser.add_argument('--year-bin', dest='year-bin', type=int, default=25, nargs="?", help='How to bin the years')
args = vars(parser.parse_args())

MINIMUM_POPULARITY_FILTER = 100
MINIMUM_YEAR = 1950
cleanup = re.compile(r"(\")") # kill quotes only
#cleanup = re.compile(r"(_[A-Za-z\_\-]+)|(\")") # kill tags and quotes

YEAR_BIN = int(args['year-bin'])
BUFSIZE = int(10e6) # We can allow huge buffers if we want...

prev_year, year, prev_ngram, ngram = None, None, None, None
count = 0
year2file = dict()
part_count = None

r = redis.StrictRedis(host='localhost', port=6379, db=0)
#r.set_response_callback('GET', int)
start = datetime.now()

column_splitter = re.compile(r"[\t\s]") # split on tabs OR spaces, since some of google seems to use one or the other. 

for l in sys.stdin:
	l = l.lower().strip()
	l = cleanup.sub("", l)  
	#l = re.sub(r"\"", "", l) # remove quotes
	#l = re.sub(r"_[A-Z\_\-]+", "", l) # remove tags

	
	parts = column_splitter.split(l)
	if part_count is None: part_count = len(parts)
	if len(parts) != part_count: continue # skip this line if its garbage NOTE: this may mess up with some unicode chars?
	
	i = int(parts[-2])
	prev_year = year
	year = int(int(parts[-3]) / YEAR_BIN) * YEAR_BIN # round the year
	prev_ngram = ngram
	ngram = "\t".join(parts[0:-3]) # join everything else
	ngram = ngram.lstrip().rstrip()

	#print "new ngram: ", ngram, " year ", year, " | prev: ", prev_ngram, " count ", count
	
	# ngram has changed so print the cumulative total
	if (ngram != prev_ngram and prev_ngram is not None):
		#print "about to set. ngram ", prev_ngram, " got to: ", count
		# Apply filters
		if count >= MINIMUM_POPULARITY_FILTER:
			#year2file[prev_year].write(  "%s\t%i\n" % (prev_ngram,count)  ) # write the year
			#print "%s\t%i\n" % (prev_ngram,count)
			if (r.get(prev_ngram) is None):
				r.set(prev_ngram, count)
				#print "set ngram: ", prev_ngram, " to ", count
			else:
				print "found existing ngram ", prev_ngram, " at ", r.get(prev_ngram), " when trying to set ", ngram, " to ", count
				#raise Exception('ValueError', 'ngram %s already exists in redis' % (prev_ngram)
		count = i
	
	#if ("quasvis" in ngram):
	#	print ngram, " | ", count, " | ", l	
	
	if year >= MINIMUM_YEAR:
		count += i

	# year2file[prev_year] = open(args['out-path']+".%i"%prev_year, 'w', BUFSIZE)
	
	
# And write the last line if we didn't alerady!
if year == prev_year and ngram == prev_ngram:
	print "insert last line ", ngram, " ", count
	
	# Apply filters
	if prev_year >= MINIMUM_YEAR and count >= MINIMUM_POPULARITY_FILTER:
		#year2file[prev_year].write("%s\t%i\n"%(ngram, c)) # write the year
		#print "%s\t%i\n"%(ngram, c)
		r.set(ngram, count)

# And close everything
for year in year2file.keys():
	year2file[year].close()

end = datetime.now()
print "total runtime: ", end - start
