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
from peewee import *
#import redis
import argparse
parser = argparse.ArgumentParser(description='MCMC for magic box!')
parser.add_argument('--out', dest='out-path', type=str, default="./tmp/", nargs="?", help='The file name for output (year appended)')
parser.add_argument('--year-bin', dest='year-bin', type=int, default=25, nargs="?", help='How to bin the years')
args = vars(parser.parse_args())

MINIMUM_POPULARITY_FILTER = 5000
MINIMUM_YEAR = 1950
YEAR_BIN = int(args['year-bin'])
BUFSIZE = int(10e6) # We can allow huge buffers if we want...

prev_year, year, prev_ngram, ngram = None, None, None, None
count = 0
all_ngrams = dict()
year2file = dict()
part_count = None
cleanup = re.compile(r"(\")") # kill quotes only
column_splitter = re.compile(r"[\t\s]") # split on tabs OR spaces, since some of google seems to use one or the other. 

#r = redis.StrictRedis(host='localhost', port=6379, db=0)
#r.set_response_callback('GET', int)
start = datetime.now()
#mysql_db = MySQLDatabase('ngrams')
#mysql_db.connect()

#class MySQLModel(Model):
#    class Meta:
#        database = mysql_db

# SET WHETHER NGRAMS SHOULD HAVE TAGS OR NOT
#class NGram_tags(MySQLModel):
#    key = CharField(max_length=50, null=False)
#    value = IntegerField()

#cleanup = re.compile(r"(_[A-Za-z\_\.\-]+)|(\")") # kill tags and quotes
cleanup = re.compile(r"(\.[A-Za-z\_\-]+)|(\")") # kill dot notation and quotes

for l in sys.stdin:
	l = l.lower().strip()
	l = cleanup.sub("", l)  
	l = re.sub(r"\"", "", l) # remove quotes

	parts = column_splitter.split(l)
	if part_count is None: part_count = len(parts)
	if len(parts) != part_count: continue # skip this line if its garbage NOTE: this may mess up with some unicode chars?
	i = int(parts[-2])
	prev_year = year
	year = int(int(parts[-3]) / YEAR_BIN) * YEAR_BIN # round the year
	prev_ngram = ngram
	ngram = "\t".join(parts[0:-3]) # join everything else
	ngram = ngram.lstrip().rstrip()

	# ngram has changed so print the cumulative total
	if (ngram != prev_ngram and prev_ngram is not None):
		#print "about to set. ngram ", prev_ngram, " got to: ", count
		# Apply filters
		if count >= MINIMUM_POPULARITY_FILTER:
			#year2file[prev_year].write(  "%s\t%i\n" % (prev_ngram,count)  ) # write the year
			if (all_ngrams.has_key(prev_ngram)):
				all_ngrams[prev_ngram] = all_ngrams[prev_ngram] + count
				sys.stderr.write('found existing ngram ' + prev_ngram + ' and added ' + str(count) + ' to make ' + str(all_ngrams[prev_ngram]) + '\n')
				#if (all_ngrams[prev_ngram] < count):
                                #	all_ngrams[prev_ngram] = count
				#	sys.stderr.write('found existing ngram ' + prev_ngram + ' and replaced with ' + count + '\n')
				#else:
				#	sys.stderr.write('found existing ngram ' + prev_ngram + ' but ignored because it was ' + count + '\n')
			else:
				print '"%s"\t%i' % (prev_ngram, count)
				all_ngrams[prev_ngram] = count
			#try:
				# Get a single record
				#ngram_get = NGram_tags.get(NGram_tags.key == prev_ngram)
				#print "found existing ngram ", prev_ngram, " at ", ngram_get.key, " when trying to set ", ngram, " to ", count
				#raise Exception('ValueError', 'ngram %s already exists in redis' % (prev_ngram)
			#except NGram_tags.DoesNotExist:
				#print "set ngram: ", prev_ngram, " to ", count
				#ngram_insert = NGram_tags.insert(key = prev_ngram, value = count)
				#ngram_insert.execute()
				#r.set(prev_ngram, count)

		count = 0
	
	#if ("quasvis" in ngram):
	#	print ngram, " | ", count, " | ", l	
	
	if year >= MINIMUM_YEAR:
		count += i

	# year2file[prev_year] = open(args['out-path']+".%i"%prev_year, 'w', BUFSIZE)
	
	
# And write the last line if we didn't alerady!
if year == prev_year and ngram == prev_ngram:
	sys.stderr.write('insert last line ' + ngram + '\n')
	
	# Apply filters
	if prev_year >= MINIMUM_YEAR and count >= MINIMUM_POPULARITY_FILTER:

		if (all_ngrams.has_key(prev_ngram)):
                	sys.stderr.write('found existing ngram ' + ngram + '\n')
                else:
                	print '"%s"\t%i' % (ngram, count)
               		all_ngrams[ngram] = count
		#year2file[prev_year].write("%s\t%i\n"%(ngram, c)) # write the year
		#ngram_insert = NGram_tags.insert(key = ngram, value = count)
                #ngram_insert.execute()
		#r.set(ngram, count)

# And close everything
#for year in year2file.keys():
#	year2file[year].close()

end = datetime.now()
sys.stderr.write('total runtime: ' + str(end - start))
