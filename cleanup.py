##
# Clean up raw google ngram data
#
# Google ngram data format according to there website:
# (http://storage.googleapis.com/books/ngrams/books/datasetsv2.html)
#
# ngram TAB year TAB match_count TAB volume_count NEWLINE
#
# DOT notation:
# ngram DOT [NUMBER] UNDERSCORE PART_OF_SPEECH
# or
# ngram UNDRESCORE PART_SPEECH
#
# For example:
# "question.10_NOUN"
# "quasidomicile_ADJ"
#
# The Google ngram data is "mostly" sorted: It is sorted but sometimes an
# entry are not located consecutively.
#
# This program process the raw google ngram input one line at a time and applies
# a series of filters to the ngram (for example, convert to lowercase, remove
# part-of-speech meta data). Each clean ngram is then stored in a Python
# dictionary with the ngram as the key and (frequency) count as the value.
#
# The result will eventually be stored into a database (SQL or NOSQL).
#

import re
import sys

def process_line(line, filters, results):
    """ Process a 'line' of ngram data, applying each filter in 'filters'
    list, and put the result in 'results' dictionary. """
    line = line.strip()
    #sys.stderr.write(">>> %s\n" % line)
    gram, year, count, _ = re.split('\t+', line)
    year = int(year)
    count = int(count)
    for f in filters:
        gram, year, count = f(gram, year, count)
    if gram not in results:
        results[gram] = 0
    #sys.stderr.write("\t\t%s %d %d\n" % (gram, year, count))
    results[gram] += count
 
conf_min_year = 1950
conf_min_count = 10

part_of_speech_pattern = re.compile \
    (r"^(\S+)(\.\d+)?_(num|verb|det|noun|adp|x|adj|adv|pron|prt|conj)$")

google_dictionary_pattern = re.compile \
    (r"^(\S+)(\.\d+)$")

def remove_dot_notation(gram, year, count):
    ''' Throw away dot notation. '''
    match = part_of_speech_pattern.match(gram)
    if match:
        gram = match.group(1)
    match = google_dictionary_pattern.match(gram)
    if match:
        gram = match.group(1)
    return (gram, year, count)

filters = [
    lambda g, y, c: (g.lower(), y, c), # to lower case
    lambda g, y, c: (g, y, 0 if y < conf_min_year else c), # minimum year
    remove_dot_notation
    ]

def process_ngram_data(fin):
    """ Process data from file object ''fin''. """
    ngrams = {}
    for line in fin:
        process_line(line, filters, ngrams)
    for g in sorted(ngrams.keys()):
        c = ngrams[g]
        if c >= conf_min_count:
            print "%s\t%d" % (g, c) 

process_ngram_data(sys.stdin)
