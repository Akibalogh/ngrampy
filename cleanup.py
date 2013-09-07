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
import functools

def memo(f):
    ''' Memoize function ''f''. '''
    cache = {}
    @functools.wraps(f)
    def wrap(*args):
        if args not in cache:
            cache[args] = f(*args)
        return cache[args]
    return wrap

# Only letters from A to Z, optionally, allow a single hyphen (-) or
# an apostrophy (') in the middle (not at either end).
# Also allow a single dot (.) in the front: ".NET", ".com"
english_word_pattern = r"\.?[a-zA-Z]+(?:[-'][a-zA-Z]+)*"

# Same as ''english_word_pattern'' but add abbreviations formed by replacing
# part of a word with a single period (.). For example:
# "U.S.A", "U.S.A.", "Mrs.", "Prof.". (The "." at the end seems optional)
english_word_with_abbrev_pattern = r"[a-zA-Z]+(?:\.[a-zA-Z])*\.?"

# Either plain word or with abbreviation
english_and_abbrev_pattern = "(?:" + english_word_pattern + ")" + "|" \
    + "(?:" + english_word_with_abbrev_pattern + ")"

##
# These compiled regex generator functions are memoized so it should be fast.
#
@memo
def ngram_re(num, word_pattern):
    ''' Get a compiled regex for ''num'' grams. It joins the
    ''word_pattern'' for ''num'' times with a single space. '''
    pat = "^" + ' '.join([word_pattern] * num) + "$"
    return re.compile(pat)

@memo
def google_dictionary_pattern(word_pattern):
    ''' Get a compiled regex for the google dictionary notation (dot notation)
    using ''word_pattern'' for the word part. '''
    dot_pattern = r"(?:\.\d+)?(?:_(?:NUM|VERB|DET|NOUN|ADP|X|ADJ|ADV|PRON|PRT|CONJ|\.))?"
    # "'", "-" and "." cannot be included because they have special meanings.
    punctuations = "[~!@#$%^&*()\]\[{}:\"<>?,/\\|=+0-9]*"
    return punctuations + "(" + word_pattern + ")" + punctuations + dot_pattern

def english_words_only(gram, year, count):
    ''' Works with n-grams. '''
    parts = re.split(r" +", gram)
    compiled_pat = ngram_re(len(parts), google_dictionary_pattern(
            english_and_abbrev_pattern))
    matched = compiled_pat.match(gram)
    if matched:
        return (' '.join(matched.groups()), year, count)
    else:
        return (None, year, 0)

def test_regex():
    test_cases = [
        ("Abundent", "Abundent"),
        ("accent", "accent"),
        ("wEiRD", "wEiRD"),
        ("IMPORTANT", "IMPORTANT"),
        ("Mrs.", "Mrs."),
        ("Ph.D.", "Ph.D."),
        ("U.S.A", "U.S.A"),
        ("knock-down", "knock-down"),
        ("slowly.5_ADV", "slowly"),
        ("Question_NUM", "Question"),
        ("Prof..5_NOUN", "Prof."),
        ("M.D.12_X", "M.D."),
        ("OK,", "OK"),
        ("question5_NOUN", "question"),
        ("Hi!.22_X", "Hi"),
        ("$dollar", "dollar"),
        ("#@%^tune()*&", "tune"),
        ("?string++", "string"),
        ("!string12", "string"),
        ("!string4.5_VERB", "string"),
        (".com_NOUN", ".com"),
        (".com.5_X", ".com"),
        ("good.2_ADJ", "good"),
        # These don't work: Do we care?
        # ".", "-", "'" are reserved (for google notation and abbreviations)
        ("..vegan-.", None),
        ("vegan_'", None),
        ("'ve", None),
        ("smooth.latte", None), # really a two gram
        ]
    for inp, expected in test_cases:
        actual = english_words_only(inp, 2000, 1)[0]
        if not actual == expected:
            print "'%s': Fail: Expected: '%s' Actual '%s'" % (inp, expected, actual)
    return

## Filter by an external dictionary
def dictionary_words_only(dictionary):
    def dictionary_filter(gram, year, count):
        ''' Filter out everything except words that are in a dictionary. '''
        return (gram, year, count) \
            if all([g in dictionary for g in gram.split(' ')]) \
            else ('', year, 0)
    return dictionary_filter
 
conf_min_year = 1950
conf_min_count = 10

##
# Order matters: For example, the minimus year function is the first because
# it is fast and filters out a lot of things; the lower case conversion is last
# because it is not needed unless the ngram is to be kept.
#
filters = [
    lambda g, y, c: (g, y, 0 if y < conf_min_year else c), # minimum year
    english_words_only,
    lambda g, y, c: (g.lower(), y, c), # to lower case
    ]

def process_line(line, filters, results):
    """ Process a 'line' of ngram data, applying each filter in 'filters'
    list, and put the result in 'results' dictionary. """
    line = line.strip()
    #sys.stderr.write(">>> %s\n" % line)
    gram, year, count, _ = re.split('\t+', line)
    year = int(year)
    count = int(count)
    for f in filters:
        if count == 0: # No need to apply subsequent filters
            return
        gram, year, count = f(gram, year, count)
    if gram not in results:
        results[gram] = 0
    #sys.stderr.write("\t\t%s %d %d\n" % (gram, year, count))
    results[gram] += count

def process_ngram_data(fin):
    """ Process data from file object ''fin''. """
    ngrams = {}
    for line in fin:
        process_line(line, filters, ngrams)
    for g in sorted(ngrams.keys()):
        c = ngrams[g]
        if c >= conf_min_count:
            print "%s\t%d" % (g, c) 

if __name__ == "__main__":
    #test_regex()
    process_ngram_data(sys.stdin)
