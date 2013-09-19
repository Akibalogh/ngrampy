import sys
import os
import urllib
import fcntl # for file locking
import cleanup # "./cleanup.py"
import tempfile
import subprocess
import signal # for clean exit upon Ctrl-C

index_file_name = "workarea/unprocessed.txt"

def init_index(ngram_numbers):
    ''' Do this once in the beginning. Adds all ngram file names to the
    index file. '''
    fo = open(index_file_name, "w")
    # Output in Python list format
    fo.write("[")
    # Do it in reverse order since we will be popping them out from back
    for n in sorted(ngram_numbers, reverse=True):
        fname = "workarea/%dgrams.txt" % n
        ngram_file_prefixes = open(fname).read().strip().split(' ')
        for prefix in sorted(ngram_file_prefixes, reverse=True):
            fo.write('"%d %s", ' % (n, prefix))
    fo.write("]\n")
    fo.close()

def display_status():
    fo = open(index_file_name)
    lst = eval(fo.read())
    num = len(lst) # Number of unprocessed ngram files
    for item in lst:
        n, prefix = item.split(' ')
        assert(int(n) in [1, 2, 3, 4, 5])
    fo.close()
    print "%d unprocessed ngram files left" % num

def mklock():
    fo = open("workarea/index_lock", "w")
    def lock():
        print "locking..."
        fcntl.lockf(fo, fcntl.LOCK_EX)
    def unlock():
        print "unlocking..."
        fcntl.lockf(fo, fcntl.LOCK_UN)
    def destroy():
        print "destroying..."
        fo.close()
    return (lock, unlock, destroy)

def next_unprocessed_ngram_file(lock, unlock):
    ''' Gets the next unprocessed ngram file pattern in the form of a
    2-tuple (n, prefix). This function takes care of file locking. '''
    lock()
    # remember the ngrams are in a python list
    idxfo = open(index_file_name, "r+")
    ngram_files = eval(idxfo.read())
    if not ngram_files:
        idxfo.close()
        unlock()
        return (None, None)
    # Pick the last one, a bit more efficient this way
    n, prefix = ngram_files.pop().split(' ')
    n = int(n)
    # Write new ngrams back
    idxfo.truncate(0)
    idxfo.seek(0)
    idxfo.write(str(ngram_files) + "\n")
    idxfo.close()
    unlock()
    return (n, prefix)

def get_ngram_file_path(n, prefix):
    ''' Returns a file object to a ngram file. The ngram file is downloaded
    if needed. '''
    fname = "googlebooks-eng-all-%dgram-20120701-%s" % (n, prefix)
    return fname

def epl(n, prefix):
    ''' Main processing and loading routine. '''
    fname = "googlebooks-eng-all-%dgram-20120701-%s" % (n, prefix)
    fname_gz = fname + ".gz"
    fpath = os.path.join("workarea", fname_gz)
    if not os.path.exists(fpath):
        # Need to download file first
        urllib.retrieve(
            "http://storage.googleapis.com/books/ngrams/books/" + fname_gz,
            fpath)
        subprocess.check_call(['gunzip', fpath])
    fin = open(fpath)
    ngrams = cleanup.process_ngram_data(fin)
    fin.close()
    # Delete raw input file to save space now that it is extracted.
    os.unlink(fname)
    fout = tempfile.NamedTemporaryFile()
    cleanup.output(ngrams, fout)
    # load into MySQL
    subprocess.check_call(['mysqlimport', '--local',
                           '--fields-terminated-by', "\t",
                           'GoogleNgrams', 'NgramsStats'])
    fout.close()

def test_epl(n, prefix):
    ''' Test EPL. '''
    fname = "googlebooks-eng-all-%dgram-20120701-%s" % (n, prefix)
    fname_gz = fname + ".gz"
    fpath = os.path.join("workarea", fname_gz)
    furl = "http://storage.googleapis.com/books/ngrams/books/" + fname_gz
    print "Download: %s to %s" % (furl, fpath)
#    subprocess.check_call(['curl', '-D', "workarea/%d%s.hd" % (n, prefix),
#                           '-o', '/dev/null', furl])
#    fo = open("workarea/%d%s.hd" % (n, prefix))
#    for line in fo:
#        line = line.strip()
#        if (line.startswith("Content-Length: ")):
#            length = line[len("Content-Length: "):]
#            print "\t%s bytes" % length
    return

# Signal ''process()'' to clean exit.
stop_processing = False
def clean_exit(signum, frame):
    ''' Signal handler for clean exit. '''
    print "Got signal %d, Cleaning up and stopping..." % signum
    global stop_processing
    stop_processing = True

# Install handler for SIGINT
signal.signal(signal.SIGINT, clean_exit)
    
def process(workfunc):
    ''' Calls 'cleanup()' to create a file. Then load the file into mysql. '''
    lock, unlock, destroylock = mklock()
    cnt = 0
    while not stop_processing:
        n, prefix = next_unprocessed_ngram_file(lock, unlock)
        if n is not None:
            cnt += 1
            workfunc(n, prefix)
        else:
            print "Hooray, All done!!!!!!"
            break
    destroylock()
    display_status()
    print "Processed %d ngrams files" % cnt
    return


if __name__ == "__main__":
    cmd = sys.argv[1]
    if cmd == 'init':
        # Doing 1, 2, 3 -grams for now. (4, 5 grams later)
        init_index([1, 2, 3])
    elif cmd == 'status':
        display_status()
    elif cmd == 'run':
        process(epl)
    elif cmd == 'test':
        process(test_epl)
    else:
        print "Usage: %s <init|run>" % (sys.argv[0])
