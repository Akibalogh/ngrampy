import sys
import os
import urllib
import fcntl # for file locking
import cleanup # "./cleanup.py"
import subprocess
import signal # for clean exit upon Ctrl-C
import time
import shutil
import re

def retry(try_fn, giveup_fn, exception_list, n, wait=0):
    ''' Try function ''try_fn'', catching exceptions in ''exceptions_list'',
    if any occurred, and retry for up to ''n'' times, then give up and call
    ''giveup_fn''. Sleeps ''wait'' seconds between retries.'''
    if not n:
        # used up retries, give up
        giveup_fn()
        return False
    try:
        try_fn()
        return True
    except exception_list as e:
        print "Got %s, retrying in %f seconds" % (e, wait)
        time.sleep(wait)
        retry(try_fn, giveup_fn, exception_list, n - 1, wait)


topdir = "."
workarea = os.path.join(topdir, "workarea")
resultsarea = os.path.join(topdir, "results")
index_file_name = os.path.join(workarea, "unprocessed.txt")
success_file = os.path.join(workarea, "succeeded-ngrams.txt")
failure_file = os.path.join(workarea, "failed-ngrams.txt")

def init_index(ngram_numbers):
    ''' Do this once in the beginning. Adds all ngram file names to the
    index file. '''
    # create directories
    if not os.path.exists(workarea):
        os.mkdir(workarea)
    # clear status files
    clear_file = lambda f: open(f, "w").close()
    map(clear_file, [success_file, failure_file])
    if not os.path.exists(resultsarea):
        os.mkdir(resultsarea)
    fo = open(index_file_name, "w")
    # Output in Python list format
    fo.write("[")
    # Do it in reverse order since we will be popping them out from back
    for n in sorted(ngram_numbers, reverse=True):
        fname = os.path.join(workarea, "%dgrams.txt" % n)
        ngram_file_prefixes = open(fname).read().strip().split(' ')
        for prefix in sorted(ngram_file_prefixes, reverse=True):
            fo.write('"%d %s", ' % (n, prefix))
    fo.write("]\n")
    fo.close()

def display_status():
    ''' Displays current ngram-processing status. '''
    # Succeeded
    if os.path.exists(success_file):
        num = len(set(line for line in open(success_file)))
        print "Successes: %d" % num
    if os.path.exists(failure_file):
        num = len(set(line for line in open(failure_file)))
        print "Failures: %d" % num
    fo = open(index_file_name)
    lst = eval(fo.read())
    num = len(lst) # Number of unprocessed ngram files
    for item in lst:
        n, prefix = item.split(' ')
        assert(int(n) in [1, 2, 3, 4, 5])
    fo.close()
    print "%d unprocessed ngram files left" % num
    if num:
        n, prefix = lst[-1].split(' ')
        print "Next in line: %sgram-%s" % (n, prefix)

def mklock():
    fpath = os.path.join(workarea, "index_lock")
    fo = open(fpath, "w")
    def lock():
        #print "locking..."
        fcntl.lockf(fo, fcntl.LOCK_EX)
    def unlock():
        #print "unlocking..."
        fcntl.lockf(fo, fcntl.LOCK_UN)
    def destroy():
        #print "destroying..."
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

def get_ngram_file(n, prefix):
    ''' Returns a file object to a ngram file. The ngram file is downloaded
    if needed. '''
    fname = "googlebooks-eng-all-%dgram-20120701-%s" % (n, prefix)
    dstpath = os.path.join(workarea, fname)
    if os.path.exists(dstpath):
        return (dstpath, open(dstpath))
    # Need to get .gz file and gunzip it.
    fname_gz = fname + ".gz"
    srcpath = os.path.join(workarea, fname_gz)
    if not os.path.exists(srcpath):
        urllib.urlretrieve(
            "http://storage.googleapis.com/books/ngrams/books/" + fname_gz,
            srcpath)
        print "Downloaded %s" % srcpath
    # Pipe the unzipped stream to save disk space
    subp = subprocess.Popen(['gzip', '-dc', srcpath], stdout=subprocess.PIPE)
    return (os.path.join(workarea, fname_gz), subp.stdout)

def load_into_db(ngram_file, n, prefix):
    ''' load ngrams into MySQL. '''
    # Import file name must be the same as database table name. So put it under
    # a uniquely named tmp directory to avoid conflict.
    tmpdir = os.path.join(workarea, "%dgram-%s" % (n, prefix))
    os.mkdir(tmpdir)
    outfpath = os.path.abspath(os.path.join(tmpdir, "ngram_%d" % n))
    shutil.copyfile(ngram_file, outfpath)
    cols = ','.join(["gram%d" % i for i in range(n)] + ['frequencies'])
    cmdargs = ['mysqlimport', '-u', 'www-data',
               "--password='Generation1234!'",
               '--local', '--fields-terminated-by="\\t"',
               '--columns=%s' % cols,
               'google_ngram', outfpath]
    cmdargs_str =  ' '.join(cmdargs)
    print cmdargs_str
    # For some reason, I have to do one big command string and set shell=True
    # for this to work.
    ret = True
    try:
        subprocess.check_call(cmdargs_str, shell=True)
    except subprocess.CalledProcessError as e:
        print "Failed to import %s (%d gram) to MySQL: %s" % (outfpath, n, e)
        ret = False
    finally:
        os.unlink(outfpath)
        os.rmdir(tmpdir)
    return ret

def epl(n, prefix):
    ''' Main processing and loading routine. '''
    fpath, fin = get_ngram_file(n, prefix)
    ngrams = cleanup.process_ngram_data(fin)
    fin.close()
    # Delete raw input file to save space now that it is extracted.
    os.unlink(fpath)
    # Write result into results dir as plain text.
    result_file = os.path.join(resultsarea, "%dgram-%s" % (n, prefix))
    cleanup.output(ngrams, result_file)
    # load into MySQL
    #load_into_db(result_file, n, prefix)

def test_epl(n, prefix):
    ''' Test EPL. '''
    fname = "googlebooks-eng-all-%dgram-20120701-%s" % (n, prefix)
    fname_gz = fname + ".gz"
    fpath = os.path.join(workarea, fname_gz)
    furl = "http://storage.googleapis.com/books/ngrams/books/" + fname_gz
    print "Download: %s to %s" % (furl, fpath)
    header_path = os.path.join(workarea, "%d%s.hd" % (n, prefix))
    subprocess.check_call(['curl', '-D', header_path, '-o', '/dev/null', furl])
    fo = open(header_path % (n, prefix))
    for line in fo:
        line = line.strip()
        if (line.startswith("Content-Length: ")):
            length = line[len("Content-Length: "):]
            print "\t%s bytes" % length
    return

# Signal ''process()'' to clean exit.
stop_processing = False
def clean_exit(signum, frame):
    ''' Signal handler for clean exit. '''
    print "Got signal %d, finishing up current file and stopping..." % signum
    global stop_processing
    stop_processing = True

# Install handler for SIGINT
signal.signal(signal.SIGINT, clean_exit)

# Make sure lock is obtained
def log_result(n, prefix, success):
    fname = success_file if success else failure_file
    fo = open(fname, "a")
    fo.write("%d-%s\t%s\n" % (n, prefix, time.asctime()))
    fo.close()

def process(workfunc):
    ''' Calls 'cleanup()' to create a file. Then load the file into mysql. '''
    lock, unlock, destroylock = mklock()
    cnt = 0
    while not stop_processing:
        n, prefix = next_unprocessed_ngram_file(lock, unlock)
        if n is not None:
            cnt += 1
            try_fn = lambda: workfunc(n, prefix)
            def giveup_fn():
                print "Failed to get ngram file %d-%s!" % (n, prefix)
                gzfile = "googlebooks-eng-all-%dgram-20120701-%s.gz" % (n, prefix)
                try:
                    os.unlink(os.path.join(workarea, gzfile))
                except OSError:
                    pass
            result = retry(try_fn, giveup_fn,
                           (IOError, subprocess.CalledProcessError, OSError),
                           3, 1)
            lock()
            log_result(n, prefix, result)
            unlock()
        else:
            print "Hooray, All done!!!!!!"
            break
    destroylock()
    display_status()
    print "Processed %d ngrams files" % cnt
    return

def is_gram_in_db(n, g):
    cmdstr_fmt = """mysql -u www-data --password='Generation1234!' -B -e 'select count(*) from ngram_%d where gram0 like "%s%%";' google_ngram"""
    cmdstr = cmdstr_fmt % (n, g)
    output = subprocess.check_output(cmdstr, shell=True)
    return (int(output.split('\n')[1]) > 0)

def checkdb(n):
    ''' Check for omitted ngrams. '''
    fname = os.path.join(workarea, "%dgrams.txt" % n)
    ngram_file_prefixes = open(fname).read().strip().split(' ')
    for prefix in sorted(ngram_file_prefixes):
        if not is_gram_in_db(n, prefix):
            print "%d %s" % (n, prefix)

if __name__ == "__main__":
    cmd = sys.argv[1]
    if cmd == 'init':
        # Doing 1, 2, 3 -grams for now. (4, 5 grams later)
        init_index([1, 2, 3])
    elif cmd == 'status':
        display_status()
    elif cmd == 'run':
        process(epl)
    elif cmd == 'checkdb':
        #checkdb(1)
        #checkdb(2)
        checkdb(3)
    elif cmd == 'test':
        process(test_epl)
    elif cmd == 'load2db':
        if len(sys.argv) != 3:
            print "google2db.py load2db results-dir"
            exit(1)
        ngrams_results_dir = sys.argv[2]
        for ngram_file in sorted(os.listdir(ngrams_results_dir)):
            mobj = re.match(r'(\d+)gram-(\w+)$', ngram_file)
            if mobj:
                n, prefix = int(mobj.group(1)), mobj.group(2)
                print "Importing (%d %s) from %s" % (n, prefix, ngram_file)
                load_into_db(os.path.join(ngrams_results_dir, ngram_file), n, prefix)
            else:
                print "Bad result file name: %s" % (ngram_file)
    else:
        print "Usage: %s <init|run|status|checkdb|load2db|test>" % (sys.argv[0])
