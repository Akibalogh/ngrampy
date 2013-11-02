import subprocess
import os
import shutil

def result_to_set(fname):
    fo = open(fname)
    result = set()
    for i in fo:
        result.add(i.strip())
    fo.close()
    return result

eagle_result = "3grams-eagle.txt"
informite_result = "3grams-imported.txt"
required = "workarea/3grams.txt"
def process():
    eagle_set = result_to_set(eagle_result)
    informite_set = result_to_set(informite_result)
    required_set = set(open(required).read().split())
    omitted_set = required_set - (eagle_set | informite_set)
    to_be_imported_set = eagle_set - informite_set
    #print ">>> To be imported from eagle:\n", to_be_imported_set
    #print ">>> 111 >>> omitted set:", omitted_set
    # Now verify DB that omitted is *really* omitted.
    import_list = sorted([g for g in to_be_imported_set if not is_gram_in_db(g)])
    omitted_list = sorted([g for g in omitted_set if not is_gram_in_db(g)])
    return (import_list, omitted_list)

def is_gram_in_db(g):
    cmdstr_fmt = """mysql -u root -B -e 'select count(*) from ngram_3 where gram0 like "%s%%";' google_ngram"""
    cmdstr = cmdstr_fmt % g
    output = subprocess.check_output(cmdstr, shell=True)
    return (int(output.split('\n')[1]) > 0)

def load_into_db(fpath, gram_n):
    cols = ','.join(["gram%d" % i for i in range(gram_n)] + ['frequencies'])
    cmdargs = ['mysqlimport', '-u', 'root',
               '--local', '--fields-terminated-by="\\t"',
               '--columns=%s' % cols,
               'google_ngram', fpath]
    cmdargs_str =  ' '.join(cmdargs)
    print cmdargs_str
    # For some reason, I have to do one big command string and set shell=True
    # for this to work.
    subprocess.check_call(cmdargs_str, shell=True)


if __name__ == "__main__":
    (import_list, omitted_list) = process()
    print "To be imported:"
    print import_list
    print ">>>> Start importing"
    for g in import_list:
        srcfpath = "/home/ning/eagle-results/results/3gram-%s.txt" % g
        tmpdir = os.path.join("workarea", "%dgram-%s" % (3, g))
        os.mkdir(tmpdir)
        dstfpath = os.path.abspath(os.path.join(tmpdir, "ngram_3"))
        shutil.copy(srcfpath, dstfpath)
        load_into_db(dstfpath, 3)
        os.unlink(dstfpath)
        os.rmdir(tmpdir)
    print "Omitted:"
    print omitted_list
