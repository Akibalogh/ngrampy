

# A script to initially process google data. The significantly speeds up everything later.

nohup pigz -dc /media/data0/ngrampy/eng-us-all/1/* | python process-google.py > /media/data0/ngrampy/Processed/eng-us-all-1 2> error-process-google.py && mailx -s "finished books processing" akibalogh@gmail.com < /dev/null &
#nohup pigz -dc /media/data0/ngrampy/eng-us-all/test/* | python process-google.py > /media/data0/ngrampy/Processed/eng-us-all/test/eng-us-all-test &
#nohup pigz -dc /CorpusA/GoogleBooks/eng-gb-all/2/* | python process-google.py > /CorpusA/GoogleBooks/Processed/eng-gb-2 &
