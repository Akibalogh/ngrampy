# A script to pre-process google data

nohup pigz -dc /media/data0/ngrampy/eng-us-all/q/* | python process-google.py > /media/data0/ngrampy/output/eng-us-all-q 2> nohup.err && mailx -s "finished books processing" akibalogh@gmail.com < /dev/null &
