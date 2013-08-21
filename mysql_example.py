from peewee import *

mysql_db = MySQLDatabase('ngrams', user='root')
mysql_db.connect()

#NOT WORKING YET
#class Database(object):
#	self.create_index()

class MySQLModel(Model):
    """A base model that will use our MySQL database"""
    class Meta:
        database = mysql_db

class NGram_tags(MySQLModel):
    key = CharField(max_length=50, null=False)
    value = IntegerField()

#NGram_tags.create_table()
#NGram_tags.create_index(key, unique=True) # Doesn't work
framing = '''ALTER TABLE %table CONVERT TO CHARACTER SET utf8 COLLATE utf8_unicode_ci;'''
alter_query = framing % {'table': 'ngram_tags' }
execute(alter_query)

# Commands in MySQL

# ALTER TABLE ngram_notags CONVERT TO CHARACTER SET utf8 COLLATE utf8_unicode_ci;

# mysqlimport --local --fields-terminated-by "\t" --fields-optionally-enclosed-by '"' ngrams ngram_notags

# UNNECESSARY? CREATE INDEX ngram_idx on ngram_notags(ngram);
