# CDCOL Cleaner

from ConfigParser import ConfigParser
from entities.LockFile import LockFile
from entities.Connection import Connection
from entities.DAGs import DAGs
from exceptions import Exception

import os, sys, datetime, traceback

CONF_FILE = 'settings.conf'

conf = ConfigParser()
conf.read(CONF_FILE)

lockfile = LockFile(conf.get('other','lock_file'))
if lockfile.search():
	print 'There\'s an execution in progress'
	sys.exit(1)
else:
	lockfile.write()

try:
	print 'CDCOL CLEANER'

	dbconn = Connection(
				host=conf.get('database','host'),
				port=conf.get('database','port'),
				name=conf.get('database','name'),
				user=conf.get('database','user'),
				password=conf.get('database','password')
				)

	dbconn.connect()

	days = conf.get('other','days')

	results_path = conf.get('paths', 'results_path')
	new_dag_path = conf.get('paths', 'new_dag_path')
	history_dag_path = conf.get('paths', 'history_dag_path')

	dags = DAGs(dbconn.curr_conn, results_path, new_dag_path, history_dag_path)
	dags.load_old_dags(days)

	for each_dag in dags.dags:
		print 'Cleaning ' + str(each_dag._id) + ' - ' + each_dag.description
		each_dag.delete_content()

except Exception as e:
	#print 'Error: ' + str(e)
	traceback.print_exc()
finally:
	lockfile.delete()
