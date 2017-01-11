# CDCOL Cleaner

from ConfigParser import ConfigParser
from entities.LockFile import LockFile
from entities.Connection import Connection
from entities.Executions import Executions
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

	executions = Executions(dbconn.curr_conn, results_path)
	executions.load_old_executions(days)

	for each_exec in executions.executions:
		print 'Cleaning ' + str(each_exec._id) + ' - ' + each_exec.description
		each_exec.delete_content()

except Exception as e:
	#print 'Error: ' + str(e)
	traceback.print_exc()
finally:
	lockfile.delete()
