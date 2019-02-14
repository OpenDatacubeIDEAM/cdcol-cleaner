from psycopg2.extensions import connection
from psycopg2.extras import DictCursor

class DAG():

	def __init__(self, connection):
		self.conn = connection

	def get_oldest_than(self, days):
		cur = self.conn.cursor(cursor_factory=DictCursor)
		cur.execute('SELECT ' +
					'id, ' +
					'dag_id, ' +
					'execution_date, ' +
					'state, ' +
					'run_id, ' +
					'external_trigger, ' +
					'conf, ' +
					'end_date, ' +
					'start_date, ' +
					'FROM dag_run ' +
					'WHERE (now() - execution_date) > \'' + days + ' day\' AND state = \'' + 'success' + '\' OR state = \'' + 'failed' + '\';')
		rows = cur.fetchall()
		return rows

	def set_deleted(self, _id, results_deleted_at):
		cur = self.conn.cursor(cursor_factory=DictCursor)
		query = ('UPDATE execution_execution SET ' +
				'results_available=FALSE, ' +
				'results_deleted_at= \'' + str(results_deleted_at) + '\' ' +
				'WHERE id=' + str(_id) + ';')
		query = query.replace('\'None\'', 'NULL')
		cur.execute(query)
		self.conn.commit()
		cur.close()
