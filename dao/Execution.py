from psycopg2.extensions import connection
from psycopg2.extras import DictCursor

class Execution():

	def __init__(self, connection):
		self.conn = connection

	def get_oldest_than(self, days):
		cur = self.conn.cursor(cursor_factory=DictCursor)
		cur.execute('SELECT ' +
					'id, ' +
					'description, ' +
					'state, ' +
					'started_at, ' +
					'finished_at, ' +
					'trace_error, ' +
					'created_at, ' +
					'updated_at, ' +
					'executed_by_id, ' +
					'version_id, ' +
					'email_sent, ' +
					'results_available, ' +
					'results_deleted_at ' +
					'FROM execution_execution ' +
					'WHERE (now() - finished_at) > interval \'' + days + ' day\' AND results_available = TRUE;')
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
