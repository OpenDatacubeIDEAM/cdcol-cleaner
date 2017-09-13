from dao.Execution import Execution as DAOExecution
import datetime, shutil, os

class Execution():

	def __init__(self, dao_execution, conn=None, results_path=''):

		self.conn = conn
		self.results_path = results_path + '/' + str(dao_execution['id'])
		self._id = dao_execution['id']
		self.description = dao_execution['description']
		self.state = dao_execution['state']
		self.started_at = dao_execution['started_at']
		self.finished_at = dao_execution['finished_at']
		self.trace_error = dao_execution['trace_error']
		self.created_at = dao_execution['created_at']
		self.updated_at = dao_execution['updated_at']
		self.executed_by_id = dao_execution['executed_by_id']
		self.version_id = dao_execution['version_id']
		self.email_sent = dao_execution['email_sent']
		self.results_available = dao_execution['results_available']
		self.results_deleted_at = dao_execution['results_deleted_at']

	def delete_content(self):
		try:
			for each_file in os.listdir(self.results_path):
				epath = self.results_path + '/' + each_file
				if os.path.isdir(epath):
					shutil.rmtree(epath)
				else:
					os.remove(epath)
		except OSError as error:
			#Solo atrapar el error si es 'No such file or directory"
			if error.errno != 2:
				raise error
			print "No se pudo borrar los resultados de la ejecucion {} debido a que no se creo la carpeta de la misma.".format(self._id)
		finally:
			self.results_available = False
			self.results_deleted_at = datetime.datetime.now()
			dao_exec = DAOExecution(self.conn)
			dao_exec.set_deleted(self._id, self.results_deleted_at)
