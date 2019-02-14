from dao.DAG import DAG as DAO_DAG
import datetime, shutil, os, glob

class DAG():

	def __init__(self, dao_execution, conn=None, results_path='', history_dag_path=''):

		self.conn = conn
		self.results_path = results_path + '/' + str(dao_execution['dag_id'])
		self.history_dag_path = dao_execution['history_dag_path']
		self.default_dag_path = dao_execution['default_dag_path']
		self._id = dao_execution['id']
		self.dag_id = dao_execution['dag_id']
		self.execution_date = dao_execution['execution_date']
		self.state = dao_execution['state']
		self.run_id = dao_execution['run_id']
		self.external_trigger = dao_execution['external_trigger']
		self.conf = dao_execution['conf']
		self.end_date = dao_execution['end_date']
		self.start_date = dao_execution['start_date']

	def delete_content(self):
		try:
			for each_file in os.listdir(self.results_path):
				dpath = self.results_path + '/' + each_file
				if os.path.isdir(dpath):
					shutil.rmtree(dpath)
				else:
					os.remove(dpath)
		except OSError as error:
			#Solo atrapar el error si es 'No such file or directory"
			if error.errno != 2:
				raise error
			print "No se pudo borrar los resultados de la ejecucion {} debido a que no se creo la carpeta de la misma.".format(self._id)
		finally:
			self.results_available = False
			self.results_deleted_at = datetime.datetime.now()
			dao_exec = DAO_DAG(self.conn)
			dao_exec.set_deleted(self._id, self.results_deleted_at)

	def move_dag(self):
		dpath = self.default_dag_path
		hpath = self.history_dag_path
		try:
			if os.path.isdir(dpath) and os.path.isdir(hpath):
				single_file = glob.glob(dpath + '/*' + self.dag_id + '*')
				for file in single_file:
					shutil.move(dpath + '/' + file, self.history_dag_path)
			elif not os.path.isdir(hpath):
				os.mkdir(hpath)

		except OSError as error:
			#Solo atrapar el error si es 'No such file or directory"
			if error.errno != 2:
				raise error
			print "No se pudo borrar los resultados de la ejecucion {} debido a que no se creo la carpeta de la misma.".format(self._id)