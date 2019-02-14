from DAG import DAG
from dao.DAG import DAG as DAO_DAG

class DAGs():

	def __init__(self, conn, results_path, new_dag_path, history_dag_path):
		self.conn = conn
		self.dags = []
		self.results_path = results_path
		self.new_dag_path = new_dag_path
		self.history_dag_path = history_dag_path

	def load_old_dags(self, days):
		dao_dag = DAO_DAG(self.conn)
		for each_row in dao_dag.get_oldest_than(days):
			dag = DAG(each_row, self.conn, self.results_path, self.new_dag_path)
			self.dags.append(dag)
