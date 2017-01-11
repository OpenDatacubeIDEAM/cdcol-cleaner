from Execution import Execution
from dao.Execution import Execution as DAOExecution

class Executions():

	def __init__(self, conn, results_path):
		self.conn = conn
		self.executions = []
		self.results_path = results_path

	def load_old_executions(self, days):
		dao_execution = DAOExecution(self.conn)
		for each_row in dao_execution.get_oldest_than(days):
			execution = Execution(each_row, self.conn, self.results_path)
			self.executions.append(execution)
