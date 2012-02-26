import time

class mtimer :
	
	def __init__(self, name):
		self.name = name
	def begin(self):
		self.t1 = time.time()
	def end(self):
		self.t2 = time.time()
	def get_interval(self):
		return  self.t2 - self.t1




#vim :set tabstop=4
