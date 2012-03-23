import time

class mtimer :
	
	def __init__(self, name, c_name = None, o_name = None):
		self.name = name
		self.c_name = c_name
		self.o_name = o_name
		self.t1 = 0.0
		self.t2 = -1.0
	def begin(self):
		self.t1 = time.time()
	def end(self):
		self.t2 = time.time()
	def get_interval(self):
		return  self.t2 - self.t1
	def set_name(self, name) :
		self.name = name
	def get_info(self) :
		return "%s\t%s\t%s\t%s\t%s" %(self.name, self.c_name, self.o_name, self.t1, self.t2)


#vim :set tabstop=4
