import logging
import logging.handlers
from logging.handlers import TimedRotatingFileHandler
from logging import Formatter
import time
import threading

class TestLogger(object):
	def __init__():
		#You should not call this
		#Call TestLogger.getInstance() instead
		None

	__instance = None 
	__logger = logging.getLogger(__name__)
	__lock = threading.Lock()

	@staticmethod
	def getInstance():
		TestLogger.__lock.acquire()
		if not TestLogger.__instance:
			TestLogger.__instance = object.__new__(TestLogger)
			object.__init__(TestLogger.__instance)
			handler = TimedRotatingFileHandler(filename = "mlibcloud.log", when='midnight')
			handler.setFormatter(Formatter("%(asctime)s\t%(message)s"))
			TestLogger.__logger.setLevel(logging.INFO)
			TestLogger.__logger.addHandler(handler)
		TestLogger.__lock.release()
		return TestLogger.__instance

	def log(self, testi_id, location, server, start_time, end_time, filesize, up_down):
		TestLogger.__logger.info("%s\t%s\t%s\t%s\t%s\t%s", location, server, start_time, end_time, file_size, up_down)
	
	def log_sentence(self, sentence):
		TestLogger.__logger.info(sentence)

if __name__ == "__main__":
	test_id = 1;		#test group id count
	location = "Beijing";
	start_time = time.time()
	end_time = time.time()
	file_size = 1024 * 16	#byte
	#server = "CDN:Azure"
	#server = "orginal:Azure"
	server = "mLibCloud:Alibaba$Azure$GoogleS3-US-West$CloudFiles-UK$S3-South-East$NineFold:3$7"
	up_down = "upload" 
	TestLogger.getInstance().log(test_id, location, server, start_time, end_time, file_size, up_down) 
	TestLogger.getInstance().log_sentence("This should be a simple sentence.")
