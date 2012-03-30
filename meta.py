from UserDict import UserDict
import json
import md5


#information about a erasure code file
	#name
	#size
	#blocksize
	#k
	#m
	#stripe location, meaning on which cloud it is saved
	#md5

class FileMeta(UserDict):
	"store metadata of the erasure coded file blocks"
	def __init__(self):
		UserDict.__init__(self)
		#self["name"] = filename

	def save_to_string(self):
		s = json.dumps(self.data)
		return s

	def save_to_file(self, filename = None):
		if filename == None:
			filename = self["name"] + ".meta"
		s = self.save_to_string()
		open(filename, "w").write(s)
	
	def load_from_string(self, s):
		try :
			self.data = json.loads(s)	
		except ValueError :
			print(s)
	
	def load_from_file(self, filename):
		self.load_from_string(open(filename, "r").read())

	def set_name(self, name):
		self["name"] = name			
		
	#size in bytes
	def set_size(self, size):
		self["size"] = size

	def set_blocksize(self, blocksize):
		self["blocksize"] = blocksize

	def set_k(self, k):
		self["k"] = k

	def set_m(self, m):
		self["m"] = m

	def set_stripe_location(self, sx, cloud_provider):
		#sx must be between s0 and sm
		self[sx] = cloud_provider
	def set_md5(self,cx,digest):
		#mx must be between m0 and mm
		self[cx] = digest
	def del_item(self,key):
		del self[key]

	def cal_md5(self):
		keys = self.data.keys()
		keys.sort()
		s = ""
		for key in keys :
			s += str(key)
			s += str(self[key])
		return md5.new(s).hexdigest()
	
	def check_md5(self) :
		meta_md5_1 = self["cmeta"]
		self.del_item("cmeta")
		meta_md5_2 = self.cal_md5()
		return meta_md5_1 == meta_md5_2

#for class test
if __name__ == "__main__":
	meta = FileMeta()
	meta.set_name("test_file")
	meta.set_size(18880)
	meta.set_blocksize(1024)
	meta.set_k(3)
	meta.set_m(5)
	meta.set_stripe_location("s0", "S3")
	meta.set_stripe_location("s1", "CloudFiles")
	meta.set_stripe_location("s2", "Google Cloud Storage")
	meta.set_stripe_location("s3", "S3")
	meta.set_stripe_location("s4", "S3")
	print meta.save_to_string()
	meta.save_to_file()	
	

	#Now load meta from the saved file.
	ameta = FileMeta()
	ameta.load_from_file("test_file.meta")
	print meta.save_to_string()
	print ameta["name"]
