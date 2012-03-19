import os
from pprint import pprint


class RandomFile(object):

	@staticmethod
	def create(filepath, filesize):
		f = open(filepath, "w")
		random = open("/dev/urandom", "r")
		size = 0
		while (size < filesize):
			data = random.read(1024)
			f.write(data)
			size += 1024
		f.flush()	
		f.close()
		return filepath

	@staticmethod
	def create_group(location, testid, size_start = 16 * 1024, size_end = 8 * 1024 * 1024, step = 2):
		size = size_start
		ret = []

		while (size <= size_end):
			filepath = "%s_%s_%s" % (location, testid, size)
			f = RandomFile.create(filepath, size)
			ret.append(filepath)
			size *= step
		return ret

	@staticmethod
	def delete_group(file_group):
		for f_path in file_group:
			os.remove(f_path)	

if __name__ == "__main__":
	f_path = RandomFile.create("test_file", 16 * 1024)

	f_group = RandomFile.create_group("Beijing", 1)
	pprint(f_group)
	RandomFile.delete_group(f_group)
