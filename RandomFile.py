import os

class RandomFile(object):

	@staticmethod
	def create(filepath, filesize):
		f = open(filepath, "w")
		random = open("/dev/random", "r")
		size = 0
		while (size < filesize):
			data = random.read(1024)
			f.write(data)
			size += 1024
		f.flush()	
		return f

	@staticmethod
	def create_group(location, testid, size_start = 16 * 1024, size_end = 8 * 1024 * 1024, step = 2):
		size = size_start
		ret = []

		while (size <= size_end):
			filepath = "%s_%s_%s" % (location, testid, size)
			f = RandomFile.create(filepath, size)
			ret.append(f)
			size *= step

		return ret

if __name__ == "__main__":
	f = RandomFile.create("test_file", 16 * 1024)
	f.close()

	f_group = RandomFile.create_group("Beijing", 1)

	for f in f_group:
		f.close()
