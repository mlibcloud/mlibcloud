import sys
from zfec import Encoder
from os import path
from pyutil import mathutil

#import a file
#export a bunch of streams
def fec_file(file, block_size, k, m):
	file_size = path.getsize(file.name)
	fencoder = Encoder(k, m)
	block_count = mathutil.div_ceil(file_size, block_size)
	#block_count_per_k = mathutil.div_ceil(block_count, k)
	ds = []
	ds.extend([""] * k)

	#for the last round, block might be not complete or empty
	index = 0
	for i in range(block_count):
		index = i % k
		ds[index] = ds[index] + file.read(block_size)

	#add zeros to the end of stripes.
	for i in range(index, k):
		ds[i] = ds[i] + "\x00" * (len(ds[0]) - len(ds[i]))

	results = fencoder.encode(ds)
	return results

def write_streams_to_file(streams, file_name_prefix = "temp_file"):
	ret_files = []
	ret_files.extend([None] * len(streams))
	for i in range(len(streams)):
		file_name = "%s.%d" % (file_name_prefix, i)
		ret_files[i] = open(file_name, "w")
		ret_files[i].write(streams[i])

def main():
	file_name = None
	k = 3
	m = 5

	file_name = sys.argv[1]
	k = int(sys.argv[2])
	m = int(sys.argv[3])

	file = open(file_name, "r")
	#block_size = 512 * 1024 #512k
	block_size = 16 #16Byte
	streams = fec_file(file, block_size, k, m)
	write_streams_to_file(streams, file_name)

if __name__ == "__main__":
	main()
