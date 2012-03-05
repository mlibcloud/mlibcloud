import os
import random


def gen_file(file_name, file_size) :
	file = open(file_name, 'w')
	block_size = 1024

	for i in range( file_size / block_size) :
		s = ""
		for j in range(block_size) :
			s += chr(random.randint(0,255))
		file.write(s)


def main():
	file_name = 'gen_file_test'
	KB = 1024
	MB = KB * KB
	file_size = 16 * KB
	gen_file(file_name, file_size)
	print(os.path.getsize(file_name))

if __name__ == '__main__' :
	main()

