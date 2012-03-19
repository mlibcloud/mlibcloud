#!/usr/local/lib/python2.7
#-*- coding:UTF-8 -*-

import sys
from zfec import Decoder
from os import path
from pyutil import mathutil


def fdc_init(k, file_dir, file_name_prefix = 'temp_file'):
	file_dir_prefix = file_dir  + file_name_prefix + '.'
	parts = []
	count = 0
	part = 0
	#TODO may cause while(1) looping
	pass
	while count < k :
		while not path.isfile(file_dir_prefix + str(part)) :
			part += 1
		parts.append(part)
		part += 1
		count += 1
	return parts

def fdc_file(parts, block_size, k, m, file_name, file_dir, file_origin_size):
#	parts = fdc_init(k, file_dir, file_name)
	fdecoder = Decoder(k, m)
	streams = []
	streams.extend([""] * k)
	file = open(file_dir  + file_name,'w')

	files = [ open(file_dir  + file_name + '.' + str(i),'r') for i in parts ]
	file_size = path.getsize(file_dir + file_name+'.'+str(parts[0])) * k
	block_count = mathutil.div_ceil(file_size, block_size)

	for count in range(block_count) :
		for i in range(k) :
			streams[i] = files[i].read(block_size)

		results = fdecoder.decode(streams, parts)
		for i in range(len(results)) :
			file.write(results[i])
#	file.truncate(file_origin_size)
	file.close()

def main():
	file_name_prefix = None
	file_name = 'thisgeneration'

	block_size = 512
	k = 3
	m = 5
	parts = [0,1,2]
	fdc_file(parts, block_size, k, m, file_name, './', 5201)
	
if __name__ == '__main__' :
	main()







