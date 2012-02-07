#!/usr/local/lib/python2.7
#-*- coding:UTF-8 -*-

import sys
from zfec import Decoder
from os import path
from pyutil import mathutil


def fdc_init(k,file_name_prefix = 'temp_file'):
	file_dir_prefix = '/home/pin/桌面/' + file_name_prefix + '.'
	parts = []
	count = 0
	part = 0
	while count < k :
		while not path.isfile(file_dir_prefix + str(part)) :
			part += 1
		parts.append(part)
		part += 1
		count += 1
	return parts

def fdc_file(block_size,k,m,file_name):
	parts = fdc_init(k,file_name)
	fdecoder = Decoder(k,m)
	streams = []
	streams.extend([""] * k)
	file = open(file_name,'w')

	files = [ open(file_name + '.' + str(i),'r') for i in parts ]
	file_size = path.getsize(file_name+'.'+str(parts[0]))	
	block_count = mathutil.div_ceil(file_size,block_size)

	for count in range(block_count) :
		for i in range(k) :
			streams[i] = files[i].read(block_size)

		results = fdecoder.decode(streams,parts)
		for i in range(len(results)) :
			file.write(results[i])


def main():
	file_name_prefix = None
	k = 3
	m = 5
	block_size = 16
	file_name_prefix = sys.argv[1]
	k = int(sys.argv[2])
	m = int(sys.argv[3])
	fdc_file(block_size,k,m,file_name_prefix)
	
if __name__ == '__main__' :
	main()







