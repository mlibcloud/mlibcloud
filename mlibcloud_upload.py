#!/usr/local/bin/python3.2
# -*- coding:UTF-8 -*-
import threading
import datetime
import time
import random
import os
import math
from Queue import Queue
from libcloud.storage.types import Provider
from libcloud.storage.providers import get_driver
from libcloud.storage.types import ContainerDoesNotExistError
from libcloud.storage.types import InvalidContainerNameError



#reader thread

class Read_File(threading.Thread):
	''' read file part and put it into queue'''
	def __init__(self,queue,block_size,file_dir,block_number):
		threading.Thread.__init__(self)
		self.data = queue
		self.block_size = block_size
		self.file_obj = open(file_dir,'rb')
		self.block_number = block_number
		
	
	def run(self):
		for i in range(self.block_number):
			self.data.put(self.file_obj.read(self.block_size),block = True)


class Write_File(threading.Thread):
	''' write file to the part1 and part2'''
	def __init__(self,queue,file_dir,block_number):
		threading.Thread.__init__(self)
		self.data = queue
		file_dirname = os.path.dirname(file_dir)
		file_basename = os.path.basename(file_dir)
		self.file_dir1 = file_dirname + '/' + file_basename + 'p1.rar'
		self.file_dir2 = file_dirname + '/' + file_basename + 'p2.rar'
		self.block_number = block_number
		self.half_number = (long)(math.ceil(block_number / 2))

	def run(self):
		f_part1 = open(self.file_dir1,'wb')
		for i in range(self.half_number):
			f_part1.write(self.data.get(block = True))
		f_part1.close()
		
		f_part2 = open(self.file_dir2,'wb')
		for i in range(self.block_number - self.half_number):
			f_part2.write(self.data.get(block = True))
		f_part2.close()


class S3Upload:
	''' for Amazon S3 upload'''
	driver  = None #driver
	conName = None #container Name
	conItem = None #container Item
	UserId  = None #UserId
	UserKey = None #UserKey
	
	def __init__(self,provider,UserId,UserKey):
		self.UserId = UserId
		self.UserKey = UserKey
		self.driver = get_driver(provider)(self.UserId,self.UserKey)


	def getOrcreateCon(self,conName):
		retCon = None
		try :
			retCon = self.driver.get_container(conName)
		except ContainerDoesNotExistError:
			try:
				retCon = self.driver.create_container(conName)
			except InvalidContainerNameError:
				print("the container Name has already used by others ")
		return retCon
			

	def doUpload(self,filePath,objName,conName):
		'''upload a obj to the container,
		filePath : obj path
		objName : user define objName uploaded to S3
		conName : the object container'''
		self.conItem = self.getOrcreateCon(conName)
		if self.conItem != None :
			print("Uploading")
			self.conItem.upload_object(file_path = filePath,object_name = objName)
			print("Upload complete!")
		else :
			print("Upload failed")





def main():

	f_dir = '/home/pin/mydev/test/test.rar' #the dir of the file you want to split and upload
	f_size = os.path.getsize(f_dir) #the size of the file
	block_size = 1024 #the size of item in the queue
	if f_size < 2048L :
		block_size = (long)(f_size / 2)
	block_number =(long)(math.ceil(f_size /(float)(block_size)))

	dirname = os.path.dirname(f_dir)
	basename = os.path.basename(f_dir)
	f_dir1 = dirname + '/' + basename + 'p1' + '.rar'
	f_dir2 = dirname + '/' + basename + 'p2' + '.rar'

	que = Queue()
	reader = Read_File(que,block_size,f_dir,block_number)
	writer = Write_File(que,f_dir,block_number)
	reader.start()
	writer.start()
	reader.join()
	writer.join()
	print("separate file complete")

	provider = Provider.S3_AP_NORTHEAST
	Id = 'id'
	Key = 'key'

	object_name = os.path.basename(f_dir1)
	conName = object_name + '.mlb'

	uploader = S3Upload(provider,Id,Key)
	print(f_dir1,object_name,conName)
	uploader.doUpload(f_dir1,object_name,conName)

	object_name = os.path.basename(f_dir2)
	conName = object_name + '.mlb'
	
	print(f_dir2,object_name,conName)
	uploader.doUpload(f_dir2,object_name,conName)


if __name__ == '__main__':
	main()


