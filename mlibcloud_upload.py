#!/usr/local/bin/python3.2
# -*- coding:UTF-8 -*-
import threading
import os
from mlibcloud_keys import mlibcloudid
from mlibcloud_keys import mlibcloudkey
from libcloud.storage.types import Provider
from libcloud.storage.providers import get_driver
from libcloud.storage.types import ContainerDoesNotExistError
from libcloud.storage.types import InvalidContainerNameError



#reader thread
class StorageUploader :
	''' storage uploader
	    @para Tnumber : Thread Number
	'''
	def __init__(self,provider,UserId,UserKey,Tnumber):
		self.driver = get_driver(provider)(UserId,UserKey)
		self.Tnumber = Tnumber
	
	def getOrcreateCon(self,conName):
		retCon = None
		try :
			retCon = self.driver.get_container(conName)
		except  ContainerDoesNotExistError:
			try :
				retCon = self.driver.create_container(conName)
			except InvalidContainerNameError :
				print("%d  Invalid Container Name" %self.Tnumber)
		return retCon

	def doUpload(self,filePath,objName,conName):
		self.conItem = self.getOrcreateCon(conName)
		if self.conItem != None:
			print("%d  Uploading..." %self.Tnumber)
			self.conItem.upload_object(file_path = filePath,object_name = objName)
			print("%d  Uploading complete!" %self.Tnumber)
		else :
			print("%d  Upload failed" %self.Tnumber)


class createThread(threading.Thread,StorageUploader):
	'''create a thread for reading --> writing --> uploading
	@para filePath: dir of the file to split and upload
	@para startp: position of the start byte
	@para endp: position of file section's end byte
	@para blockSize : read file block by block,considering lack of RAM
	@para provider : the storage provider
	@para UserId : storage account Sid
	@para UserKey: storage account Skey
	@para partNum : file part sequence number
	
		0123456789
		******
		startp = 0 endp = 6
	'''
	def __init__(self,filePath,startp,endp,provider,Sid,Skey,partNum,blockSize = 1024L):
		threading.Thread.__init__(self)
		StorageUploader.__init__(self,provider,Sid,Skey,partNum)
		self.filePath = filePath
		self.startp = startp
		self.endp = endp
		self.blockSize = blockSize
		self.partNum = partNum

	def fileInit(self):
		self.fileDirname = os.path.dirname(self.filePath)
		self.fileBasename = os.path.basename(self.filePath)
		tfdir = self.filePath + 'P' + str(self.partNum) + self.filePath[self.filePath.rindex('.'):]
		self.targetBasename = os.path.basename(tfdir)
		self.reader = open(self.filePath,'rb')
		self.writer = open(tfdir,'wb')
		self.reader.seek(self.startp,0)

	def run(self):
		self.fileInit()
		print("%d  file writing ...." % self.partNum)
		while self.reader.tell() < self.endp - self.blockSize:
			self.writer.write(self.reader.read(self.blockSize))
		self.writer.write(self.reader.read(self.endp - self.reader.tell()))
		print("%d  writing complete!" %self.partNum)
		self.reader.close()
		self.writer.close()
		conName = self.fileBasename + '.mlb'
		objName = self.targetBasename
		self.doUpload(self.fileDirname +'/'+ self.targetBasename,objName,conName)



def main():
	fdir = '/home/pin/mydev/test/test.rar'
	f_size = os.path.getsize(fdir)
	half_size = long(f_size / 2)
	block_size = 1024L
	if f_size < 2048L :
		block_size = (long)(f_size / 2)
	provider = Provider.S3_AP_NORTHEAST

	containerIniter = StorageUploader(provider,mlibcloudid,mlibcloudkey,0)
	containerIniter.getOrcreateCon(os.path.basename(fdir) + '.mlb')


	t1 = createThread(fdir,0,half_size,provider,mlibcloudid,mlibcloudkey,1,block_size)
	t2 = createThread(fdir,0,half_size,provider,mlibcloudid,mlibcloudkey,2,block_size)
	t1.start()
	t2.start()
	t1.join()
	t2.join()

if __name__ == '__main__':
	main()





