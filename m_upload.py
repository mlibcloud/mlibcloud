#!/usr/local/bin/python3.2
# -*- coding:UTF-8 -*-
import threading
import os
from libcloud.storage.types import Provider
from libcloud.storage.providers import get_driver
from libcloud.storage.types import ContainerDoesNotExistError
from libcloud.storage.types import InvalidContainerNameError
from libcloud.common.types import LibcloudError



#reader thread
class StorageUploader :
	''' storage uploader'''
	def __init__(self, provider, UserId, UserKey):
		self.driver = get_driver(provider)(UserId, UserKey)
	
	def getOrcreateCon(self, conName):
		retCon = None
		try :
			retCon = self.driver.get_container(conName)
		except  ContainerDoesNotExistError :
			try :
				retCon = self.driver.create_container(conName)
			except LibcloudError :
				print("LibcloudError")
		return retCon

	def doUpload(self, filePath, objName, conName):
#		self.conItem = self.getOrcreateCon(conName)
#		print("conName is %s" % conName)
		if self.conItem != None:
			print("Uploading...")
			self.conItem.upload_object(file_path = filePath,
						   object_name = objName,
						   extra = {'content_type':'zip'})
			print("Uploading complete!")
		else :
			print("Upload failed")


class createThread(threading.Thread, StorageUploader):

	def __init__(self, file_name, container_name, provider, Sid, Skey):
		threading.Thread.__init__(self)
		StorageUploader.__init__(self, provider, Sid, Skey)
		self.file_name = file_name
#		self.conName = os.path.splitext(self.file_name)[0] + '-mlb'
		self.conName = container_name
		self.conItem = self.getOrcreateCon(self.conName)

	def run(self):
		objName = self.file_name
		self.doUpload(self.file_name, objName, self.conName)


