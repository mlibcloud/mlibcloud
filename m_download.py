
import os
from libcloud.storage.types import Provider
from libcloud.storage.providers import get_driver
from libcloud.storage.types import ContainerDoesNotExistError
from libcloud.storage.types import ObjectDoesNotExistError
from libcloud.common.types import LibcloudError


class StorageDownloader :
	''' download object from cloud'''
	def __init__(self,provider,UserId,UserKey) :
		self.driver = get_driver(provider)(UserId,UserKey)

	def getObject(self,conName,objName) :
		try :
			retObj = self.driver.get_object(conName,objName)
		except ContainerDoesNotExistError :
			print("%s container does not exist" % conName)
			retObj = None
		except ObjectDoesNotExistError :
			print("%s object does not exist " %s objName)
			retObj = None
		return retObj

	def doDownload(self,conName,objName,dest_path = os.path.abspath('.'),overwrite_existing = False,delete_on_failure = True) :
		try :
			objItem = self.getObject(conName,objName)
		except LibcloudError , le:
			print(le.str())
			objItem = None
		
		if objItem != None :
			print("Downloading ... ")
			return self.driver.download_object(objItem,dest_path,overwrite_existing,delete_on_failure)
		else :
			return False


class createThread(threading.Thread,StorageDownloader) :
	
	def __init__(self,conName,objName,provider,Sid,Skey):
		threading.Thread.__init__(self):
		StorageDownloader.__init__(self,provider,UserId,UserKey)
		self.conName = conName
		self.objName = objName

	def run(self):
		result = self.doDownload(self.conName,self.objName)
		if result :
			print("%s %s download success" %(self.conName,self.objName))
		else :
			print("%s %s download failed" %(self.conName,self.objName))
		
