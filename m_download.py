import os
import threading

from libcloud.storage.types import Provider
from libcloud.storage.providers import get_driver
from libcloud.storage.types import ContainerDoesNotExistError
from libcloud.storage.types import ObjectDoesNotExistError
from libcloud.common.types import LibcloudError
from libcloud.storage.providers import DRIVERS

from mtimer import mtimer


class StorageDownloader :
	''' download object from cloud'''
	def __init__(self,provider, UserId, UserKey) :
		self.driver = get_driver(provider)(UserId,UserKey)

	def getObject(self, conName, objName) :
		try :
			retObj = self.driver.get_object(conName, objName)
		except ContainerDoesNotExistError :
			print("%s container does not exist" % conName)
			retObj = None
		except ObjectDoesNotExistError :
			print("%s object does not exist " % objName)
			retObj = None
		return retObj

	def doDownload(self, conName, objName, dest_path = os.path.abspath('.'), overwrite_existing = False, delete_on_failure = True) :
		try :
			objItem = self.getObject(conName, objName)
		except LibcloudError , le:
			print(le.str())
			objItem = None
		
		if objItem != None :
			print("Downloading ... ")
#			print(dest_path)
			return self.driver.download_object(objItem, dest_path, overwrite_existing, delete_on_failure)
		else :
			return False


class createThread(threading.Thread, StorageDownloader) :
	
	def __init__(self, conName, objName, provider ,p_name , Sid, Skey, dest_path = os.path.abspath('.'), timing = False):
		threading.Thread.__init__(self)
		StorageDownloader.__init__(self, provider, Sid, Skey)
		self.conName = conName
		self.objName = objName
		self.dest_path = dest_path
		self.timing = timing
		self.p_name = p_name

	def run(self):
		if self.timing :
			mt = mtimer(self.p_name)
			mt.begin()
		result = self.doDownload(self.conName, self.objName, dest_path = self.dest_path)
		if self.timing :
			mt.end()
			self.time = mt.get_interval()
			self.name = mt.name
		if result :
			print("%s %s download success" %(self.conName,self.objName))
		else :
			print("%s %s download failed" %(self.conName,self.objName))



