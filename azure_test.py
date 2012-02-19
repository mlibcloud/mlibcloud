#-*- coding:UTF-8 -*-
from winazurestorage import *
import base64
import sys
import os


class AzureUploader :
	
	def __init__(self, account, key) :
		self.driver = BlobStorage(CLOUD_BLOB_HOST, account, key)

	def get_or_create_container(self, container_name):
		#suppose container is a private container. notice that azure provide public blob container
		#azure container is not a object
		if self.driver.container_exists(container_name) :
			return True;
		if self.driver.create_container(container_name) == 201 :
			return True
		return False

	def do_upload(self, file_path, container_name, obj_name) :
		print("uploading...")
		ret = self.driver.put_blob( container_name, obj_name, open(file_path,'r').read())
		if ret == 201 :
			print("upload complete")
		else :
			print("upload failed")


class AzureDownloader :
	
	def __init__(self, account, key):
		self.driver = BlobStorage(CLOUD_BLOB_HOST, account, key )

	def do_download(self, container_name, object_name, dest_path):
		if not self.driver.blob_exists(container_name, object_name) :
			print("object not exist")
			return 
		file = open(dest_path + '/' + object_name,'w')
		
		print("downloading ... ")
		ret = self.driver.get_blob(container_name, object_name)
		file.write(ret)
		file.close()
		print("download complete")


def main() :
	account = ""
	key = ""
	file_path = "/home/pin/桌面/thisgeneration"
	container_name = os.path.basename(file_path) + "-mlb"
	obj_name = os.path.basename(file_path)
	upt = AzureUploader(account, key)
	upt.get_or_create_container(container_name) 
	upt.do_upload(file_path, container_name, obj_name)

	print(container_name,obj_name)
	dwt = AzureDownloader(account, key)
	dwt.do_download(container_name, obj_name, "/home/pin/桌面/azure")



if __name__ == "__main__" :
	main()




