import urllib2
from urllib2 import HTTPError
import time
from TestLogger import TestLogger
import os
from libcloud.common.types import LibcloudError
from libcloud.storage.providers import get_driver
from libcloud.storage.types import Provider, ContainerDoesNotExistError, ObjectHashMismatchError
from RandomFile import RandomFile
import string
import socket

location = "Beijing"
testid = 0;

#generate azure_cdn_urls
azure_cdn_base = "http://az160243.vo.msecnd.net/mlibcloud/Beijing_0_"
size = 16 * 1024
azure_cdn_urls=[]
while size <= 8 * 1024 * 1024:
	azure_cdn_urls.append("%s%s" % (azure_cdn_base, size))
	size *= 2

s3_cdn_base = "http://d1dce9m55tbjje.cloudfront.net/Beijing_0_"
size = 16 * 1024
s3_cdn_urls=[]
while size <= 8 * 1024 * 1024:
	s3_cdn_urls.append("%s%s" % (s3_cdn_base, size))
	size *= 2

def test_cdn(server, cdn_base, cdn_urls):
	up_down = "download"
	for url in cdn_urls:
		start_time = time.time()
		end_time = 0
		code = os.system("wget %s -O tempfile --timeout=10" % url)
		if code == 0:
			end_time = time.time()
		else:
			end_time = -1
		file_size = url[len(cdn_base):]
		TestLogger.getInstance().log(testid, location, server, start_time, end_time, file_size, up_down)

def test_azure_cdn():
	server = "CDN:Azure"
	test_cdn(server, azure_cdn_base, azure_cdn_urls)

def test_s3_cdn():
	server = "CDN:S3"
	test_cdn(server, s3_cdn_base, s3_cdn_urls)

def test_original(driver, container_name, server):
	try:
		container = driver.get_container(container_name)
	except ContainerDoesNotExistError:
		container = driver.create_container(container_name)
	
	#test for upload
#	f_group = RandomFile.create_group(location, testid)
#	for f_path in f_group:
#		start_time = time.time()
#		try:
#			driver.upload_object(f_path, container, f_path, {"content_type": "application/octo-stream"})
#			end_time = time.time()
#		except  LibcloudError, ObjectHashMismatchError:
#			end_time = -1
#		except socket.error:
#			end_time = -1
#		file_size = f_path.split("_")[-1]
#		up_down = "upload"
#		TestLogger.getInstance().log(testid, location, server, start_time, end_time, file_size, up_down)
#	RandomFile.delete_group(f_group)
	
	#test for download
	objects = []
	try:
		objects = driver.list_container_objects(container)	
	except LibcloudError, socket.error:
		None

	down_dic = {}
	for o in objects:
		fs_path = o.name			
		info = fs_path.split("_")
		#delete the object if it is generated locally 10 rounds ago
		roundid = string.atoi(info[1])
		loc = info[0]
		if cmp(loc, location) == 0 and (testid - roundid) > 10:
			conainer.delete_object(o)
		#prepare to pick a group to download
		file_size = string.atoi(info[2])
		if file_size == 8 * 1024 * 1024:
			if down_dic.has_key(loc) and down_dic[loc] > roundid:
				None
			else:
				down_dic[loc] = roundid

	#prefer to download from other client uploads	
	if len(down_dic) > 1 and down_dic.has_key(location):
		down_dic.pop(location)
	#TODO random	
	info_tuple = down_dic.items()[0]	
	to_down_objects = []
	
	for o in objects:
		info = o.name.split("_")
		loc = info[0]
		roundid = string.atoi(info[1])
		if cmp(loc, info_tuple[0]) == 0 and roundid == info_tuple[1]:
			to_down_objects.append(o)
	
	f_group = []
	for o in to_down_objects:
		start_time = time.time()
		try:
			f_group.append(o.name)
			driver.download_object(o, o.name, True)
			end_time = time.time()
		except LibcloudError, ObjectHashMismatchError:
			end_time = -1
		except socket.error:
			end_time = -1

		file_size = string.atoi(o.name.split("_")[-1])
		up_down = "download"
		TestLogger.getInstance().log(testid, location, server, start_time, end_time, file_size, up_down)
	RandomFile.delete_group(f_group)

def test_s3_us_west():
	S3_US_WEST = get_driver(Provider.S3_US_WEST)
	driver_s3_us_west = S3_US_WEST("AKIAITLX6IDDU5VTNAPA", "Pi0BhJiVan/l6a2+Yg9JVxrNvZSTRMGIx39XWAGq");
	server = "original:S3_US_WEST"
	container_name = "mlibclouds3uswest"
	test_original(driver_s3_us_west, container_name, server)

def test_s3_ap_southeast():
	S3_AP_SOUTHEAST = get_driver(Provider.S3_AP_SOUTHEAST)
	driver = S3_AP_SOUTHEAST("AKIAITLX6IDDU5VTNAPA", "Pi0BhJiVan/l6a2+Yg9JVxrNvZSTRMGIx39XWAGq");
	server = "original:S3_AP_SOUTHEAST"
	container_name = "mlibclouds3apsoutheast"
	test_original(driver, container_name, server)

def test_s3_ap_northeast():
	S3_AP_NORTHEAST = get_driver(Provider.S3_AP_NORTHEAST)
	driver = S3_AP_NORTHEAST("AKIAITLX6IDDU5VTNAPA", "Pi0BhJiVan/l6a2+Yg9JVxrNvZSTRMGIx39XWAGq");
	server = "original:S3_AP_NORTHEAST"
	container_name = "mlibclouds3apnortheast"
	test_original(driver, container_name, server)

def test_ali():
	Ali = get_driver(Provider.ALIYUN_STORAGE)
	driver = Ali("hqxxyywptpn3juer4zd5rods", "WfUMI6vw28r0GD4gwPtNRpS/unU=")
	server = "original:ALIYUN_STORAGE"
	container_name = "mlibcloudali"
	test_original(driver, container_name, server)

def test_azure_us():
	Azure = get_driver(Provider.WINDOWS_AZURE_STORAGE)
	driver = Azure("mlibcloud", "qdLKg2Cu1cWOkItbqb6gTl1WcOxvA9ED3fPo1KbSwdKw9ApJMhVEbyklurrBK23r8pTf6ajLN9tueSj5gVpiNQ==")
	server = "original:Azure"
	container_name = "mlibcloudazure"
	test_original(driver, container_name, server)

def test_cloudfiles_us():
	Cloudfiles_US = get_driver(Provider.CLOUDFILES_US)
	driver = Cloudfiles_US("mlibcloud", "5140858194409ed2dd2ec13e008ac754")
	server = "original:CloudFiles_US"
	container_name = "mlibcloudcloudfilesus"
	test_original(driver, container_name, server)

def test_cloudfiles_uk():
	Cloudfiles_UK = get_driver(Provider.CLOUDFILES_UK)
	driver = Cloudfiles_UK("mlibcloud0", "d544e3b4a183ba4d07777be6e6ce0b77z")
	server = "original:CloudFiles_UK"
	container_name = "mlibcloudcloudfilesuk"
	test_original(driver, container_name, server)


if __name__ == "__main__":
	while True:
		testid += 1	
		
		#test for cdns
		#test for azure cdn	
#		test_azure_cdn()

		#test for s3 cdn
#		test_s3_cdn()

		#test for original clouds
		#test for s3_us_west()
		test_s3_us_west()	
	
		test_s3_ap_southeast()
		
		test_s3_ap_northeast()

		test_ali()

		test_azure_us()

		test_cloudfiles_us()
		
		test_cloudfiles_uk()

		#test for mlibcloud
