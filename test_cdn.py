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
import random
#from GroupDriver import GroupDriver

location = "Beijing"


#read testid from a checkpoint file
def read_checkpoint():
	checkpoint = 0
	try:
		f = open("test-checkpoint", "r")
		line = f.readline()
		checkpoint = string.atoi(line)
		f.close()
	except IOError:
		None
	return checkpoint

def write_checkpoint(checkpoint):
	try:
		f = open("test-checkpoint", "w")
		f.write("%s" % checkpoint)
		f.close()
	except IOError:
		None
	
testid = read_checkpoint();


#drivers
Ali = get_driver(Provider.ALIYUN_STORAGE)
driver_ali = Ali("hqxxyywptpn3juer4zd5rods", "WfUMI6vw28r0GD4gwPtNRpS/unU=")

Azure = get_driver(Provider.WINDOWS_AZURE_STORAGE)
driver_azure_us = Azure("mlibcloud", "qdLKg2Cu1cWOkItbqb6gTl1WcOxvA9ED3fPo1KbSwdKw9ApJMhVEbyklurrBK23r8pTf6ajLN9tueSj5gVpiNQ==")

GoogleStorage = get_driver(Provider.GOOGLE_STORAGE)
driver_google_storage = GoogleStorage("GOOGULXCXRFPGQNEFPTE", "ys9om0uf2dYlXov4NOjO8jzGXLdtR7pwv9/nIK1V")

S3_US_WEST = get_driver(Provider.S3_US_WEST)
driver_s3_us_west = S3_US_WEST("AKIAITLX6IDDU5VTNAPA", "Pi0BhJiVan/l6a2+Yg9JVxrNvZSTRMGIx39XWAGq");

NineFold = get_driver(Provider.NINEFOLD)
driver_ninefold = NineFold("f9946e04515a46cf98a998f2cb34dd3b/mlibcloud_1328774465274", "fRRs33RyQOmVOrB38UNqV+R3uAM=");

S3_AP_SOUTHEAST = get_driver(Provider.S3_AP_SOUTHEAST)
driver_ap_southeast = S3_AP_SOUTHEAST("AKIAITLX6IDDU5VTNAPA", "Pi0BhJiVan/l6a2+Yg9JVxrNvZSTRMGIx39XWAGq");

S3_AP_NORTHEAST = get_driver(Provider.S3_AP_NORTHEAST)
driver_ap_southeast = S3_AP_NORTHEAST("AKIAITLX6IDDU5VTNAPA", "Pi0BhJiVan/l6a2+Yg9JVxrNvZSTRMGIx39XWAGq");

#Cloudfiles_UK = get_driver(Provider.CLOUDFILES_UK)
#driver_cloudfiles_uk = Cloudfiles_UK("mlibcloud0", "d544e3b4a183ba4d07777be6e6ce0b77z")

Cloudfiles_US = get_driver(Provider.CLOUDFILES_US)
driver_cloudfiles_us = Cloudfiles_US("mlibcloud", "5140858194409ed2dd2ec13e008ac754")

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
	f_group = RandomFile.create_group(location, testid)
	for f_path in f_group:
		start_time = time.time()
		try:
			driver.upload_object(f_path, container, f_path, {"content_type": "application/octo-stream"})
			end_time = time.time()
		except  LibcloudError, ObjectHashMismatchError:
			end_time = -1
		except socket.error:
			end_time = -1
		file_size = f_path.split("_")[-1]
		up_down = "upload"
		TestLogger.getInstance().log(testid, location, server, start_time, end_time, file_size, up_down)
	RandomFile.delete_group(f_group)
	
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
	#randomly pick a group to download
	info_tuple = random.choice(down_dic.items())	
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
	driver = driver_s3_us_west
	server = "original:S3_US_WEST"
	container_name = "mlibclouds3uswest"
	test_original(driver, container_name, server)

def test_s3_ap_southeast():
	driver = driver_ap_southeast 
	server = "original:S3_AP_SOUTHEAST"
	container_name = "mlibclouds3apsoutheast"
	test_original(driver, container_name, server)

def test_s3_ap_northeast():
	driver = driver_ap_northeast
	server = "original:S3_AP_NORTHEAST"
	container_name = "mlibclouds3apnortheast"
	test_original(driver, container_name, server)

def test_ali():
	driver = driver_ali
	server = "original:ALIYUN_STORAGE"
	container_name = "mlibcloudali"
	test_original(driver, container_name, server)

def test_azure_us():
	driver = driver_azure_us
	server = "original:AZURE"
	container_name = "mlibcloudazure"
	test_original(driver, container_name, server)

def test_cloudfiles_us():
	driver = driver_cloudfiles_us
	server = "original:CLOUDFILES_US"
	container_name = "mlibcloudcloudfilesus"
	test_original(driver, container_name, server)

def test_cloudfiles_uk():
	driver = driver_cloudfiles_uk 
	server = "original:CLOUDFILES_UK"
	container_name = "mlibcloudcloudfilesuk"
	test_original(driver, container_name, server)

def test_google_storage():
	driver = driver_google_storage
	server = "original:GOOGLESTORAGE"
	container_name = "mlibcloudgs"
	test_original(driver, container_name, server)

def test_ninefold():
	driver = driver_ninefold 
	server = "original:NINFOLD"
	container_name = "mlibcloudninefold"
	test_original(driver, container_name, server)


def test_mlibcloud_3_7():
	driver = GroupDriver([driver_ali, driver_azure_us, driver_google_storage, driver_s3_us_west, driver_cloudfiles_uk, driver_s3ap_southeast, driver_ninefold])
	driver.set_original_share(3)
	driver.set_total_share(7)
	server = "mLibCloud:ALI$AZURE_US$GOOGLESTORAGE$S3_US_WEST$CLOUDFILES_UK$S3_AP_SOUTHEAST$NINEFOLD:3$7"
	container_name = "mlibcloud37"
	test_original(driver, container_name, server)

def test_mlibcloud_3_6():
	driver = GroupDriver([driver_ali, driver_azure_us, driver_google_storage, driver_s3_us_west, driver_cloudfiles_uk, driver_s3ap_southeast])
	driver.set_original_share(3)
	driver.set_total_share(6)
	server = "mLibCloud:ALI$AZURE_US$GOOGLESTORAGE$S3_US_WEST$CLOUDFILES_UK$S3_AP_SOUTHEAST:3$6"
	container_name = "mlibcloud36"
	test_original(driver, container_name, server)

def test_mlibcloud_3_5():
	driver = GroupDriver([driver_ali, driver_azure_us, driver_google_storage, driver_s3_us_west, driver_cloudfiles_uk])
	driver.set_original_share(3)
	driver.set_total_share(6)
	server = "mLibCloud:ALI$AZURE_US$GOOGLESTORAGE$S3_US_WEST$CLOUDFILES_UK:3$5"
	container_name = "mlibcloud35"
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
#		test_s3_us_west()	
	
#		test_s3_ap_southeast()
		
#		test_s3_ap_northeast()

#		test_ali()

#		test_azure_us()

#		test_cloudfiles_us()
		
		#TODO
#		test_cloudfiles_uk()

#		test_google_storage()

		#TODO
#		test_ninefold()
	
		#TODO
		#test for mlibcloud
#		test_mlibcloud_3_5()
#		test_mlibcloud_3_6()
#		test_mlibcloud_3_7()

		write_checkpoint(testid)
		
