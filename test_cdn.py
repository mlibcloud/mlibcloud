import urllib2
from urllib2 import HTTPError
import time
from TestLogger import TestLogger
import os
from libcloud.common.types import LibcloudError
from libcloud.storage.providers import get_driver
from libcloud.storage.types import Provider, ContainerDoesNotExistError, ObjectHashMismatchError
from RandomFile import RandomFile

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

def test_s3_us_west():
	S3_US_WEST = get_driver(Provider.S3_US_WEST)
	driver_s3_us_west = S3_US_WEST("AKIAITLX6IDDU5VTNAPA", "Pi0BhJiVan/l6a2+Yg9JVxrNvZSTRMGIx39XWAGq");
	driver = driver_s3_us_west
	try:
		container_s3_us_west = driver.get_container("mlibclouds3uswest")
	except ContainerDoesNotExistError:
		container_s3_us_west = driver.create_container("mlibclouds3uswest")
	
	#test for upload
	f_group = RandomFile.create_group(location, testid)
	for f_path in f_group:
		start_time = time.time()
		try:
			driver.upload_object(f_path, container_s3_us_west, f_path, {"content_type": "application/octo-stream"})
			end_time = time.time()
		except LibcloudError, ObjectHashMismatchError:
			end_time = -1
		server = "original:S3_US_WEST"
		file_size = f_path.split("_")[-1]
		up_down = "upload"
		TestLogger.getInstance().log(testid, location, server, start_time, end_time, file_size, up_down)
	RandomFile.delete_group(f_group)
	
	#test for download
	objects = driver.list_container_objects(container_s3_us_west)	

	down_dic = {}
	for o in objects:
		fs_path = o.name			
		info = fs_path.split("_")
		#delete the object if it is generated locally 10 rounds ago
		roundid = string.atoi(info[1])
		loc = info[0]
		if cmp(loc, location) == 0 and (testid - roundid) > 10:
			conainer_s3_us_west.delete_object(o)
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
	
	info_tuple = down_dic.items()[1]	
	to_down_objects = []
	for o in objects:
		info = fs_path.split("_")
		loc = info[0]
		roundid = string.atoi(info[1])
		if cmp(loc, info_tuple[0]) and roundid == loc:
			to_down_object.append(o)

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

		#test for mlibcloud
