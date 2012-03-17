import urllib2
from urllib2 import HTTPError
import time
from TestLogger import TestLogger
import os

location = "Beijing"
testid = 0;

#generate azure_cdn_urls
azure_cdn_base = "http://az160243.vo.msecnd.net/mlibcloud/Beijing_0_"
size = 16 * 1024
azure_cdn_urls=[]
while size <= 8 * 1024 * 1024:
	azure_cdn_urls.append("%s%s" % (azure_cdn_base, size))
	size *= 2

if __name__ == "__main__":
	

	while True:
		testid += 1	
		
		#test for azure cdn	
		server = "CDN:Azure"
		up_down = "download"
		file_size = 16 * 1024

		for url in azure_cdn_urls:
			try:
				start_time = time.time()
				end_time = 0;
				u = urllib2.urlopen(url)
				temp_f = open("tempfile", "w")
				temp_f.write(u.read())
				temp_f.close()
				end_time = time.time()
				os.remove("tempfile");
			except HTTPError:
				end_time = -1
			finally:
				TestLogger.getInstance().log(testid, location, server, start_time, end_time, file_size, up_down)
				file_size *= 2
				
		#test for original cloud


		#test for mlibcloud
