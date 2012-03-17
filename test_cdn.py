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
		for url in azure_cdn_urls:
			try:
				#print url
				start_time = time.time()
				end_time = 0;
				code = os.system("wget %s -O tempfile --timeout=10" % url)	
				#user_agent = 'Mozilla/4.0 (compatible; MSIE 5.5; Windows NT)'
				#headers = {'User-Agent' : user_agent}
				#req = urllib2.Request(url, headers = headers)
				#response = urllib2.urlopen(req)
				#page = response.read()
				#temp_f = open("tempfile", "w")
				#temp_f.write(page)
				#temp_f.close()
				if code == 0:
					end_time = time.time()
				else :
					end_time = -1
				os.remove("tempfile");
			except HTTPError:
				end_time = -1
			finally:
				file_size = url[len(azure_cdn_base):]	
				TestLogger.getInstance().log(testid, location, server, start_time, end_time, file_size, up_down)
				
		#test for original cloud


		#test for mlibcloud
