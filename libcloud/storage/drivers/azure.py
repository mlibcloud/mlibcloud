#!usr/bin/env python
# encoding: utf-8
"""
Python wrapper around Windows Azure storage
Sriram Krishnan <sriramk@microsoft.com>
Steve Marx <steve.marx@microsoft.com>
"""

import base64
import hmac
import hashlib
import time
import sys
import os
from xml.dom import minidom #TODO: Use a faster way of processing XML
import re
from urllib2 import Request, urlopen, URLError
from urllib import urlencode
from urlparse import urlsplit, parse_qs
from datetime import datetime, timedelta

from libcloud.common.types import InvalidCredsError, LibcloudError
from libcloud.common.base import ConnectionUserAndKey, RawResponse

from libcloud.storage.base import Object, Container, StorageDriver
from libcloud.storage.types import ContainerIsNotEmptyError
from libcloud.storage.types import InvalidContainerNameError
from libcloud.storage.types import ContainerDoesNotExistError
from libcloud.storage.types import ObjectDoesNotExistError
from libcloud.storage.types import ObjectHashMismatchError
from libcloud.common.types import LazyList



#DEVSTORE_ACCOUNT = "devstoreaccount1"
#DEVSTORE_SECRET_KEY = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="

#DEVSTORE_BLOB_HOST = "127.0.0.1:10000"

CLOUD_BLOB_HOST = "blob.core.windows.net"

PREFIX_PROPERTIES = "x-ms-prop-"
PREFIX_METADATA = "x-ms-meta-"
PREFIX_STORAGE_HEADER = "x-ms-"

NEW_LINE = "\x0A"

DEBUG = False

TIME_FORMAT ="%a, %d %b %Y %H:%M:%S %Z"

def parse_edm_datetime(input):
	d = datetime.strptime(input[:input.find('.')], "%Y-%m-%dT%H:%M:%S")
	if input.find('.') != -1:
		d += timedelta(0, 0, int(round(float(input[input.index('.'):-1])*1000000)))
	return d

def parse_edm_int32(input):
	return int(input)

def parse_edm_double(input):
	return float(input)

def parse_edm_boolean(input):
	return input.lower() == "true"

class SharedKeyCredentials(object):
	def __init__(self, account_name, account_key, use_path_style_uris = None):
		self._account = account_name
		self._key = base64.decodestring(account_key)

	def _sign_request_impl(self, request, for_tables = False,  use_path_style_uris = None):
		(scheme, host, path, query, fragment) = urlsplit(request.get_full_url())
		if use_path_style_uris:
			path = path[path.index('/'):]

		canonicalized_resource = "/" + self._account + path
		q = parse_qs(query)
		if len(q.keys()) > 0:
			canonicalized_resource +=''.join(["\n%s:%s" % (k, ','.join(sorted(q[k]))) for k in sorted(q.keys())])

		if use_path_style_uris is None:
			use_path_style_uris = re.match('^[\d.:]+$', host) is not None

		request.add_header(PREFIX_STORAGE_HEADER + 'version', '2011-08-18')
		request.add_header(PREFIX_STORAGE_HEADER + 'date', time.strftime("%a, %d %b %Y %H:%M:%S GMT", time.gmtime())) #RFC 1123
		if for_tables:
			request.add_header('Date', request.get_header((PREFIX_STORAGE_HEADER + 'date').capitalize()))
			request.add_header('DataServiceVersion', '1.0;NetFx')
			request.add_header('MaxDataServiceVersion', '1.0;NetFx')
		canonicalized_headers = NEW_LINE.join(('%s:%s' % (k.lower(), request.get_header(k).strip()) for k in sorted(request.headers.keys(), lambda x,y: cmp(x.lower(), y.lower())) if k.lower().startswith(PREFIX_STORAGE_HEADER)))

		string_to_sign = request.get_method().upper() + NEW_LINE # verb
		if not for_tables:
			string_to_sign += (request.get_header('Content-encoding') or '') + NEW_LINE
			string_to_sign += (request.get_header('Content-language') or '') + NEW_LINE
			string_to_sign += (request.get_header('Content-length') or '') + NEW_LINE
		string_to_sign += (request.get_header('Content-md5') or '') + NEW_LINE
		string_to_sign += (request.get_header('Content-type') or '') + NEW_LINE
		string_to_sign += (request.get_header('Date') or '') + NEW_LINE
		if not for_tables:
			string_to_sign += (request.get_header('If-modified-since') or '') + NEW_LINE
			string_to_sign += (request.get_header('If-match') or '') + NEW_LINE
			string_to_sign += (request.get_header('If-none-match') or '') + NEW_LINE
			string_to_sign += (request.get_header('If-unmodified-since') or '') + NEW_LINE
			string_to_sign += (request.get_header('Range') or '') + NEW_LINE
		if not for_tables:
			string_to_sign += canonicalized_headers + NEW_LINE
		string_to_sign += canonicalized_resource

		request.add_header('Authorization', 'SharedKey ' + self._account + ':' + base64.encodestring(hmac.new(self._key, unicode(string_to_sign).encode("utf-8"), hashlib.sha256).digest()).strip())
		return request

	def sign_request(self, request, use_path_style_uris = None):
		return self._sign_request_impl(request, use_path_style_uris)

	def sign_table_request(self, request, use_path_style_uris = None):
		return self._sign_request_impl(request, for_tables = True, use_path_style_uris = use_path_style_uris)

class RequestWithMethod(Request):
	'''Subclass urllib2.Request to add the capability of using methods other than GET and POST.
	   Thanks to http://benjamin.smedbergs.us/blog/2008-10-21/putting-and-deleteing-in-python-urllib2/'''
	def __init__(self, method, *args, **kwargs):
		self._method = method
		Request.__init__(self, *args, **kwargs)

	def get_method(self):
		return self._method


class AzureStorageDriver(StorageDriver) :

	name = 'Windows Azure Storage'
	
	def __init__(self, key, secret = None, secure = True, host = None, port = None):
		self._credentials = SharedKeyCredentials(key, secret)
		StorageDriver.__init__(self, key, secret, secure, host, port)

	def get_base_url(self):
		return "http://%s.%s" % (self.key, CLOUD_BLOB_HOST)

	def container_exists(self,container_name):
		req = Request("%s/?comp=list&prefix=%s" % (self.get_base_url(), container_name))
		self._credentials.sign_request(req)
		dom = minidom.parseString(urlopen(req).read())
		containers = dom.getElementsByTagName("Container")
		for container in containers :
			if container_name == container.getElementsByTagName("Name")[0].firstChild.data :
				return True
		return False 

	def get_container(self, container_name):
		if self.container_exists(container_name) :
			ret = Container(container_name, None, self)
			return ret
		raise ContainerDoesNotExistError(value = None, driver = self, 
						container_name = container_name)
		return None

	def create_container(self, container_name):
		req = RequestWithMethod("PUT", "%s/%s?restype=container" % (self.get_base_url(), container_name))
		req.add_header("Content-Length", "0")
		self._credentials.sign_request(req)
		try:
			response = urlopen(req)
			if response.code == 201 :
				container = Container(name = container_name, extra = None, driver = self)
				return container
			else :
				raise LibcloudError("Unexpected status code %s" % (response.status), driver = self)
		except URLError, e:
			raise LibcloudError("Unexpected URLError %s " %(e.code), driver = self)


	def upload_object(self, file_path, container, object_name, extra = None,
				verify_hash = True, ex_storage_class = None):
		data  = open(file_path,'r').read()
		req = RequestWithMethod("PUT", "%s/%s/%s" % (self.get_base_url(), container.name, object_name), data)
		req.add_header("Content-Length", "%d" % len(data))
		req.add_header('x-ms-blob-type', 'BlockBlob')
		metadata = {}
		for key, value in metadata.items():
			req.add_header("x-ms-meta-%s" % key, value)
		req.add_header("Content-Type", "")
		self._credentials.sign_request(req)
		try:
			response = urlopen(req)
			if response.code == 201 :
				obj = Object(object_name, len(data), None, None, None, container, self)
				return obj
			else :
				raise LibcloudError("Unexpected status code %s" % (response.status) ,driver = self)	
		except URLError, e:
			raise LibcloudError("Unexpected URLError %s " %(e.code) ,driver = self)

	def object_exists(self, container_name, object_name):
		req = RequestWithMethod("HEAD", "%s/%s/%s" % (self.get_base_url(), container_name, object_name))
		self._credentials.sign_request(req)
		try:
			urlopen(req)
			return True
		except:
			return False

	def get_object(self, container_name, object_name):
		container = self.get_container(container_name)
		if self.object_exists(container_name, object_name) :
			obj = Object(object_name, size = None, hash = None, extra = None,
					meta_data = None,container = container, driver = self) 
			return obj

		else :
			raise ObjectDoesNotExistError(value = None, driver = self,
				 object_name = object_name)

	def download_object(self, obj, destination_path, overwrite_existing = False,
						delete_on_failure = True) :
		try :
			file = open(destination_path,'w')
			req = Request("%s/%s/%s" % (self.get_base_url(), obj.container.name, obj.name))
			self._credentials.sign_request(req)
			file.write(urlopen(req).read())
			file.close()
			return True
		except :
			return False

	def list_container_objects(self, container):
		marker = None
		while True:
			url = "%s/%s?restype=container&comp=list" % (self.get_base_url(), container.name)
			if not marker is None: url += "&marker=%s" % marker
			req = Request(url)
			self._credentials.sign_request(req)
			dom = minidom.parseString(urlopen(req).read())
			blobs = dom.getElementsByTagName("Blob")
			for blob in blobs:
				blob_name = blob.getElementsByTagName("Name")[0].firstChild.data
#				etag = blob.getElementsByTagName("Etag")[0].firstChild.data
				yield(Object(blob_name, size = None, hash = None, extra = None,
					meta_data = None,container = container, driver = self) )
			try: 
				marker = dom.getElementsByTagName("NextMarker")[0].firstChild.data
			except: 
				marker = None
			if marker is None: break

	def delete_object(self, obj):
		container_name = obj.container.name
		obj_name = obj.name
		try :
			req = RequestWithMethod("DELETE", "%s/%s/%s" % (self.get_base_url(), container_name, obj_name))
			self._credentials.sign_request(req)
			ret = urlopen(req)
			if ret.code / 100 == 2:
				return True
			else :
				raise LibcloudError("Unexpected status code %s" % (ret.status) ,driver = self)	
				return False
		except URLError, e:
			raise LibcloudError("Unexpected URLError %s " %(e.code) ,driver = self)
			return False





def main():
	pass

if __name__ == '__main__':
	main()
