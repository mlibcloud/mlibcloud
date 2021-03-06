#!/usr/bin/env python
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



DEVSTORE_ACCOUNT = "devstoreaccount1"
DEVSTORE_SECRET_KEY = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="

DEVSTORE_BLOB_HOST = "127.0.0.1:10000"

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


class Storage(object):
#	def __init__(self, host, account_name, secret_key, use_path_style_uris):
	 def __init__(self, account_name, secret_key, use_path_style_uris = None , host = CLOUD_BLOB_HOST) :
		self._host = host
		self._account = account_name
		self._key = secret_key
		if use_path_style_uris is None:
			use_path_style_uris = re.match(r'^[^:]*[\d:]+$', self._host)
		self._use_path_style_uris = use_path_style_uris
		self._credentials = SharedKeyCredentials(self._account, self._key)

	def get_base_url(self):
		if self._use_path_style_uris:
			return "http://%s/%s" % (self._host, self._account)
		else:
			return "http://%s.%s" % (self._account, self._host)


class BlobStorage(Storage):
#	def __init__(self, host = DEVSTORE_BLOB_HOST, account_name = DEVSTORE_ACCOUNT, secret_key = DEVSTORE_SECRET_KEY, use_path_style_uris = None):
#		super(BlobStorage, self).__init__(host, account_name, secret_key, use_path_style_uris)

	def create_container(self, container_name, is_public = False):
		req = RequestWithMethod("PUT", "%s/%s?restype=container" % (self.get_base_url(), container_name))
		req.add_header("Content-Length", "0")
		if is_public: req.add_header(PREFIX_PROPERTIES + "publicaccess", "true")
		self._credentials.sign_request(req)
		try:
			response = urlopen(req)
			return response.code
		except URLError, e:
			return e.code

	def delete_container(self, container_name):
		req = RequestWithMethod("DELETE", "%s/%s?restype=container" % (self.get_base_url(), container_name))
		self._credentials.sign_request(req)
		try:
			response = urlopen(req)
			return response.code
		except URLError, e:
			return e.code

	def list_containers(self):
		req = Request("%s/?comp=list" % self.get_base_url())
		self._credentials.sign_request(req)
		dom = minidom.parseString(urlopen(req).read())
		containers = dom.getElementsByTagName("Container")
		for container in containers:
			container_name = container.getElementsByTagName("Name")[0].firstChild.data
			etag = container.getElementsByTagName("Etag")[0].firstChild.data
#			last_modified = time.strptime(container.getElementsByTagName("LastModified")[0].firstChild.data, TIME_FORMAT)
#			yield (container_name, etag, last_modified)
			yield (container_name,etag)
		
		dom.unlink() #Docs say to do this to force GC. Ugh.


	def container_exists(self,container_name):
		req = Request("%s/?comp=list&prefix=%s" % (self.get_base_url(), container_name))
		self._credentials.sign_request(req)
		dom = minidom.parseString(urlopen(req).read())
		containers = dom.getElementsByTagName("Container")
		for container in containers :
			if container_name == container.getElementsByTagName("Name")[0].firstChild.data :
				return True
		return False 


	def put_blob(self, container_name, blob_name, data, content_type = "", metadata = {}):
		req = RequestWithMethod("PUT", "%s/%s/%s" % (self.get_base_url(), container_name, blob_name), data=data)
		req.add_header("Content-Length", "%d" % len(data))
		req.add_header('x-ms-blob-type', 'BlockBlob')
		for key, value in metadata.items():
			req.add_header("x-ms-meta-%s" % key, value)
		req.add_header("Content-Type", content_type)
		self._credentials.sign_request(req)
		try:
			response = urlopen(req)
			return response.code
		except URLError, e:
			return e.code

	def delete_blob(self, container_name, blob_name):
		req = RequestWithMethod("DELETE", "%s/%s/%s" % (self.get_base_url(), container_name, blob_name))
		self._credentials.sign_request(req)
		urlopen(req)

	def get_blob(self, container_name, blob_name):
		req = Request("%s/%s/%s" % (self.get_base_url(), container_name, blob_name))
		self._credentials.sign_request(req)
		return urlopen(req).read()

	def get_blob_with_metadata(self, container_name, blob_name):
		req = Request("%s/%s/%s" % (self.get_base_url(), container_name, blob_name))
		self._credentials.sign_request(req)
		response = urlopen(req)
		metadata = {}
		for key, value in response.info().items():
			if key.startswith('x-ms-meta-'):
				metadata[key[len('x-ms-meta-'):]] = value
		return metadata, response.read()

	def blob_exists(self, container_name, blob_name):
		req = RequestWithMethod("HEAD", "%s/%s/%s" % (self.get_base_url(), container_name, blob_name))
		self._credentials.sign_request(req)
		try:
			urlopen(req)
			return True
		except:
			return False

	def list_blobs(self, container_name, blob_prefix=None):
		marker = None
		while True:
			url = "%s/%s?restype=container&comp=list" % (self.get_base_url(), container_name)
			if not blob_prefix is None: url += "&%s" % urlencode({"prefix": blob_prefix})
			if not marker is None: url += "&marker=%s" % marker
			req = Request(url)
			self._credentials.sign_request(req)
			dom = minidom.parseString(urlopen(req).read())
			blobs = dom.getElementsByTagName("Blob")
			for blob in blobs:
				blob_name = blob.getElementsByTagName("Name")[0].firstChild.data
				etag = blob.getElementsByTagName("Etag")[0].firstChild.data
#				last_modified = time.strptime(blob.getElementsByTagName("LastModified")[0].firstChild.data, TIME_FORMAT)
#				yield (blob_name, etag, last_modified)
				yield(blob_name,etag)
			try: marker = dom.getElementsByTagName("NextMarker")[0].firstChild.data
			except: marker = None
			if marker is None: break

	def put_block(self, container_name, blob_name, block_id, data):
		encoded_block_id = urlencode({"comp": "block", "blockid": block_id})
		req = RequestWithMethod("PUT", "%s/%s/%s?%s" % (self.get_base_url(), container_name, blob_name, encoded_block_id), data=data)
		req.add_header("Content-Type", "")
		req.add_header("Content-Length", "%d" % len(data))
		self._credentials.sign_request(req)
		try:
			response = urlopen(req)
			return response.code
		except URLError, e:
			return e.code


class AzureStorageDriver(StorageDriver) :
	
	def get_container(self, container_name):
		container

def main():
	pass

if __name__ == '__main__':
	main()
