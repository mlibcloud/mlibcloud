from libcloud.storage.drivers.oss_api import *
from libcloud.storage.drivers.oss_xml_handler import *
from urllib2 import Request, urlopen, URLError ,HTTPError

from libcloud.common.base import ConnectionUserAndKey, RawResponse
from libcloud.storage.base import Object, Container, StorageDriver
from libcloud.common.types import LazyList

from libcloud.common.types import InvalidCredsError, LibcloudError
from libcloud.storage.types import ContainerIsNotEmptyError
from libcloud.storage.types import InvalidContainerNameError
from libcloud.storage.types import ContainerDoesNotExistError
from libcloud.storage.types import ObjectDoesNotExistError
from libcloud.storage.types import ObjectHashMismatchError




class AliyunStorageDriver(OssAPI):
	
	name = 'Aliyun Storage'

	def __init__(self, key, secret ):
		OssAPI.__init__(self, 'storage.aliyun.com', key, secret)

	def get_container(self, container_name):
		res = self.list_all_my_buckets()
		if (res.status / 100) == 2:
			body = res.read()
			h = GetServiceXml(body)
			for i in h.list():
				if i[0] == container_name :
					ret = Container(container_name, None, self)
					return ret
		raise ContainerDoesNotExistError(value = None, driver = self,
										container_name = container_name)
		return None

	def create_container(self, container_name):
		try :
			res = self.put_bucket(container_name, 'private', {})
			if (res.status / 100) == 2 :
				ret = Container(container_name, None, self)
				return ret
			raise LibcloudError("Unexpected status code %s" % (response.status), driver = self)
			return None
		except HTTPError ,e :
			raise LibcloudError("Unexpected HTTPError %s " %(e.code), driver = self)
			return None
		except URLError, e:
			raise LibcloudError("Unexpected URLError %s " %(e.args), driver = self)
			return None

	def upload_object(self, file_path, container, object_name, extra = None,
				verify_hash = True, ex_storage_class = None):
		try :
			res = self.put_object_from_file(container.name, object_name, file_path, 'text/HTML', {})
			if (res.status / 100) == 2 :
				obj = Object(object_name, None, None, None, None, container, self)
				return obj
			else :
				raise LibcloudError("Unexpected status code %s" % (res.status) ,driver = self)	

		except HTTPError ,e :
			raise LibcloudError("Unexpected HTTPError %s " %(e.code), driver = self)

		except URLError, e:
			raise LibcloudError("Unexpected URLError %s " %(e.args), driver = self)



	def object_exists(self, container_name, object_name):
		try :
			res = self.list_bucket(container_name, prefix=object_name)
			body = res.read()
			h = GetBucketXml(body).list()
			if len(h[0]) == 0 :
				return False
			for i in h[0]:
				if i[0] == object_name :
					return True
			return False
		except :
			return False

	
	def get_object(self, container_name, object_name):
		container = self.get_container(container_name)
		if self.object_exists(container_name, object_name):
			obj = Object(object_name, size=None, hash=None, extra=None,
							meta_data=None, container=container, driver=self)
			return obj
		else :
			raise ObjectDoesNotExistError(value = None, driver = self,
				 object_name = object_name)

	def download_object(self, obj, destination_path, overwrite_existing = True,
						delete_on_failure = True) :
		try :
			file = open(destination_path,'w')
			res = self.object_operation("GET", obj.container.name, obj.name, {})
			if res.status == 200 :
				file.write(res.read())
			else :
				raise LibcloudError("Unexpected status code %s" % (res.status) ,driver = self)	
				file.close()
				return False
			file.close()
			return True
		except :
			return False

	def delete_object(self, obj):
		try :
			res = self.object_operation("DELETE", obj.container.name, obj.name, {} )
			if res.status / 100 == 2:
				return True
			return False
		except :
			return False

	def list_container_objects(self, container):
		ret = None
		try :
			res = self.list_bucket(container.name)
			body = res.read()
			h = GetBucketXml(body).list()
			if len(h[0]) == 0:
				ret = []
				return ret
			ret = [ Object(i[0], size=None, hash=None, extra=None,
                            meta_data=None, container=container, driver=self)
					for i in h[0] ]
			return ret
		except :
			return ret

