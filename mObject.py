from libcloud.storage.base import Object
from meta import FileMeta

class mObject:
	def __init__(self, name, size, hash, extra, meta_data, 
				 obj_list, container_name, driver, integrated = False):
		self.objs = obj_list
		self.name = name
		self.size = size
		self.hash = hash
		self.extra = extra or {}
		self.meta_data = meta_data or None
		self.container_name = container_name
		self.driver = driver
		self.integrated = integrated 
#not integrated :name container_name driver

	def download(self,destination_path, overwrite_existing=False,
				delete_on_failure=True):
		return self.driver.download_object(self, destination_path,
										overwrite_existing,
										delete_on_failure)

	def delete(self):
		return self.driver.delete_object(self)

	def integrate(self):
		return self.driver.integrate(self)

