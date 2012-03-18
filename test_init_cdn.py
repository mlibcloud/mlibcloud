from libcloud.storage.providers import get_driver
from libcloud.storage.types import Provider, ContainerDoesNotExistError, InvalidContainerNameError
from pprint import pprint
from RandomFile import RandomFile

def init_azure():
	Azure = get_driver(Provider.WINDOWS_AZURE_STORAGE)
	driver = Azure("mlibcloudcdn", "sE6yLNnYRjNM3vizbvbxfvEvDMUFEvtOfi01R2oZXH9NRfrh/ssOKfzMHNEoXhCqYaGnXMqWbJ7T+umzPWZGWA==")
	
	container_name = "mlibcloudcdn"
	if not driver.container_exists(container_name):
		driver.create_container(container_name)
	container = driver.get_container(container_name)
	#containers = driver.list_containers()
	#pprint(containers)
	
	
	#temp_file = open("temp_file", "w")
	f_group = RandomFile.create_group("Beijing", 0)
	for f_path in f_group:
		print "uploading to azure, %s" % f_path
		driver.upload_object(f_path, container, f_path)
	#	temp_object = driver.get_object(container_name, f_path)
	#	driver.download_object(temp_object, "temp_file", True)
	RandomFile.delete_group(f_group)
	#container_objects = driver.list_container_objects(container)
	#pprint(container_objects)
	#driver.delete_object(temp_object)
	
	
	#driver.delete_container(container)
	
def init_s3():
	S3 = get_driver(Provider.S3_AP_SOUTHEAST)
	driver = S3("AKIAITLX6IDDU5VTNAPA", "Pi0BhJiVan/l6a2+Yg9JVxrNvZSTRMGIx39XWAGq")
	container_name = "mlibcloudcdn"
	try:
		container = driver.get_container(container_name)
	except ContainerDoesNotExistError:
		try:
			container = driver.create_container(container_name)
		except InvalidContainerNameError:
			print "Oh!"
			exit()
	f_group = RandomFile.create_group("Beijing", 0)
	for f_path in f_group:
		driver.upload_object(f_path, container, f_path, {"content_type":"application/octor-stream"})
	RandomFile.delete_group(f_group)
	
if __name__ == "__main__":
	#init_azure()
	init_s3()
