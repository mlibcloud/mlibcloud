from libcloud.storage.providers import get_driver
from libcloud.storage.types import Provider
from pprint import pprint
from RandomFile import RandomFile

if __name__ == "__main__":
	Azure = get_driver(Provider.WINDOWS_AZURE_STORAGE)
	driver = Azure("mlibcloudcdn", "sE6yLNnYRjNM3vizbvbxfvEvDMUFEvtOfi01R2oZXH9NRfrh/ssOKfzMHNEoXhCqYaGnXMqWbJ7T+umzPWZGWA==")
	
	container_name = "mlibcloud"
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
