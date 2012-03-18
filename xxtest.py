from key_secret_dict import read_keys_from_file
from libcloud.storage.providers import get_driver
from libcloud.storage.types import Provider



def main():
	key_file = 'mlibcloud_keys'
	key_dict = read_keys_from_file(key_file)
#	Aliyun = get_driver(Provider.ALIYUN_STORAGE)
#	driver_ali = Aliyun(key_dict['ALIYUN_STORAGE'][0],key_dict['ALIYUN_STORAGE'][1])
#	driver = driver_ali
	Azure = get_driver(Provider.WINDOWS_AZURE_STORAGE)
	driver = Azure(key_dict['WINDOWS_AZURE_STORAGE'][0],key_dict['WINDOWS_AZURE_STORAGE'][1])
	container_name = 'thisgeneration-mlibcloud'
	obj_name = 'thisgeneration'

#	driver.create_container(container_name)
	container = driver.get_container(container_name)
#	driver.upload_object('/home/pin/debug/thisgeneration',container,obj_name)
	obj = driver.get_object(container_name,obj_name)
	print(obj)
	d = obj.driver
	d.download_object(obj,'/home/pin/Desktop')
	#dwonload
#	dest_path = '/home/pin/debug/'
#	container_name = 'fork-mlibcloud'
#	obj_name = 'forkyou'
#	driver.download_object(container_name, obj_name, dest_path)




if __name__ == '__main__':
	main()

