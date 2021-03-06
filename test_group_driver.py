
from GroupDriver import GroupDriver
from key_secret_dict import read_keys_from_file
from libcloud.storage.providers import get_driver
from libcloud.storage.types import Provider



def main():
	key_file = 'mlibcloud_keys'
	key_dict = read_keys_from_file(key_file)
	Google = get_driver(Provider.GOOGLE_STORAGE)
	S3 = get_driver(Provider.S3_US_WEST)
	Aliyun = get_driver(Provider.ALIYUN_STORAGE)

	driver_cf = Google(key_dict['GOOGLE_STORAGE'][0],key_dict['GOOGLE_STORAGE'][1])
	driver_s3 = S3(key_dict['S3_US_WEST'][0],key_dict['S3_US_WEST'][1])
	driver_ali = Aliyun(key_dict['ALIYUN_STORAGE'][0],key_dict['ALIYUN_STORAGE'][1])

	driver = GroupDriver([driver_cf, driver_s3, driver_ali])


	upload = True
	download = False
	delete = False

	if upload :
		#upload
		driver.set_original_share(2)
		driver.set_total_share(3)
		driver.set_block_size(512)

		container_name = 'fuckyou-mlibcloud'
		file_path = '/home/pin/debug/fuckyou'
		obj_name = 'fuckyou'

		driver.create_container(container_name)
		containers = driver.get_container(container_name)
		driver.upload_object(file_path, containers, obj_name)


	if download :
		#dwonload
		dest_path = '/home/pin/debug/'
		container_name = 'fuckyou-mlibcloud'
		obj_name = 'fuckyou'

		mobj = driver.get_object(container_name, obj_name)
		driver.download_object(mobj, dest_path)




if __name__ == '__main__':
	main()

