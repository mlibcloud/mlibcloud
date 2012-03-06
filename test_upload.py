from gen_file import gen_file
from fec_upload import fec_upload
from key_secret_dict import read_keys_from_file

def upload_test_init() :
	file_name = 'forkyoooo'
	file_size = 3 * 1024
	gen_file(file_name, file_size)
	return (file_name, file_size)

def upload_test() :
	file_name ,file_size = upload_test_init()
	container_name = 'forkyoooo' + '-mlb'
	block_size = 512
	k = 3
	m = 5
	stripe_location = ['ALIYUN_STORAGE' for i in range(m)]
	keys_file = 'mlibcloud_keys'
	keys_dict = read_keys_from_file(keys_file)

	fec_upload(file_name, container_name, block_size, k, m, stripe_location, keys_dict)


def main():
	upload_test()

if __name__ == '__main__' :
	main()

