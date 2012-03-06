from fdc_download import fdc_download
from key_secret_dict import read_keys_from_file

def download_test():
	file_name = 'forkyoooo'
	container_name = 'forkyoooo' + '-mlb'
	provider_list = []
	provider_list.append("ALIYUN_STORAGE")
	keys_file = "mlibcloud_keys"
	keys_dict = read_keys_from_file(keys_file)
	
	fdc_download(file_name, container_name, provider_list, keys_dict)


def main():
	download_test()


if __name__ == '__main__':
	main()
