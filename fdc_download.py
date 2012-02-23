from os import path
import md5
import os
from fdc import fdc_file
from meta import FileMeta
from m_download import StorageDownloader
from m_download import createThread
from key_secret_dict import read_keys_from_file
from provider_dict import get_cloud_provider


def fdc_download(file_name, container_name, provider_list, keys_dict) :

	#provider_list is a list of providers which user has that account
	#create folders like below
	#file_name-mlb/
	# 	provider1/
	#	provider2/
	# 	.../
	curdir = path.abspath('.')+'/'+file_name+'-mlb/'
	os.mkdir(curdir)

	for i in provider_list :
		os.mkdir(curdir  + i)

	provider_dir = [ curdir  + i +'/' for i in provider_list]

	print("create folder complete")

	#download .meta to provider folder
	meta_name = file_name + '.meta'
	meta_threads = [createThread(container_name,
								 meta_name,
				     			 get_cloud_provider(provider_list[i]),
								 keys_dict[provider_list[i]][0],
								 keys_dict[provider_list[i]][1],
				     			 provider_dir[i] )
		       		for i in range(len(provider_list))]

	for it in meta_threads :
		it.start()
	for it in meta_threads :
		it.join()

	print("download meta complete")
	
	#check if .meta file's md5
	meta = FileMeta()
	meta.load_from_file(provider_dir[0] + meta_name)
	if meta.check_md5() : 
		print("meta md5 check True")
	else :
		print("meta md5 check False")
		return 

	#read k,m,block_size,stripe_location from .meta 
	meta.load_from_file(provider_dir[0]  + meta_name)
	k = meta["k"]
	m = meta["m"]
	file_size = meta["size"]
	block_size = meta["blocksize"]
	stripe_location = [ meta["s"+str(i)] for i in range(m)]

	print("read from meta complete")
	
	#threading download stripes to file_name-mlb folder
	
	stripe_threads = [createThread(container_name,
								   file_name+'.'+str(i),
								   get_cloud_provider(stripe_location[i]),
								   keys_dict[stripe_location[i]][0],
								   keys_dict[stripe_location[i]][1],
								   curdir) 
					 for i in range(m)]
	
	for it in stripe_threads :
		it.start()
	for it in stripe_threads :
		it.join()

	#fdc file 
	fdc_file(block_size, k, m, file_name, curdir, file_size)
	
	print("fdc file complete")

def main():
	file_name = "thisgeneration"
	container_name = "thisgeneration-mlb"
	keys_file = "mlibcloud_keys"
	provider_list = []
	provider_list.append("WINDOWS_AZURE_STORAGE")
	keys_dict = read_keys_from_file(keys_file)
	fdc_download(file_name, container_name, provider_list, keys_dict)


if __name__ == "__main__" :
	main()

#vim :set tabstop=4
