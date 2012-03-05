from os import path
import md5
import os
from fdc import fdc_file
from meta import FileMeta
from m_download import StorageDownloader
from m_download import createThread
from key_secret_dict import read_keys_from_file
from provider_dict import get_cloud_provider
from GLOBAL import EXP
from mtimer import mtimer


def folders_init(file_name, provider_list):
	#provider_list is a list of providers which user has that account
	#create folders like below
	#file_name-mlb/
	#	provider1/
	#	provider2/
	#	.../
	curdir = path.abspath('.') + '/' + file_name + '-mlb/'
	os.mkdir(curdir)

	for i in provider_list :
		os.mkdir(curdir + i)
	provider_dirs = [ curdir + i + '/'  for i in provider_list ]
	print("folders_init complete")
	return (provider_dirs, curdir)

def download_meta(meta_name, container_name, provider_list, keys_dict, provider_dirs) :
	#download .meta file
	#provider_list is a list of (str(provider_name))
	meta_threads = [ createThread(container_name,
								  meta_name,
								  get_cloud_provider(provider_list[i]),
								  provider_list[i],
								  keys_dict[provider_list[i]][0],
								  keys_dict[provider_list[i]][1],
								  provider_dirs[i],
								  timing = True)
					for i in range(len(provider_list)) ]
	for it in meta_threads:
		it.start()
	for it in meta_threads:
		it.join()
	temp = {it.name : it.time for it in meta_threads }
	return temp
	print("download .meta complete")


def check_meta(provider_dir, meta_name):
	#check single meta file md5
	meta = FileMeta()
	meta.load_from_file(provider_dir + meta_name)
	if meta.check_md5() :
		return True
	else :
		return False


def get_info_from_meta(meta) :
	#read k, m, file_size, block_size, stripe_location from meta Object
	k = meta["k"]
	m = meta["m"]
	file_size = meta["size"]
	block_size = meta["blocksize"]
	stripe_location = { 's' + str(i) : meta["s" + str(i)] for i in range(m) }
	print("read from meta completa")
	return (k, m, file_size, block_size, stripe_location)

def download_stripes(file_name, container_name, stripes, k, curdir, keys_dict):
	#stripes is a list of ( provider, i) ,i means ith stripe
	stripe_threads = [createThread(container_name,
								   file_name + '.' + str(stripes[i][1]),
								   get_cloud_provider(stripes[i][0]),
								   stripes[i][0],
								   keys_dict[stripes[i][0]][0],
								   keys_dict[stripes[i][0]][1],
								   curdir )
					  for i in range(k) ]
	for it in stripe_threads :
		it.start()
	for it in stripe_threads :
		it.join()



def fdc_download(file_name, container_name, provider_list, keys_dict) :
	#download file stripes and meta ,reconstruct to origin file

	meta_name = file_name + '.meta'
	#floder_init
	provider_dirs, curdir = folders_init(file_name, provider_list)
	#download meta files 

	if EXP :
		EXP_timer = mtimer("[EXP] download_meta :")
		EXP_timer.begin()

	provider_time = download_meta(meta_name, container_name, provider_list, keys_dict, provider_dirs)

	if EXP :
		EXP_timer.end()
		EXP_timer.record_data()

	#check meta files 
	meta = None
	for it in provider_dirs :
		if not check_meta(it, meta_name) :
			print("%s meta md5 check False" % it)
		else :
			print("%s meta md5 check True" % it)
			meta = FileMeta()
			meta.load_from_file(it + meta_name)
			break
	if meta == None :
		print("There is no valid meta file")
		return 
	#get info from meta
	k, m, file_size, block_size, stripe_location = get_info_from_meta(meta)

	#download k file stripes,according to download speed
	#temp is a list of  (provider , i, cost_time) sorted by cost_time ,i means the ith stripe
	temp = [ (	stripe_location['s' + str(i)], i, 
				provider_time[stripe_location['s' + str(i)]] )
			for i in range(m)]
	temp = sorted(temp ,key = lambda t : t[2] )
	#stripe_to_download is a list of ( provider, i) , i means the ith stripes
	stripe_to_download = [ (temp[i][0] ,temp[i][1]) for i in range(k) ]
	
	if EXP :
		EXP_timer.set_name("[EXP] download_stripes :")
		EXP_timer.begin()
		
	download_stripes(file_name, container_name, stripe_to_download, k, curdir, keys_dict)

	if EXP :
		EXP_timer.end()
		EXP_timer.record_data()

	#fdc_file
	parts = [ stripe_to_download[i][1] for i in range(k) ]

	if EXP :
		EXP_timer.set_name("[EXP] fdc_file :")
		EXP_timer.begin()

	fdc_file(parts ,block_size, k, m, file_name, curdir, file_size)
	
	if EXP :
		EXP_timer.end()
		EXP_timer.record_data()

	print("fdc file complete")


def main():
	file_name = "forkyou"
	container_name = "forkyou-mlb"
	keys_file = "mlibcloud_keys"
	provider_list = []
	provider_list.append("S3")
	keys_dict = read_keys_from_file(keys_file)
	fdc_download(file_name, container_name, provider_list, keys_dict)

if __name__ == "__main__" :
	main()

#vim :set tabstop=4
