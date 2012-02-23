import md5
from os import path
from fec import fec_file
from fec import write_streams_to_file
from meta import FileMeta
from m_upload import StorageUploader
from m_upload import createThread
from key_secret_dict import read_keys_from_file
from provider_dict import get_cloud_provider


def Fec_Upload(file_name, container_name, block_size, k, m, stripe_location, keys_dict):

	#fec_file
	file = open(file_name,"r")
	streams = fec_file(file,block_size,k,m)
	write_streams_to_file(streams,file_name)
	print("fec file complete")
	
	#generate .meta file
	#note: the size of stripe_location should be equal with m
	#the content of stripe_location shoud be Storage providers
	meta = FileMeta()
	meta.set_name(file_name)
	meta.set_size(path.getsize(file_name))
	meta.set_blocksize(block_size)
	meta.set_k(k)
	meta.set_m(m)
	for i in range(m):
		meta.set_stripe_location("s" + str(i), stripe_location[i])


	#generate md5 for stripes and .meta
	for i in range(m):
		file_it = open(file_name+'.'+str(i))
		meta.set_md5("c"+str(i), md5.new(file_it.read()).hexdigest())
	

	meta.set_md5("cmeta", meta.cal_md5())
	
	meta.save_to_file()
	print("save meta complete")

	#threading upload
	#different Sotrage Providers should have different mlibcloudid and mlibcloudkey
	threads = [createThread(file_name + '.' + str(i), 
							container_name, 
							get_cloud_provider(stripe_location[i]),
							keys_dict[stripe_location[i]][0],
							keys_dict[stripe_location[i]][1])
				for i in range(m)]

	for it in threads :
		it.start()
	for it in threads :
		it.join()

	#upload .meta to cloud
	meta_location = set(stripe_location)
	meta_threads = [createThread(file_name + '.meta',
								container_name,
				     			get_cloud_provider(i), 
								keys_dict[i][0],
								keys_dict[i][1])
					for i in meta_location ]

	for it in meta_threads :
		it.start()
	for it in meta_threads :
		it.join()


def main():
	file_name = 'thisgeneration'
	container_name = "thisgeneration-mlb"
	keys_file = 'mlibcloud_keys'
	block_size = 16
	k = 3
	m = 5
	keys_dict = read_keys_from_file(keys_file)
	stripe_location = ["WINDOWS_AZURE_STORAGE" for i in range(m)]
	Fec_Upload(file_name, container_name, block_size, k, m, stripe_location, keys_dict)


if __name__ == "__main__":
	main()

#vim :set tabstop=4
