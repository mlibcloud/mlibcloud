from os import path
from fec import fec_file
from fec import write_streams_to_file
from meta import FileMeta
from m_upload import StorageUploader
from m_upload import createThread
from mlibcloud_keys import mlibcloudid
from mlibcloud_keys import mlibcloudkey
from provider_dict import get_cloud_provider


def Fec_Upload(file_name,block_size,k,m,stripe_location):

	'''fec_file'''
	file = open(file_name,"r")
	streams = fec_file(file,block_size,k,m)
	write_streams_to_file(streams,file_name)
	print("fec file complete")
	
	'''meta
	note: the size of stripe_location should be equal with m
	the content of stripe_location shoud be Storage providers'''
	meta = FileMeta()
	meta.set_name(file_name)
	meta.set_size(path.getsize(file_name))
	meta.set_blocksize(block_size)
	meta.set_k(k)
	meta.set_m(m)
	for i in range(m):
		meta.set_stripe_location("s" + str(i),stripe_location[i])
	
	meta.save_to_file()
	print("save meta complete")

	#threading upload
	#different Sotrage Providers should have different mlibcloudid and mlibcloudkey
	threads = [createThread(file_name + '.' + str(i),get_cloud_provider(stripe_location[i]),mlibcloudid,mlibcloudkey) for i in range(m)]
	for it in threads :
		it.start()
	for it in threads :
		it.join()

	#upload .meta to cloud
	meta_location = set(stripe_location)
	meta_threads = [createThread(file_name + '.meta',get_cloud_provider(i),mlibcloudid,mlibcloudkey) for i in meta_location]
	for it in meta_threads :
		it.start()
	for it in meta_threads :
		it.join()

	

def main():
	file_name = 'indata'
	block_size = 16
	k = 3
	m = 5
	stripe_location = ["S3_AP_NORTHEAST" for i in range(m)]
	Fec_Upload(file_name,block_size,k,m,stripe_location)


if __name__ == "__main__":
	main()


