from key_secret_dict import read_keys_from_file
import md5
from fec import fec_file
from fdc import fdc_file
from fec import write_streams_to_file
from meta import FileMeta

import threading
from os import path
from libcloud.storage.providers import get_driver
from libcloud.storage.types import Provider
from mtimer import mtimer

class upload_object_thread(threading.Thread):
	
	def __init__(self, driver, file_path, container, obj_name):
		threading.Thread.__init__(self)
		self.driver = driver
		self.file_path = file_path
		self.container = container
		self.obj_name = obj_name

	def run(self):
		try:
			self.driver.upload_object(self.file_path, self.container, self.obj_name, extra = {'content_type' : 'zip'})
		except : 
			print("upload error")


class download_object_thread(threading.Thread):
	def __init__(self, driver, obj, dest_path, timing = False):
		threading.Thread.__init__(self)
		self.driver = driver
		self.obj = obj
		self.dest_path = dest_path
		self.timing = timing
	def run(self):
		ret = False
		print(self.dest_path)
		if self.timing :
			mt = mtimer(self.driver.__class__.name)
			mt.begin()

		try:
			ret = self.driver.download_object(self.obj, self.dest_path, overwrite_existing=False, delete_on_failure=True)
			if self.timing :
				mt.end()
				self.time = mt.get_interval()
				self.name = mt.name
		except :
			print("download object error")
			if self.timing :
				self.time = float(9999999999)
				self.name = mt.name

		return ret



class GroupDriver :
	
	def __init__(self, driver_list):
		self.drivers = driver_list
		self.driver_dict = { d.__class__.name : d for d in self.drivers}
		#TODO
	
	def set_original_share(self, k):
		self.k = k

	def set_total_share(self, m):
		self.m = m

	def set_block_size(self, block_size):
		self.block_size = block_size

	def create_container(self, container_name):
		for d in self.drivers :
			d.create_container(container_name)

	def get_container(self, container_name):
		#return a list of container
		containers = None
		containers = [ d.get_container(container_name) for d in self.drivers ]
		return containers

	def upload_object(self, file_path, container, obj_name) :
		file_name = path.basename(file_path)
		file = open(file_path,'r')
		streams = fec_file(file, self.block_size, self.k, self.m)
		write_streams_to_file(streams,file_path)
		print("fec file complete")
		
		#generate .meta file
		meta = FileMeta()
		meta.set_name(file_name)
		meta.set_size(path.getsize(file_path))
		meta.set_blocksize(self.block_size)
		meta.set_k(self.k)
		meta.set_m(self.m)
		for i in range(self.m) :
			meta.set_stripe_location("s" + str(i), self.drivers[i].__class__.name)

		#generate md5 for stripes and .meta
		for i in range(self.m):
			file_it = open(file_path + '.' + str(i))
			meta.set_md5("c"+str(i), md5.new(file_it.read()).hexdigest())

		meta.set_md5("cmeta", meta.cal_md5())
	
		meta.save_to_file(file_path+'.meta')
		print("save meta complete")
	
		#threading upload
		stripe_threads = [upload_object_thread(self.drivers[i],
									 file_path+'.'+str(i),
									 container[i],
									 obj_name+'.'+str(i))
						for i in range(self.m) ]

		for it in stripe_threads :
			it.start()
		for it in stripe_threads :
			it.join()

		#upload .meta to cloud
		meta_threads = [upload_object_thread(self.drivers[i],
										file_path+'.meta',
										container[i],
										obj_name+'.meta')
						for i in range(self.m) ]
		for it in meta_threads :
			it.start()
		for it in meta_threads :
			it.join()

	def get_objects(self, container_name, obj_name):
		ret = None
		ret = [ self.drivers[i].get_object(container_name, obj_name) for i in range(len(self.drivers)) ]
		return ret

	def get_specific_objects(self, container_name, obj_name ,drivers) :
		ret = None
		ret = [ drivers[i].get_object(container_name[i], obj_name[i]) for i in range(len(drivers)) ]
		return ret

	def download_meta(self, meta_name, container_name, dest_path):
		meta = self.get_objects(container_name, meta_name)
		#dwonload .meta file
		meta_threads = [ download_object_thread(self.drivers[i],
												meta[i],
												dest_path + meta_name+'.'+str(i),
												timing = True)
						for i in range(len(self.drivers)) ]

		for it in meta_threads :
			it.start()
		for it in meta_threads :
			it.join()
		
		temp = {it.name : it.time for it in meta_threads }
		print('download meta complete')
		return temp
	
	def check_meta(self, meta_path) :
		meta = FileMeta()
		meta.load_from_file(meta_path)
		if meta.check_md5():
			return True
		return False
	
	def get_info_from_meta(self,meta) :
		k = meta['k']
		m = meta['m']
		file_size = meta["size"]
		block_size = meta["blocksize"]
		stripe_location = { 's' + str(i) : meta["s" + str(i)] for i in range(m) }
		print("read from meta completa")
		return (k, m, file_size, block_size, stripe_location)

	def download_object(self, container_name,  obj_name, dest_path):
		file_name = obj_name
		meta_name = file_name + '.meta'

#		metas = self.get_objects(container_name, meta_name)
		
		provider_time = self.download_meta(meta_name, container_name, dest_path)
		
		#check meta files
		meta = None
		for i in range(len(self.drivers)):
			if not self.check_meta(dest_path + meta_name + '.' + str(i)):
				print(" %d th meta md5 check False " % i)
			else :
				print(" %d th meta md5 check True " % i)
				meta = FileMeta()
				meta.load_from_file(dest_path + meta_name + '.' + str(i))
				break


		if meta == None : 
			print("There is no valid meta file ")
			return 

		k, m, file_size, block_size, stripe_location = self.get_info_from_meta(meta)
		#download k file stripes,according to download speed
		#temp is a list of  (provider , i, cost_time) sorted by cost_time ,i means the ith stripe
		temp = [ (	stripe_location['s' + str(i)], i, 
					provider_time[stripe_location['s' + str(i)]] )
				for i in range(m)]
		temp = sorted(temp ,key = lambda t : t[2] )
		#stripes_to_download is a list of ( driver, i) , i means the ith stripes
		stripes_to_download = [ (self.driver_dict[temp[i][0]] ,temp[i][1]) for i in range(k) ]
		container_names = [container_name for i in range(k)]
		obj_names = [file_name + '.' + str(i[1]) for i in stripes_to_download ]
		temp_drivers = [i[0] for i in stripes_to_download ]
		parts = [ stripes_to_download[i][1] for i in range(k) ]

		objs = self.get_specific_objects(container_names, obj_names, temp_drivers)
		stripe_threads = [ download_object_thread(stripes_to_download[i][0] ,
													objs[i],
													dest_path + file_name +'.' + str(parts[i]))
							for i in range(k) ]
		for it in stripe_threads :
			it.start()
		for it in stripe_threads :
			it.join()

		#fdc_file
		fdc_file(parts, block_size, k, m, file_name, dest_path, file_size)
		print("fdc_file complete")


	def list_container_objects(self, driver, container):
		ret = None
		ret = driver.list_container_objects(container)
		return ret


#	def delete_object(self, objs):
		#objs is a list of objs to delete
#		ret = None
#		ret = [ self.drivers[i].delete_object(objs[i]



