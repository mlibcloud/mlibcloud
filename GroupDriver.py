from key_secret_dict import read_keys_from_file
import socket
import md5
from fec import fec_file
from fdc import fdc_file
from fec import write_streams_to_file
from meta import FileMeta
from mObject import mObject

import threading
import os
import socket
from libcloud.storage.providers import get_driver
from libcloud.storage.types import Provider, ContainerDoesNotExistError
from mtimer import mtimer
from libcloud.storage.types import LibcloudError
from libcloud.storage.types import InvalidContainerNameError
from provider_dict import get_cloud_provider



class upload_object_thread(threading.Thread):
	
	def __init__(self, driver, file_path, container, obj_name, extra):
		threading.Thread.__init__(self)
		self.driver = driver
		self.file_path = file_path
		self.container = container
		self.obj_name = obj_name
		self.extra = extra
		self.ret = False

	def run(self):
		try:
			ret = self.driver.upload_object(self.file_path, self.container, self.obj_name, extra = self.extra)
			self.ret = ret != None and True or False
			if not self.ret :
				raise LibcloudError("upload %s failed" %self.obj_name, driver = self.driver)

		except LibcloudError, e: 
			print("upload error")
			print(e)
			self.ret = False
		except socket, e:
			self.ret = False


class download_object_thread(threading.Thread):
	def __init__(self, driver, obj, dest_path, overwrite_existing = True, timing = False):
		threading.Thread.__init__(self)
		self.driver = driver
		self.obj = obj
		self.dest_path = dest_path
		self.timing = timing
		self.ret = False
		self.owe = overwrite_existing
	def run(self):
		ret = False
		print(self.dest_path)
		if self.timing :
			mt = mtimer(self.driver.__class__.name)
			mt.begin()

		try:
			self.ret = self.driver.download_object(self.obj, self.dest_path, overwrite_existing=self.owe, delete_on_failure=True)
			if self.timing :
				mt.end()
				self.time = mt.get_interval()
				self.name = mt.name
			if not self.ret :
				raise LibcloudError("download %s failed " %self.obj.name , driver = self.driver )
		except (LibcloudError, socket.error):
			print("download object error")
			self.ret = False
			if self.timing :
				self.time = float(9999999999)
				self.name = mt.name

class list_container_objects_thread(threading.Thread) :
	def __init__(self, container):
		threading.Thread.__init__(self)
		self.container = container
		self.obj_list = None

	def run(self):
		self.obj_list = self.container.driver.list_container_objects(self.container)


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

	def get_provider_list(self) :
		provider_list = [ get_cloud_provider(d.__class__.name) 
							for d in self.drivers ]
		return provider_list

	def create_container(self, container_name):

		container_name_suffix = [ str(i) for i in self.get_provider_list() ]

		ret = [ None for i in range(self.m) ]
		for i in range(self.m) :
			try :
				ret[i] = self.drivers[i].create_container(container_name + container_name_suffix[i] )
			except InvalidContainerNameError :
				ret[i] = self.drivers[i].get_container(container_name + container_name_suffix[i] )
		return ret

	def get_container(self, container_name):
		#return a list of container
		containers = [ None for i in range(self.m) ]
		container_name_suffix = [str(i) for i in self.get_provider_list() ]
		try :
			for i in range(self.m) :
				containers[i] = self.drivers[i].get_container(container_name + container_name_suffix[i])
		except ContainerDoesNotExistError :
			raise 
		
		return containers

	def upload_object(self, file_path, container, obj_name, extra = None) :
		file_name = os.path.basename(file_path)
		file = open(file_path,'r')
		streams = fec_file(file, self.block_size, self.k, self.m)
		write_streams_to_file(streams,file_path)
		print("fec file complete")
		
		#generate .meta file
		meta = FileMeta()
		meta.set_name(file_name)
		meta.set_size(os.path.getsize(file_path))
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
									 obj_name+'.'+str(i),
									 extra)
						for i in range(self.m) ]

		for it in stripe_threads :
			it.start()
		for it in stripe_threads :
			it.join()

		for it in stripe_threads :
			if not it.ret :
				print it.driver.__class__.name		
				raise LibcloudError("upload stripes failed", driver = self)
				break

		#try to reupload
#		retry_count = 0
#		while retry_count < 3:
#			retry_threads = []
#			retry = False
#			retry_count += 1
#			for it in stripe_threads :
#				if not it.ret :
#					retry_threats.append(it)
#					retry = True
#			if not retry :
#				break
#			stripe_threads = retry_threads 
#			for it in stripe_threads :
#				it.start()
#			for it in stripe_threads :
#				it.join()

#		if retry :
#			print("upload stripes failed")
#			raise LibcloudError("upload stripes failed", driver = self )



		#upload .meta to cloud
		meta_threads = [upload_object_thread(self.drivers[i],
										file_path+'.meta',
										container[i],
										obj_name+'.meta',
										extra)
						for i in range(self.m) ]
		for it in meta_threads :
			it.start()
		for it in meta_threads :
			it.join()

		for it in meta_threads :
			if not it.ret :
				raise LibcloudError("upload metas failed", driver = self)
				break


		#retry upload 
#		retry_count = 0
#		while retry_count < 3:
#			retry_threads = []
#			retry = False
#			retry_count += 1
#			for it in meta_threads :
#				if not it.ret :
#					retry_threads.append(it)
#					retry = True
#			if not retry :
#				break
#			meta_threads = retry_threads 
#			for it in stripe_threads :
#				it.start()
#			for it in stripe_threads :
#				it.join()

#		if retry :
#			print("upload meta failed")
#			raise LibcloudError("upload meta failed", driver = self )



		#delete file stripes and file meta
		for i in range(self.m) :
			os.remove(file_path + '.' + str(i))
		os.remove(file_path + '.meta')


	def get_object(self, container_name, obj_name):
		meta_name = obj_name + '.meta'
		container_name_suffix = [str(i) for i in self.get_provider_list() ]
		#get .meta objects 
		meta_objs = [self.drivers[i].get_object(
								container_name+container_name_suffix[i], 
								meta_name)
					for i in range(len(self.drivers)) ]
		
		#dwonload .meta file
		if not os.path.exists('./temp'):
			os.mkdir('./temp')
		dest_path = './temp/'
		meta_threads = [ download_object_thread(self.drivers[i], meta_objs[i],
												dest_path + meta_name +'.'+str(i),
												overwrite_existing = True,
												timing = True)
						for i in range(len(self.drivers)) ]
		for it in meta_threads :
			it.start()
		for it in meta_threads :
			it.join()
		#driver_times {driver_name : driver_time }
		driver_times = {it.name : it.time for it in meta_threads }
		
		#check meta files
		meta = None
		for i in range(len(self.drivers)):
			if not self.check_meta( dest_path + meta_name + '.' + str(i)):
				print(" %d th meta md5 check False " % i)
			else :
				print(" %d th meta md5 check True " % i)
				meta = FileMeta()
				meta.load_from_file(dest_path + meta_name + '.' + str(i))
				break

		if meta == None : 
			print("There is no valid meta file ")
			raise LibcloudError("No valid meta file Error" ,driver = self)
			return None

		k, m, file_size, block_size, stripe_location = self.get_info_from_meta(meta)
		self.k  = k
		self.m  = m
		self.block_size = block_size
		#delete meta files 
		for i in range(m) :
			os.remove('./temp/' + meta_name + '.' + str(i))

		#download k file stripes,according to download speed
		#temp is a list of  (provider , i, cost_time) sorted by cost_time ,i means the ith stripe
		temp = [ (	stripe_location['s' + str(i)], i, 
					driver_times[stripe_location['s' + str(i)]] )
				for i in range(m)]
		temp = sorted(temp ,key = lambda t : t[2] )

		#stripes_to_download is a list of ( driver, i) , i means the ith stripes
		sorted_drivers = [ (self.driver_dict[temp[i][0]] ,temp[i][1]) 
								for i in range(m) ]
		obj_names = [obj_name + '.' + str(i[1]) for i in sorted_drivers ]
		obj_drivers = [i[0] for i in sorted_drivers ]
#		parts = [ sorted_drivers[i][1] for i in range(m) ]
		
		container_name_suffix = [ get_cloud_provider(obj_drivers[i].__class__.name)
									for i in range(m) ]


		obj_list = [ obj_drivers[i].get_object(
							container_name + container_name_suffix[i], 
							obj_names[i])
					for i in range(m) ]

		ret = mObject(obj_name, file_size, None, {} ,meta, obj_list, 
						container_name, obj_drivers )
		return ret

	
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
		print("read from meta complete")
		return (k, m, file_size, block_size, stripe_location)

	def download_object(self, mobj, dest_path, overwrite_existing = True):
		file_name = mobj.name
		objs = mobj.objs
		name_suffixs = [ os.path.splitext(i.name)[1] for i in objs] 

		if not os.path.exists('./temp'):
			os.mkdir('./temp')

		stripe_threads = [ download_object_thread(objs[i].driver, objs[i],
												'./temp/'+ file_name + name_suffixs[i],
												overwrite_existing)
							for i in range(self.k) ]
		for it in stripe_threads :
			it.start()
		for it in stripe_threads :
			it.join()


		success_objs = []
		pt = self.k
		while True :
			retry = False
			retry_threads = []
			for it in stripe_threads :
				if not it.ret :
					if pt < self.m :
						retry_threads.append( 
							download_object_thread(objs[pt].dirver,
												objs[pt],
												'./temp/'+file_name+name_suffixs[pt],
												overwrite_existing))
						pt += 1
						retry = True
					else :
						retry = False
						break
				else :
					success_objs.append(it.obj)

			if not retry :
				break
			stripe_threads = retry_threads
			for it in stripe_threads :
				it.start()
			for it in stripe_threads :
				it.join()

		if not (pt < self.m) :
			print("download stripes failed")
			raise LibcloudError("download stripes failed", driver = self )

		parts = [ int(os.path.splitext(i.name)[1][1:]) for i in success_objs ]

		#fdc_file
		fdc_file(parts, self.block_size, self.k, self.m, mobj.name, dest_path, mobj.size)
		print("fdc_file complete")

		#delete file stripes 
		for i in name_suffixs :
			os.remove('./temp/' + file_name + i)
		


	def list_container_objects(self, container):
		#container is a list of containers
		ret = None
		obj_list = []
		list_threads = [list_container_objects_thread(container[i]) 
						for i in range(self.m) ]
		
		for it in list_threads :
			it.start()
		for it in list_threads :
			it.join()

		for it in list_threads :
			obj_list.extend(it.obj_list)

		obj_name_list = []
		for i in obj_list :
			if cmp(i.name[-5 : ], '.meta') == 0 :
				obj_name_list.append(os.path.splitext(i.name)[0])
		obj_name_set = list(set(obj_name_list))
		ret = [self.get_object(container[i].name, obj_name_set[i]) 
				for i in range(len(obj_name_set)) ]
		return ret


	def delete_object(self, mobj):
		#delete .meta file
		for d in mobj.drivers :
			d.delete_object(d.get_object(mobj.container_name +
										get_cloud_provider(d.__class__.name), 
										mobj.name))
		
		#delete file stripes
		ret = [ d.delete_object(mobj.objs[i]) 
				for d in mobj.drivers ]			
		for it in ret :
			if not it :
				return False
		return True


