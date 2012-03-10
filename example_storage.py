# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from pprint import pprint

from libcloud.storage.types import Provider
from libcloud.storage.providers import get_driver


# Following is the original libcloud interface.
CloudFiles = get_driver(Provider.CLOUDFILES_UK)

driver = CloudFiles('access key id', 'secret key')

driver.create_container("test_container")
test_container = driver.get_container("test_container")
temp_file = open("temp_file", "w")
driver.upload_object("temp_file", test_container, "temp_object")
temp_object = driver.get_object("test_container", "temp_object")
driver.download_object(temp_object, "temp_file", True)
driver.delete_object(obj)

containers = driver.list_containers()
container_objects = driver.list_container_objects(containers[0])
pprint(containers)
pprint(container_objects)

driver.delete_container(test_container)

# Following is the modified mlibcloud interface.
# As seen, it is quite similar to the original interfaces.
CloudFiles = get_driver(Provider.CLOUDFILES_UK)
S3 = get_driver(Provider.S3_US_WEST)	
GoogleStorage = get_driver(Provider.GOOGLE_STORAGE)

driver_cf = CloudFiles('id', 'key')
driver_s3 = S3('id', 'key')
driver_gs = GoogleStorage('id', 'key')

driver = GroupDriver([driver_cf, driver_s3, driver_gs])
driver.set_original_share(2)	#set k
driver.set_total_share(3)		#set m

driver.create_container("test_container")
test_container = driver.get_container("test_container")
temp_file = open("temp_file", "w")
driver.upload_object("temp_file", test_container, "temp_object")
temp_object = driver.get_object("test_container", "temp_object")
driver.download_object(temp_object, "temp_file", True)
driver.delete_object(obj)

containers = driver.list_containers()
container_objects = driver.list_container_objects(containers[0])
pprint(containers)
pprint(container_objects)

driver.delete_container(test_container)
