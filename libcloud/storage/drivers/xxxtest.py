from oss_xml_handler import *
from oss_api import *



AccessID = 'hqxxyywptpn3juer4zd5rods'
AccessKey = 'WfUMI6vw28r0GD4gwPtNRpS/unU='
HOST = 'storage.aliyun.com'

oss = OssAPI(HOST, AccessID, AccessKey)


container_name = 'thisgeneration-mlibcloud'

res = oss.list_bucket(container_name)
body = res.read()
h = GetBucketXml(body).list()
ret = None
for i in h[0]:
	print(i[0])


