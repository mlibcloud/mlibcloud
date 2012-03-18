from UserDict import UserDict
from libcloud.storage.types import Provider

Provider_Dict = UserDict()
Provider_Dict["Dummy Storage Provider"] = Provider.DUMMY
Provider_Dict["CloudFiles (US)"] = Provider.CLOUDFILES_US
Provider_Dict["CloudFiles (UK)"] = Provider.CLOUDFILES_UK
Provider_Dict["Amazon S3 (standard)"] = Provider.S3
Provider_Dict["Amazon S3 (us-west-1)"] = Provider.S3_US_WEST
Provider_Dict["Amazon S3 (us-west-2)"] = Provider.S3_US_WEST_OREGON
Provider_Dict["Amazon S3 (eu-west-1)"] = Provider.S3_EU_WEST
Provider_Dict["Amazon S3 (ap-southeast-1)"] = Provider.S3_AP_SOUTHEAST
Provider_Dict["Amazon S3 (ap-northeast-1)"] = Provider.S3_AP_NORTHEAST
Provider_Dict["Ninefold"] = Provider.NINEFOLD
Provider_Dict["Google Storage"] = Provider.GOOGLE_STORAGE
Provider_Dict["Windows Azure Storage"] = Provider.WINDOWS_AZURE_STORAGE
Provider_Dict["Aliyun Storage"] = Provider.ALIYUN_STORAGE

def get_cloud_provider(provider_name):
	return Provider_Dict[provider_name]
	



