from UserDict import UserDict
from libcloud.storage.types import Provider

Provider_Dict = UserDict()
Provider_Dict["DUMMY"] = Provider.DUMMY
Provider_Dict["CLOUDFILES_US"] = Provider.CLOUDFILES_US
Provider_Dict["CLOUDFILES_UK"] = Provider.CLOUDFILES_UK
Provider_Dict["S3"] = Provider.S3
Provider_Dict["S3_US_WEST"] = Provider.S3_US_WEST
Provider_Dict["S3_US_WEST_OREGON"] = Provider.S3_US_WEST_OREGON
Provider_Dict["S3_EU_WEST"] = Provider.S3_EU_WEST
Provider_Dict["S3_AP_SOUTHEAST"] = Provider.S3_AP_SOUTHEAST
Provider_Dict["S3_AP_NORTHEAST"] = Provider.S3_AP_NORTHEAST
Provider_Dict["NINEFOLD"] = Provider.NINEFOLD
Provider_Dict["GOOGLE_STORAGE"] = Provider.GOOGLE_STORAGE
Provider_Dict["WINDOWS_AZURE_STORAGE"] = Provider.WINDOWS_AZURE_STORAGE

def get_cloud_provider(provider_name):
	return Provider_Dict[provider_name]
	



