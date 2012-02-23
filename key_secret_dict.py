from UserDict import UserDict


def read_keys_from_file(file_name):
	#storage_provider"\t"key"\t"secret
	ret_dict = UserDict()
	file = open(file_name, 'r')
	for line in file :
		if line[-1] == '\n' :
			line = line[:-1]
		if len(line) == 0 :
			continue
		temp = line.split('\t',2)
		ret_dict[temp[0]] = (temp[1],temp[2])

	return ret_dict

