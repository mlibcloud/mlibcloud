import urllib2
from urllib2 import HTTPError
import time
from TestLogger import TestLogger
import os
from libcloud.common.types import LibcloudError
from libcloud.storage.providers import get_driver
from libcloud.storage.types import Provider, ContainerDoesNotExistError, ObjectHashMismatchError, ObjectDoesNotExistError
from libcloud.storage.drivers.atmos import AtmosError 
from RandomFile import RandomFile
import string
import socket
import random
import ssl
from GroupDriver import GroupDriver
from test_mlibcloud import test_azure_us




test_azure_us()









