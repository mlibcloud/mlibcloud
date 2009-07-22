from libcloud.types import NodeState, Node
import base64
import hmac
import httplib
import sha
import time
import urllib
import hashlib
import urlparse
from xml.etree import ElementTree as ET

AUTH_HOST = 'auth.api.rackspacecloud.com'
API_VERSION = 'v1.0'
NAMESPACE = 'http://docs.rackspacecloud.com/servers/api/v1.0'

class RackspaceConnection(object):
  def __init__(self, user, key):

    self.user = user
    self.key = key
    self.auth = httplib.HTTPSConnection("%s:%d" % (AUTH_HOST, 443))
    self.auth.request('GET', '/%s' % API_VERSION, headers={'X-Auth-User': user, 'X-Auth-Key': key})
    ret = self.auth.getresponse()
    self.token = ret.getheader('x-auth-token')
    self.endpoint = ret.getheader('x-server-management-url')

    (scheme, server, self.path, param, query, fragment) = urlparse.urlparse(self.endpoint)
    self.api = httplib.HTTPSConnection("%s:%d" % (server, 443))

  def _headers(self):
    return {'X-Auth-Token': self.token, 'Accept': 'application/xml' }

  def make_request(self, path, data=''):
    self.api.request('GET', '%s/%s' % (self.path, path), headers=self._headers())
    return self.api.getresponse()

  def list_servers(self):
    return Response(self.make_request('servers/detail'))

class Response(object):
  def __init__(self, http_response):
    self.http_response = http_response
    self.http_xml = http_response.read()

class RackspaceProvider(object):

  def __init__(self, creds):
    self.creds = creds
    self.api = RackspaceConnection(creds.key, creds.secret)

  def _fixxpath(self, xpath):
    # ElementTree wants namespaces in its xpaths, so here we add them.
    return "/".join(["{%s}%s" % (NAMESPACE, e) for e in xpath.split("/")])

  def _findtext(self, element, xpath):
    return element.findtext(self._fixxpath(xpath))

  def _to_node(self, element):
    states = {'BUILD':NodeState.PENDING,
              'ACTIVE':NodeState.RUNNING,
              'SUSPENDED':NodeState.TERMINATED,
              'QUEUE_RESIZE':NodeState.PENDING,
              'PREP_RESIZE':NodeState.PENDING,
              'RESCUE':NodeState.PENDING,
              'REBUILD':NodeState.PENDING,
              'REBOOT':NodeState.REBOOTING,
              'HARD_REBOOT':NodeState.REBOOTING }

    attribs = element.attrib

    node_attrs = attribs

    try:
      state = states[attribs['status']]
    except:
      state = NodeState.UNKNOWN

    n = Node(uuid = self.get_uuid(attribs['id']),
             name = attribs['name'],
             state = state,
             ipaddress = self._findtext(element, 'metadata/addresses/public'),
             creds = self.creds,
             attrs = node_attrs)
    return n

  def get_uuid(self, field):
    return hashlib.sha1("%s:%d" % (field,self.creds.provider)).hexdigest()
    
  def list_nodes(self):
    res = self.api.list_servers()
    return [ self._to_node(el) for el in ET.XML(res.http_xml).findall(self._fixxpath('server')) ]