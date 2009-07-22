from libcloud.types import Provider, ProviderCreds

def get_provider(provider):
  if provider == Provider.DUMMY:
    from libcloud.drivers.dummy import DummyProvider
    return DummyProvider
  if provider == Provider.EC2:
    from libcloud.drivers.ec2 import EC2Provider
    return EC2Provider
  if provider == Provider.RACKSPACE:
    from libcloud.drivers.rackspace import RackspaceProvider
    return RackspaceProvider

def connect(provider, key, secret=None):
  creds = ProviderCreds(provider, key, secret)
  return get_provider(provider)(creds)