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

__all__ = [
    'LinodeDNSDriver'
]


from libcloud.common.linode import (API_ROOT, LinodeException,
                                    LinodeConnection,
                                    LINODE_PLAN_IDS)
from libcloud.common.linode import API_HOST, API_ROOT, LinodeException
from libcloud.dns.types import Provider, RecordType
from libcloud.dns.types import ZoneDoesNotExistError, RecordDoesNotExistError
from libcloud.dns.base import DNSDriver, Zone, Record


VALID_ZONE_EXTRA_PARAMS = ['SOA_Email', 'Refresh_sec', 'Retry_sec',
                           'Expire_sec', 'status', 'master_ips']

VALID_RECORD_EXTRA_PARAMS = ['Priority', 'Weight', 'Port', 'Protocol',
                             'TTL_sec']

RECORD_TYPE_MAP = {
    RecordType.A: 'A',
    RecordType.AAAA: 'AAAAA',
    RecordType.CNAME: 'CNAME',
    RecordType.TXT: 'TXT',
    RecordType.SRV: 'SRV',
}


class LinodeDNSDriver(DNSDriver):
    type = Provider.LINODE
    name = 'Linode DNS'
    connectionCls = LinodeConnection

    def list_zones(self):
        params = {'api_action': 'domain.list'}
        data = self.connection.request(API_ROOT, params=params).objects[0]
        zones = self._to_zones(data)
        return zones

    def list_records(self, zone):
        params = {'api_action': 'domain.resource.list', 'DOMAINID': zone.id}

        try:
            data = self.connection.request(API_ROOT, params=params).objects[0]
        except LinodeException, e:
            # TODO: Refactor LinodeException, args[0] should be error_id
            if e.args[0] == 5:
                raise ZoneDoesNotExistError(value='', driver=self,
                                            zone_id=zone.id)

        records = self._to_records(items=data, zone=zone)
        return records

    def get_zone(self, zone_id):
        params = {'api_action': 'domain.list', 'DomainID': zone_id}
        data = self.connection.request(API_ROOT, params=params).objects[0]
        zones = self._to_zones(data)

        if len(zones) != 1:
            raise ZoneDoesNotExistError(value='', driver=self, zone_id=zone_id)

        return zones[0]

    def get_record(self, zone_id, record_id):
        zone = self.get_zone(zone_id=zone_id)
        params = {'api_action': 'domain.resource.list', 'DomainID': zone_id,
                   'ResourceID': record_id}
        data = self.connection.request(API_ROOT, params=params).objects[0]
        records = self._to_records(items=data, zone=zone)

        if len(records) != 1:
            raise RecordDoesNotExistError(value='', driver=self,
                                          record_id=record_id)

        return records[0]

    def create_zone(self, domain, type='master', ttl=None, extra=None):
        """
        Create a new zone.

        API docs: http://www.linode.com/api/dns/domain.create
        """
        params = {'api_action': 'domain.create', 'Type': type,
                  'Domain': domain}

        if ttl:
            params['TTL_sec'] = ttl

        merged = self._merge_valid_keys(params=params,
                                        valid_keys=VALID_ZONE_EXTRA_PARAMS,
                                        extra=extra)
        data = self.connection.request(API_ROOT, params=params).objects[0]
        zone = Zone(id=data['DomainID'], domain=domain, type=type, ttl=ttl,
                    extra=merged, driver=self)
        return zone

    def create_record(self, name, zone, type, data, extra=None):
        """
        Create a new record.

        API docs: http://www.linode.com/api/dns/domain.resource.create
        """
        params = {'api_action': 'domain.resource.create', 'DomainID': zone.id,
                  'Name': name, 'Target': data, 'Type': RECORD_TYPE_MAP[type]}
        merged = self._merge_valid_keys(params=params,
                                        valid_keys=VALID_RECORD_EXTRA_PARAMS,
                                        extra=extra)

        data = self.connection.request(API_ROOT, params=params).objects[0]
        record = Record(id=data['ResourceID'], name=name, type=type,
                        data=data, extra=merged, zone=zone, driver=self)
        return record

    def update_record(self, record, name, type, data, extra):
        """
        Update an existing record.

        API docs: http://www.linode.com/api/dns/domain.resource.update
        """
        params = {'api_action': 'domain.resource.update',
                  'ResourceID': record.id, 'DomainID': record.zone.id,
                  'Name': name, 'Target': data, 'Type': RECORD_TYPE_MAP[type]}
        merged = self._merge_valid_keys(params=params,
                                        valid_keys=VALID_RECORD_EXTRA_PARAMS,
                                        extra=extra)

        data = self.connection.request(API_ROOT, params=params).objects[0]
        record = Record(id=data['ResourceID'], name=name, type=type,
                        data=data, extra=merged, zone=record.zone, driver=self)
        return record

    def delete_zone(self, zone):
        params = {'api_action': 'domain.delete', 'DomainID': zone.id}

        try:
            data = self.connection.request(API_ROOT, params=params).objects[0]
        except LinodeException, e:
            # TODO: Refactor LinodeException, args[0] should be error_id
            if e.args[0] == 5:
                raise ZoneDoesNotExistError(value='', driver=self,
                                            zone_id=zone.id)

        return 'DomainID' in data

    def delete_record(self, record):
        params = {'api_action': 'domain.resource.delete',
                  'DomainID': record.zone.id, 'ResourceID': record.id}

        try:
            data = self.connection.request(API_ROOT, params=params).objects[0]
        except LinodeException, e:
            # TODO: Refactor LinodeException, args[0] should be error_id
            if e.args[0] == 5:
                raise RecordDoesNotExistError(value='', driver=self,
                                              record_id=record.id)

        return 'ResourceID' in data

    def _merge_valid_keys(self, params, valid_keys, extra):
        """
        Merge valid keys from extra into params dictionary and return
        dictionary with keys which have been merged.

        Note: params is modified in place.
        """
        merged = {}
        if not extra:
            return merged

        for key in valid_keys:
            if key in extra:
                params[key] = extra[key]
                merged[key] = extra[key]

        return merged

    def _to_zones(self, items):
        """
        Convert a list of items to the Zone objects.
        """
        zones = []

        for item in items:
            zones.append(self._to_zone(item))

        return zones

    def _to_zone(self, item):
        """
        Build an Zone object from the item dictionary.
        """
        extra = {'soa_email': item['SOA_EMAIL'], 'status': item['STATUS'],
                  'description': item['DESCRIPTION']}
        zone = Zone(id=item['DOMAINID'], domain=item['DOMAIN'],
                    type=item['TYPE'], ttl=item['TTL_SEC'], extra=extra,
                    driver=self)
        return zone

    def _to_records(self, items, zone=None):
        """
        Convert a list of items to the Record objects.
        """
        records = []

        for item in items:
            records.append(self._to_record(item=item, zone=zone))

        return records

    def _to_record(self, item, zone=None):
        """
        Build a Record object from the item dictionary.
        """
        extra = {'protocol': item['PROTOCOL'], 'ttl_sec': item['TTL_SEC'],
                  'port': item['PORT'], 'weight': item['WEIGHT']}
        type = self._string_to_record_type(item['TYPE'])
        record = Record(id=item['RESOURCEID'], name=item['NAME'], type=type,
                        data=item['TARGET'], extra=extra, zone=zone,
                        driver=self)
        return record