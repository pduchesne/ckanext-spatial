import re
import urllib
import urlparse

import logging

from ckan import model

from ckan.plugins.core import SingletonPlugin, implements

from ckanext.harvest.interfaces import IHarvester
from ckanext.harvest.model import HarvestObject
from ckanext.harvest.model import HarvestObjectExtra as HOExtra

from ckanext.spatial.lib.csw_client import CswService
from ckanext.spatial.harvesters.base import SpatialHarvester, text_traceback


class CSWHarvester(SpatialHarvester, SingletonPlugin):
    '''
    A Harvester for CSW servers
    '''
    implements(IHarvester)

    csw=None

    def info(self):
        return {
            'name': 'csw',
            'title': 'CSW Server',
            'description': 'A server that implements OGC\'s Catalog Service for the Web (CSW) standard'
            }


    def get_original_url(self, harvest_object_id):
        obj = model.Session.query(HarvestObject).\
                                    filter(HarvestObject.id==harvest_object_id).\
                                    first()

        parts = urlparse.urlparse(obj.source.url)

        params = {
            'SERVICE': 'CSW',
            'VERSION': '2.0.2',
            'REQUEST': 'GetRecordById',
            'OUTPUTSCHEMA': 'http://www.isotc211.org/2005/gmd',
            'OUTPUTFORMAT':'application/xml' ,
            'ID': obj.guid
        }

        url = urlparse.urlunparse((
            parts.scheme,
            parts.netloc,
            parts.path,
            None,
            urllib.urlencode(params),
            None
        ))

        return url

    def output_schema(self):
        return 'gmd'

    def gather_stage(self, harvest_job):
        log = logging.getLogger(__name__ + '.CSW.gather')
        log.debug('CswHarvester gather_stage for job: %r', harvest_job)
        # Get source URL
        url = harvest_job.source.url

        self._set_source_config(harvest_job.source.config)

        query = model.Session.query(HarvestObject.guid, HarvestObject.package_id).\
                                    filter(HarvestObject.current==True).\
                                    filter(HarvestObject.harvest_source_id==harvest_job.source.id)
        guid_to_package_id = {}

        for guid, package_id in query:
            guid_to_package_id[guid] = package_id

        guids_in_db = set(guid_to_package_id.keys())
        log.debug('Starting gathering for %s' % url)
        guids_in_harvest = set()

        is_static_xml = self.source_config.get('staticxml')

        try:
            if is_static_xml:
                self._setup_csw_client(None)
                csw = self.csw._Implementation(None, timeout=60, skip_caps=True)

                csw.request = url
                csw._invoke()
                csw.records = {}
                from owslib.csw import namespaces

                csw._parserecords(namespaces[self.output_schema()], 'brief')
                csw_identifiers = csw.records.keys()
            else:
                try:
                    self._setup_csw_client(url)
                except Exception, e:
                    self._save_gather_error('Error contacting the CSW server: %s' % e, harvest_job)
                    return None

                # extract cql filter if any
                cql = self.source_config.get('cql')

                csw_identifiers = self.csw.getidentifiers(page=10, outputschema=self.output_schema(), cql=cql)

            for identifier in csw_identifiers:
                try:
                    log.info('Got identifier %s from the CSW', identifier)
                    if identifier is None:
                        log.error('CSW returned identifier %r, skipping...' % identifier)
                        continue

                    guids_in_harvest.add(identifier)
                except Exception, e:
                    self._save_gather_error('Error for the identifier %s [%r]' % (identifier,e), harvest_job)
                    continue


        except Exception, e:
            log.error('Exception: %s' % text_traceback())
            self._save_gather_error('Error gathering the identifiers from the CSW server [%s]' % str(e), harvest_job)
            return None

        new = guids_in_harvest - guids_in_db
        delete = guids_in_db - guids_in_harvest
        change = guids_in_db & guids_in_harvest

        log.info('Received %s CSW records to harvest, %s new, %s to delete, %s to update', len(guids_in_harvest), len(new), len(delete), len(change))

        ids = []
        for guid in new:
            obj = HarvestObject(guid=guid, job=harvest_job,
                                extras=[HOExtra(key='status', value='new')])
            obj.save()
            ids.append(obj.id)
        for guid in change:
            obj = HarvestObject(guid=guid, job=harvest_job,
                                package_id=guid_to_package_id[guid],
                                extras=[HOExtra(key='status', value='change')])
            obj.save()
            ids.append(obj.id)
        for guid in delete:
            obj = HarvestObject(guid=guid, job=harvest_job,
                                package_id=guid_to_package_id[guid],
                                extras=[HOExtra(key='status', value='delete')])
            model.Session.query(HarvestObject).\
                  filter_by(guid=guid).\
                  update({'current': False}, False)
            obj.save()
            ids.append(obj.id)

        if len(ids) == 0:
            self._save_gather_error('No records received from the CSW server', harvest_job)
            return None

        return ids

    def fetch_stage(self,harvest_object):

        # Check harvest object status
        status = self._get_object_extra(harvest_object, 'status')

        if status == 'delete':
            # No need to fetch anything, just pass to the import stage
            return True

        log = logging.getLogger(__name__ + '.CSW.fetch')
        log.debug('CswHarvester fetch_stage for object: %s', harvest_object.id)

        url = harvest_object.source.url
        self._set_source_config(harvest_object.job.source.config)
        is_static_xml = self.source_config.get('staticxml')

        identifier = harvest_object.guid

        if is_static_xml:
            self._setup_csw_client(None)
            csw = self.csw._Implementation(None, timeout=60, skip_caps=True)

            csw.request = url
            csw._invoke()
            csw.records = {}
            from owslib.csw import namespaces

            csw._parserecords(namespaces[self.output_schema()], 'brief')
            record = self.csw._xmd(csw.records[identifier])
        else:
            try:
                self._setup_csw_client(url)
            except Exception, e:
                self._save_object_error('Error contacting the CSW server: %s' % e,
                                        harvest_object)
                return False

            try:
                record = self.csw.getrecordbyid([identifier], outputschema=self.output_schema())
            except Exception, e:
                self._save_object_error('Error getting the CSW record with GUID %s' % identifier, harvest_object)
                return False

        from owslib.util import bind_url
        from owslib import csw

        data = {
            'service': 'CSW', # self.csw.service,
            'version': '2.0.2', #self.csw.version,
            'request': 'GetRecordById',
            'outputFormat': 'application/xml',
            'outputSchema': csw.get_namespaces()[self.output_schema()],
            'elementsetname': "full",
            'id': '',
            }

        original_request = '%s%s%s' % (bind_url(url), urllib.urlencode(data), identifier)

        if record is None:
            self._save_object_error('Empty record for GUID %s' % identifier,
                                    harvest_object)
            return False

        try:
            # Save the fetch contents in the HarvestObject
            # Contents come from csw_client already declared and encoded as utf-8
            # Remove original XML declaration
            content = re.sub('<\?xml(.*)\?>', '', record['xml'])

            harvest_object.content = content.strip()
            harvest_object.raw_metadata_request = original_request
            harvest_object.save()
        except Exception,e:
            self._save_object_error('Error saving the harvest object for GUID %s [%r]' % \
                                    (identifier, e), harvest_object)
            return False

        log.debug('XML content saved (len %s)', len(record['xml']))
        return True

    def _setup_csw_client(self, url):
        self.csw = CswService(url)
        if (self.source_config and 'sortby' in self.source_config):
            self.csw.sortby =  self.source_config.get('sortby')

