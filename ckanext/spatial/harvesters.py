'''
Different harvesters for spatial metadata

These are designed for harvesting GEMINI2 for the UK Location Programme
but can be easily adapted for other INSPIRE/ISO19139 XML metadata
    - GeminiCswHarvester - CSW servers
    - GeminiDocHarvester - An individual GEMINI resource
    - GeminiWafHarvester - An index page with links to GEMINI resources

TODO: Harvesters for generic INSPIRE CSW servers

'''
import cgitb
import warnings
import urllib2
from urlparse import urlparse
from datetime import datetime
from string import Template
from numbers import Number
import sys
import uuid
import os
import logging
import difflib
import traceback

from lxml import etree
from pylons import config
from sqlalchemy.sql import update, bindparam
from sqlalchemy.exc import InvalidRequestError
from owslib import wms
from paste.deploy.converters import asbool

from ckan import model
from ckan.model import Session, Package
from ckan.lib.munge import munge_title_to_name
from ckan.plugins.core import SingletonPlugin, implements
from ckan.lib.helpers import json

from ckan import logic
from ckan.logic import get_action, ValidationError
from ckan.lib.navl.validators import not_empty

from ckanext.harvest.interfaces import IHarvester
from ckanext.harvest.model import HarvestObject, HarvestGatherError, \
                                    HarvestObjectError, HarvestGatherError

from ckanext.spatial.model import GeminiDocument
from ckanext.spatial.lib.csw_client import CswService
from ckanext.spatial.validation import Validators
from ckanext.spatial.lib.coupled_resource import extract_guid, update_coupled_resources

log = logging.getLogger(__name__)

class GetContentError(Exception):
    pass

def text_traceback():
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        res = 'the original traceback:'.join(
            cgitb.text(sys.exc_info()).split('the original traceback:')[1:]
        ).strip()
    return res

# When developing, it might be helpful to 'export DEBUG=1' to reraise the
# exceptions, rather them being caught.
debug_exception_mode = bool(os.getenv('DEBUG'))

class SpatialHarvester(object):
    # Q: Why does this not inherit from HarvesterBase in ckanext-harvest?
    # A: HarvesterBase just provides some useful util methods. The key thing
    #    a harvester does is it implements(IHarvester).

    @classmethod
    def _is_wms(cls, url):
        '''Given a URL this method returns whether it thinks it is a WMS server
        or not. It does it by making basic WMS requests.
        '''
        # Here's a neat way to test this manually:
        # python -c "import logging; logging.basicConfig(level=logging.INFO); from ckanext.spatial.harvesters import SpatialHarvester; print SpatialHarvester._is_wms('http://www.ordnancesurvey.co.uk/oswebsite/xml/atom/')"
        try:
            capabilities_url = wms.WMSCapabilitiesReader().capabilities_url(url)
            res = urllib2.urlopen(capabilities_url,None,10)
            xml = res.read()
            try:
                s = wms.WebMapService(url,xml=xml)
            except AttributeError, e:
                # e.g. http://csw.data.gov.uk/geonetwork/srv/en/csw
                log.info('WMS check for %s failed due to GetCapabilities response not containing a required field: %s', url, traceback.format_exc())
                return False
            except etree.XMLSyntaxError, e:
                # e.g. http://www.ordnancesurvey.co.uk/oswebsite/xml/atom/
                log.info('WMS check for %s failed parsing the XML response: %s', url, traceback.format_exc())
                return False
            return isinstance(s.contents, dict) and s.contents != {}
        except Exception, e:
            log.exception('WMS check for %s failed with uncaught exception: %s' % (url, str(e)))
        return False

    def _get_validator(self):
        if not hasattr(self, '_validator'):
            profiles = [
                x.strip() for x in
                config.get(
                    'ckan.spatial.validator.profiles',
                    'iso19139,gemini2',
                ).split(',')
            ]
            self._validator = Validators(profiles=profiles)
        return self._validator

    def _save_gather_error(self, message, job):
        err = HarvestGatherError(message=message, job=job)
        try:
            err.save()
        except InvalidRequestError:
            Session.rollback()
            err.save()
        finally:
            log.error(message)

    def _save_object_error(self,message,obj,stage=u'Fetch'):
        err = HarvestObjectError(message=message,object=obj,stage=stage)
        try:
            err.save()
        except InvalidRequestError,e:
            Session.rollback()
            err.save()
        finally:
            log.error(message)

    def _get_content(self, url):
        '''
        Requests the URL and returns the response.

        It returns a str string i.e. not unicode. The content will probably contain
        character encoding. The XML may have a declaration such as:
          <?xml version='1.0' encoding='ASCII'?>
        but often won\'t. The assumed encoding for Gemini2 is UTF8.

        May raise GetContentError.
        '''
        url = url.replace(' ','%20')
        try:
            http_response = urllib2.urlopen(url)
        except urllib2.HTTPError, e:
            raise GetContentError('Server responded with an error when accessing URL: %s Status: %s Reason: %r' % \
                   (url, e.code, e.msg))
            # NB HTTPError.reason is the documented way, but that only works for
            #    Python 2.7 and is an alias for HTTPError.msg anyway.
            return None
        except urllib2.URLError, e:
            raise GetContentError('URL syntax error or could not make connection to the host server. URL: "%s" Error: %r' % \
                                  (url, e.reason))
            return None
        return http_response.read()

class GeminiHarvester(SpatialHarvester):
    '''Base class for spatial harvesting GEMINI2 documents for the UK Location
    Programme. May be easily adaptable for other INSPIRE and spatial projects.

    All three harvesters share the same import stage
    '''

    force_import = False

    extent_template = Template('''
    {"type":"Polygon","coordinates":[[[$minx, $miny],[$minx, $maxy], [$maxx, $maxy], [$maxx, $miny], [$minx, $miny]]]}
    ''')

    def import_stage(self, harvest_object):
        log = logging.getLogger(__name__ + '.import')
        log.debug('Import stage for harvest object: %r', harvest_object)

        if not harvest_object:
            log.error('No harvest object received')
            return False

        # Save a reference
        self.obj = harvest_object

        if harvest_object.content is None:
            self._save_object_error('Empty content for object %s' % harvest_object.id,harvest_object,'Import')
            return False
        try:
            self.import_gemini_object(harvest_object.content,
                                      harvest_object.harvest_source_reference)
            log.info('Import completed - GUID %s', harvest_object.guid)
            return True
        except Exception, e:
            log.error('Exception during import: %s' % text_traceback())
            if not str(e).strip():
                self._save_object_error('Error importing Gemini document.', harvest_object, 'Import')
            else:
                self._save_object_error('Error importing Gemini document\n%s' % str(e), harvest_object, 'Import')

            if debug_exception_mode:
                raise

    def import_gemini_object(self, gemini_string, harvest_source_reference):
        '''Imports the Gemini metadata into CKAN.

        First it does XML Validation on the gemini.

        The harvest_source_reference is an ID that the harvest_source uses
        for the metadata document. It is the same ID the Coupled Resources
        use to link dataset and service records.

        Non-fatal errors are recorded with _save_object_error().
        Fatal errors simply raise an Exception.
        '''
        log = logging.getLogger(__name__ + '.import')

        # gemini_string is a unicode string because that is the type of the database
        # field. Harvests store the XML as a non-unicode string in whatever
        # encoding it came as (using _get_content) - these we can just call str() on
        # quite safely. But we enclose in the try: block because that will
        # be needed when we start using _get_content_as_unicode in the future.
        try:
            gemini_string = str(gemini_string)
        except UnicodeEncodeError:
            pass
        xml = etree.fromstring(gemini_string)

        valid, messages = self._get_validator().is_valid(xml)
        if not valid:
            reject = asbool(config.get('ckan.spatial.validator.reject', False))
            log.error('Errors found for object with GUID %s:' % self.obj.guid)
            out = ''
            if reject:
                out = '** ABORT! ** Import of this object is aborted because of errors during validation.\n\n'
            out += messages[0] + ':\n\n' + '\n\n'.join(messages[1:]) + '\n\n'
            if not reject:
                out += 'Validation errors have not caused the import of this object to be aborted.\n\n' # but possibly something else may cause abort later
            if reject:
                raise Exception(out)
            else:
                self._save_object_error(out,self.obj,'Import')

        unicode_gemini_string = etree.tostring(xml, encoding=unicode, pretty_print=True)

        # may raise Exception for errors
        package_dict = self.write_package_from_gemini_string(unicode_gemini_string)

        if package_dict:
            package = Session.query(Package).get(package_dict['id'])
            update_coupled_resources(package, harvest_source_reference)


    def write_package_from_gemini_string(self, content):
        '''Create or update a Package based on some content (gemini_string)
        that has come from a URL.

        Returns the package_dict of the result.
        If there is an error, it returns None or raises Exception.
        '''
        log = logging.getLogger(__name__ + '.import')
        package = None
        gemini_document = GeminiDocument(content)
        gemini_values = gemini_document.read_values()
        gemini_guid = gemini_values['guid']

        # Save the metadata reference date in the Harvest Object
        try:
            metadata_modified_date = datetime.strptime(gemini_values['metadata-date'],'%Y-%m-%d')
        except ValueError:
            try:
                metadata_modified_date = datetime.strptime(gemini_values['metadata-date'],'%Y-%m-%dT%H:%M:%S')
            except:
                raise Exception('Could not extract reference date for GUID %s (%s)' \
                        % (gemini_guid,gemini_values['metadata-date']))

        self.obj.metadata_modified_date = metadata_modified_date
        self.obj.save()

        last_harvested_object = Session.query(HarvestObject) \
                            .filter(HarvestObject.guid==gemini_guid) \
                            .filter(HarvestObject.current==True) \
                            .all()

        if len(last_harvested_object) == 1:
            last_harvested_object = last_harvested_object[0]
        elif len(last_harvested_object) > 1:
                raise Exception('Application Error: more than one current record for GUID %s' % gemini_guid)

        reactivate_package = False
        if last_harvested_object:
            # We've previously harvested this (i.e. it's an update)

            # Use metadata modified date instead of content to determine if the package
            # needs to be updated
            if last_harvested_object.metadata_modified_date is None \
                or last_harvested_object.metadata_modified_date < self.obj.metadata_modified_date \
                or self.force_import \
                or (last_harvested_object.metadata_modified_date == self.obj.metadata_modified_date and
                    last_harvested_object.source.active is False):

                if self.force_import:
                    log.info('Import forced for object %s with GUID %s' % (self.obj.id,gemini_guid))
                else:
                    log.info('Package for object with GUID %s needs to be created or updated' % gemini_guid)

                package = last_harvested_object.package

                # If the package has a deleted state, we will only update it and reactivate it if the
                # new document has a more recent modified date
                if package.state == u'deleted':
                    if last_harvested_object.metadata_modified_date < self.obj.metadata_modified_date:
                        log.info('Package for object with GUID %s will be re-activated' % gemini_guid)
                        reactivate_package = True
                    else:
                         log.info('Remote record with GUID %s is not more recent than a deleted package, skipping... ' % gemini_guid)
                         return None

            else:
                if last_harvested_object.content != self.obj.content and \
                 last_harvested_object.metadata_modified_date == self.obj.metadata_modified_date:
                    diff_generator = difflib.unified_diff(
                        last_harvested_object.content.split('\n'),
                        self.obj.content.split('\n'))
                    diff = '\n'.join([line for line in diff_generator])
                    raise Exception('The contents of document with GUID %s changed, but the metadata date has not been updated.\nDiff:\n%s' % (gemini_guid, diff))
                else:
                    # The content hasn't changed, no need to update the package
                    log.info('Document with GUID %s unchanged, skipping...' % (gemini_guid))
                return None
        else:
            log.info('No package with GEMINI guid %s found, let\'s create one' % gemini_guid)

        extras = {
            'UKLP': 'True',
            'harvest_object_id': self.obj.id,
            'harvest_source_reference': self.obj.harvest_source_reference,
            'metadata-date': metadata_modified_date.strftime('%Y-%m-%d'),
        }

        # Just add some of the metadata as extras, not the whole lot
        for name in [
            # Essentials
            'bbox-east-long',
            'bbox-north-lat',
            'bbox-south-lat',
            'bbox-west-long',
            'spatial-reference-system',
            'guid',
            # Usefuls
            'dataset-reference-date',
            'resource-type',
            'metadata-language', # Language
            'coupled-resource',
            'contact-email',
            'frequency-of-update',
            'spatial-data-service-type',
        ]:
            extras[name] = gemini_values[name]

        # Licence
        licence_extras = self._process_licence(
            use_constraints=gemini_values.get('use-constraints', ''),
            anchor_href=gemini_values.get('use-constraints-anchor-href'),
            anchor_title=gemini_values.get('use-constraints-anchor-title'),
            )
        extras.update(licence_extras)

        extras['access_constraints'] = gemini_values.get('limitations-on-public-access','')
        if gemini_values.has_key('temporal-extent-begin'):
            #gemini_values['temporal-extent-begin'].sort()
            extras['temporal_coverage-from'] = gemini_values['temporal-extent-begin']
        if gemini_values.has_key('temporal-extent-end'):
            #gemini_values['temporal-extent-end'].sort()
            extras['temporal_coverage-to'] = gemini_values['temporal-extent-end']

        # Save responsible organization roles
        provider, responsible_parties = self._process_responsible_organisation(
            gemini_values['responsible-organisation'])
        extras['provider'] = provider
        extras['responsible-party'] = '; '.join(responsible_parties)

        # Construct a GeoJSON extent so ckanext-spatial can register the extent geometry
        extent_string = self.extent_template.substitute(
                minx = extras['bbox-east-long'],
                miny = extras['bbox-south-lat'],
                maxx = extras['bbox-west-long'],
                maxy = extras['bbox-north-lat']
                )

        extras['spatial'] = extent_string.strip()

        tags = []
        for tag in gemini_values['tags']:
            tag = tag[:50] if len(tag) > 50 else tag
            tags.append({'name':tag})

        package_dict = {
            'title': gemini_values['title'],
            'notes': gemini_values['abstract'],
            'tags': tags,
            'resources':[]
        }

        if self.obj.source.publisher_id:
            package_dict['groups'] = [{'id':self.obj.source.publisher_id}]


        if reactivate_package:
            package_dict['state'] = u'active'

        if package is None or package.title != gemini_values['title']:
            name = self.gen_new_name(gemini_values['title'])
            if not name:
                name = self.gen_new_name(str(gemini_guid))
            if not name:
                raise Exception('Could not generate a unique name from the title or the GUID. Please choose a more unique title.')
            package_dict['name'] = name
        else:
            package_dict['name'] = package.name

        resource_locators = gemini_values.get('resource-locator', [])

        if len(resource_locators):
            for resource_locator in resource_locators:
                url = resource_locator.get('url','')
                if url:
                    resource_format = ''
                    resource = {}
                    if extras['resource-type'] == 'service':
                        # Check if the service is a view service
                        test_url = url.split('?')[0] if '?' in url else url
                        if self._is_wms(test_url):
                            resource['verified'] = True
                            resource['verified_date'] = datetime.now().isoformat()
                            resource_format = 'WMS'
                    resource.update(
                        {
                            'url': url,
                            'name': resource_locator.get('name',''),
                            'description': resource_locator.get('description') if resource_locator.get('description') else 'Resource locator',
                            'format': resource_format or None,
                            'resource_locator_protocol': resource_locator.get('protocol',''),
                            'resource_locator_function':resource_locator.get('function','')

                        })
                    package_dict['resources'].append(resource)

            # Guess the best view service to use in WMS preview
            verified_view_resources = [r for r in package_dict['resources'] if 'verified' in r and r['format'] == 'WMS']
            if len(verified_view_resources):
                verified_view_resources[0]['ckan_recommended_wms_preview'] = True
            else:
                view_resources = [r for r in package_dict['resources'] if r['format'] == 'WMS']
                if len(view_resources):
                    view_resources[0]['ckan_recommended_wms_preview'] = True

        extras_as_dict = []
        for key,value in extras.iteritems():
            if isinstance(value,(basestring,Number)):
                extras_as_dict.append({'key':key,'value':value})
            else:
                extras_as_dict.append({'key':key,'value':json.dumps(value)})

        package_dict['extras'] = extras_as_dict

        if package == None:
            # Create new package from data.
            package = self._create_package_from_data(package_dict)
            log.info('Created new package ID %s with GEMINI guid %s', package['id'], gemini_guid)
        else:
            package = self._create_package_from_data(package_dict, package = package)
            log.info('Updated existing package ID %s with existing GEMINI guid %s', package['id'], gemini_guid)

        # Flag the other objects of this source as not current anymore
        from ckanext.harvest.model import harvest_object_table
        u = update(harvest_object_table) \
                .where(harvest_object_table.c.package_id==bindparam('b_package_id')) \
                .values(current=False)
        Session.execute(u, params={'b_package_id':package['id']})
        Session.commit()

        # Refresh current object from session, otherwise the
        # import paster command fails
        Session.remove()
        Session.add(self.obj)
        Session.refresh(self.obj)

        # Set reference to package in the HarvestObject and flag it as
        # the current one
        if not self.obj.package_id:
            self.obj.package_id = package['id']

        self.obj.current = True
        self.obj.save()

        assert gemini_guid == [e['value'] for e in package['extras'] if e['key'] == 'guid'][0]
        assert self.obj.id == [e['value'] for e in package['extras'] if e['key'] ==  'harvest_object_id'][0]

        return package # i.e. a package_dict

    @classmethod
    def _process_responsible_organisation(cls, responsible_organisations):
        '''Given the list of responsible_organisations and their roles,
        (extracted from the GeminiDocument) determines who the provider is
        and the list of all responsible organisations and their roles.

        :param responsible_organisations: list of dicts, each with keys
                      includeing 'organisation-name' and 'role'
        :returns: tuple of: 'provider' (string, may be empty) and
                  'responsible-parties' (list of strings)
        '''
        parties = {}
        owners = []
        publishers = []
        for responsible_party in responsible_organisations:
            if responsible_party['role'] == 'owner':
                owners.append(responsible_party['organisation-name'])
            elif responsible_party['role'] == 'publisher':
                publishers.append(responsible_party['organisation-name'])

            if responsible_party['organisation-name'] in parties:
                if not responsible_party['role'] in parties[responsible_party['organisation-name']]:
                    parties[responsible_party['organisation-name']].append(responsible_party['role'])
            else:
                parties[responsible_party['organisation-name']] = [responsible_party['role']]

        responsible_parties = []
        for party_name in parties:
            responsible_parties.append('%s (%s)' % (party_name, ', '.join(parties[party_name])))

        # Save provider in a separate extra:
        # first organization to have a role of 'owner', and if there is none, first one with
        # a role of 'publisher'
        if len(owners):
            provider = owners[0]
        elif len(publishers):
            provider = publishers[0]
        else:
            provider = u''

        return provider, responsible_parties

    @classmethod
    def _process_licence(cls, use_constraints, anchor_href, anchor_title):
        '''
        The three "use-constraints" fields can contain three
        sorts of values:
          * use-constraints - free text and URLs
          * use-constraints-anchor-href - URLs
          * use-constraints-anchor-title - names for URLs

        These are extracted into their types and deposited into
        three extra fields:
          * licence URL -> extras['licence_url']
          * licence name for the licence URL -> extras['licence_url_title']
          * free text and subsequent URLs -> extras['licence']

        URLs in use-constraints-anchor-href takes priority over those
        in use-constraints.
        '''
        extras = {}

        free_text = []
        urls = []
        if anchor_href:
            if anchor_title:
                urls.append((anchor_href, anchor_title))
            else:
                urls.append(anchor_href)
        for use_constraint in use_constraints:
            if cls._is_url(use_constraint):
                urls.append(use_constraint)
            else:
                free_text.append(use_constraint)
        extras['licence'] = free_text or ''
        if urls:
            # first url goes in the licence_url field
            url = urls[0]
            if isinstance(url, tuple):
                extras['licence_url'] = url[0]
                extras['licence_url_title'] = url[1]
            else:
                extras['licence_url'] = url
            # subsequent urls just go in the licence field
            for url in urls[1:]:
                extras['licence'].append(url)

        # TODO try and match a licence_url to an appropriate license_id and
        # save it in the license_id field, but that requires the form schema
        # to allow both license_id and licence.

        return extras

    def gen_new_name(self, title):
        name = munge_title_to_name(title).replace('_', '-')
        while '--' in name:
            name = name.replace('--', '-')
        like_q = u'%s%%' % name
        pkg_query = Session.query(Package).filter(Package.name.ilike(like_q)).limit(100)
        taken = [pkg.name for pkg in pkg_query]
        if name not in taken:
            return name
        else:
            counter = 1
            while counter < 101:
                if name+str(counter) not in taken:
                    return name+str(counter)
                counter = counter + 1
            return None

    @classmethod
    def _extract_licence_urls(cls, licences):
        '''Given a list of pieces of licence info, hunt for all the ones
        that looks like a URL and return them as a list.'''
        licence_urls = []
        for licence in licences:
            if cls._is_url(licence):
                licence_urls.append(licence)
        return licence_urls

    @classmethod
    def _is_url(cls, licence_str):
        '''Given a string containing licence text, return boolean
        whether it looks like a URL or not.'''
        o = urlparse(licence_str)
        return o.scheme and o.netloc

    def _create_package_from_data(self, package_dict, package = None):
        '''
        Given a package_dict describing a package, creates or updates
        a package object. If you supply package then it will update it,
        otherwise it will create a new one.

        Uses the logic layer to create it.

        Returns a package_dict of the resulting package.

        {'name': 'council-owned-litter-bins',
         'notes': 'Location of Council owned litter bins within Borough.',
         'resources': [{'description': 'Resource locator',
                        'format': 'Unverified',
                        'url': 'http://www.barrowbc.gov.uk'}],
         'tags': [{'name':'Utility and governmental services'}],
         'title': 'Council Owned Litter Bins',
         'extras': [{'key':'INSPIRE','value':'True'},
                    {'key':'bbox-east-long','value': '-3.12442'},
                    {'key':'bbox-north-lat','value': '54.218407'},
                    {'key':'bbox-south-lat','value': '54.039634'},
                    {'key':'bbox-west-long','value': '-3.32485'},
                    # etc.
                    ]
        }
        '''

        if not package:
            package_schema = logic.schema.default_create_package_schema()
        else:
            package_schema = logic.schema.default_update_package_schema()

        # The default package schema does not like Upper case tags
        tag_schema = logic.schema.default_tags_schema()
        tag_schema['name'] = [not_empty,unicode]
        package_schema['tags'] = tag_schema

        # TODO: user
        context = {'model':model,
                   'session':Session,
                   'user':'harvest',
                   'schema':package_schema,
                   'extras_as_string':True,
                   'api_version': '2'}
        if not package:
            # We need to explicitly provide a package ID, otherwise ckanext-spatial
            # won't be be able to link the extent to the package.
            package_dict['id'] = unicode(uuid.uuid4())
            package_schema['id'] = [unicode]

            action_function = get_action('package_create')
        else:
            action_function = get_action('package_update')
            package_dict['id'] = package.id

        try:
            package_dict = action_function(context, package_dict)
        except ValidationError,e:
            raise Exception('Validation Error: %s' % str(e.error_summary))
            if debug_exception_mode:
                raise

        return package_dict

    def get_gemini_string_and_guid(self,gemini_string,url=None):
        '''From a string buffer containing Gemini XML, return the tree
        under gmd:MD_Metadata and the GUID for it. 

        If it cannot parse the XML it will raise lxml.etree.XMLSyntaxError.
        If it cannot find the GUID element, then gemini_guid will be ''.

        :param content: string containing Gemini XML (character encoded, not unicode)
        :param url: string giving info about the location of the XML to be
                    used only in validation errors
        :returns: (gemini_string, gemini_guid)
        '''
        try:
            gemini_string = str(gemini_string)
        except UnicodeEncodeError:
            pass
        xml = etree.fromstring(gemini_string)

        # The validator and GeminiDocument don\'t like the container
        metadata_tag = '{http://www.isotc211.org/2005/gmd}MD_Metadata'
        if xml.tag == metadata_tag:
            gemini_xml = xml
        else:
            gemini_xml = xml.find(metadata_tag)

        if gemini_xml is None:
            self._save_gather_error('Content is not a valid Gemini document without the gmd:MD_Metadata element', self.harvest_job)

        gemini_string = etree.tostring(gemini_xml)
        gemini_document = GeminiDocument(gemini_string)
        try:
            gemini_guid = gemini_document.read_value('guid')
        except KeyError:
            gemini_guid = None

        return gemini_string, gemini_guid


class GeminiCswHarvester(GeminiHarvester, SingletonPlugin):
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

    def gather_stage(self, harvest_job):
        log = logging.getLogger(__name__ + '.CSW.gather')
        #log = HarvestGatherLogger(harvest_job)
        log.debug('GeminiCswHarvester gather_stage for job: %r', harvest_job)
        # Get source URL
        url = harvest_job.source.url

        try:
            self._setup_csw_client(url)
        except Exception, e:
            self._save_gather_error('Error contacting the CSW server: %s' % e, harvest_job)
            return None


        log.debug('Starting gathering for %s' % url)
        used_identifiers = []
        ids = []
        try:
            for identifier in self.csw.getidentifiers(page=10):
                try:
                    log.info('Got identifier %s from the CSW', identifier)
                    if identifier in used_identifiers:
                        log.error('CSW identifier %r already used, skipping...' % identifier)
                        continue
                    if identifier is None:
                        log.error('CSW returned identifier %r, skipping...' % identifier)
                        ## log an error here? happens with the dutch data
                        continue

                    # Create a new HarvestObject for this identifier
                    obj = HarvestObject(guid=identifier, job=harvest_job,
                                        harvest_source_reference=identifier)
                    # NB: Gemini uses GUID for the harvest_source_reference
                    #     whereas INSPIRE specifies the Unique Resource
                    #     Identifier
                    obj.save()

                    ids.append(obj.id)
                    used_identifiers.append(identifier)
                except Exception, e:
                    self._save_gather_error('Error for the identifier %s [%r]' % (identifier,e), harvest_job)
                    if debug_exception_mode:
                        raise
                    continue

        except Exception, e:
            log.error('Exception: %s' % text_traceback())
            self._save_gather_error('Error gathering the identifiers from the CSW server [%s]' % str(e), harvest_job)
            if debug_exception_mode:
                raise
            return None

        if len(ids) == 0:
            self._save_gather_error('No records received from the CSW server', harvest_job)
            return None

        return ids

    def fetch_stage(self,harvest_object):
        log = logging.getLogger(__name__ + '.CSW.fetch')
        log.debug('GeminiCswHarvester fetch_stage for object: %r', harvest_object)

        url = harvest_object.source.url
        try:
            self._setup_csw_client(url)
        except Exception, e:
            self._save_object_error('Error contacting the CSW server: %s' % e,
                                    harvest_object)
            return False

        identifier = harvest_object.guid
        try:
            record = self.csw.getrecordbyid([identifier])
        except Exception, e:
            self._save_object_error('Error getting the CSW record with GUID %s' % identifier, harvest_object)
            return False

        if record is None:
            self._save_object_error('Empty record for GUID %s' % identifier,
                                    harvest_object)
            return False

        try:
            # Save the fetch contents in the HarvestObject
            harvest_object.content = record['xml']
            harvest_object.save()
        except Exception,e:
            self._save_object_error('Error saving the harvest object for GUID %s [%r]' % \
                                    (identifier, e), harvest_object)
            return False

        log.debug('XML content saved (len %s)', len(record['xml']))
        return True

    def _setup_csw_client(self, url):
        self.csw = CswService(url)


class GeminiDocHarvester(GeminiHarvester, SingletonPlugin):
    '''
    A Harvester for individual GEMINI documents
    '''

    implements(IHarvester)

    def info(self):
        return {
            'name': 'gemini-single',
            'title': 'Single GEMINI 2 document',
            'description': 'A single GEMINI 2.1 document'
            }

    def gather_stage(self,harvest_job):
        log = logging.getLogger(__name__ + '.individual.gather')
        log.debug('GeminiDocHarvester gather_stage for job: %r', harvest_job)

        self.harvest_job = harvest_job

        # Get source URL
        url = harvest_job.source.url

        # Get contents
        try:
            content = self._get_content(url)
        except GetContentError, e:
            self._save_gather_error('Unable to get document: %r' % e,
                                    harvest_job)
            return None
        except Exception, e:
            self._save_gather_error('Unable to get document from URL: %s: %r' % \
                                    (url, e), harvest_job)
            return None
        try:
            # We need to extract the guid to pass it to the next stage
            gemini_string, gemini_guid = self.get_gemini_string_and_guid(content,url)

            if gemini_guid:
                # Create a new HarvestObject for this identifier.
                # Generally the content will be set in the fetch stage, but as we already
                # have it, we might as well save a request.
                # NB The content is ascii and probably encoded, but is saved to a unicode
                # database field.
                obj = HarvestObject(guid=gemini_guid,
                                    job=harvest_job,
                                    content=gemini_string,
                                    harvest_source_reference=gemini_string)
                obj.save()

                log.info('Got GUID %s' % gemini_guid)
                return [obj.id]
            else:
                self._save_gather_error('Could not get the GUID for source %s' % url, harvest_job)
                return None
        except Exception, e:
            self._save_gather_error('Error parsing the document. Is this a valid Gemini document?: %s [%r]'% (url,e),harvest_job)
            if debug_exception_mode:
                raise
            return None


    def fetch_stage(self,harvest_object):
        # The fetching was already done in the previous stage
        return True


class GeminiWafHarvester(GeminiHarvester, SingletonPlugin):
    '''
    A Harvester from a WAF server containing GEMINI documents.
    e.g. Apache serving a directory of GEMINI files.
    '''

    implements(IHarvester)

    def info(self):
        return {
            'name': 'gemini-waf',
            'title': 'Web Accessible Folder (WAF) - GEMINI',
            'description': 'A Web Accessible Folder (WAF) displaying a list of GEMINI 2.1 documents'
            }

    def gather_stage(self,harvest_job):
        log = logging.getLogger(__name__ + '.WAF.gather')
        #log = HarvestGatherLogger(harvest_job)
        log.debug('GeminiWafHarvester gather_stage for job: %r', harvest_job)

        self.harvest_job = harvest_job

        # Get source URL
        Session.refresh(harvest_job.source) # in case the url has recently changed
        url = harvest_job.source.url
        log.debug('WAF URL: %r', url)

        # Get contents
        try:
            content = self._get_content(url)
        except GetContentError, e:
            self._save_gather_error('Unable to get WAF content: %r' % e,
                                    harvest_job)
            return None
        except Exception,e:
            self._save_gather_error('Unable to get WAF content at URL: %s: %r' % \
                                        (url, e),harvest_job)
            return None

        ids = []
        try:
            for url in self._extract_urls(content, url, log):
                try:
                    content = self._get_content(url)
                except GetContentError, e:
                    self._save_gather_error('Unable to get WAF link: %r' % e,
                                            harvest_job)
                    return None
                except Exception, e:
                    msg = 'Couldn\'t harvest WAF link: %s: %s' % (url, e)
                    self._save_gather_error(msg,harvest_job)
                    continue
                else:
                    # We need to extract the guid to pass it to the next stage
                    try:
                        gemini_string, gemini_guid = self.get_gemini_string_and_guid(content,url)
                        if gemini_guid:
                            log.debug('Got GUID %s' % gemini_guid)
                            # Create a new HarvestObject for this identifier
                            # Generally the content will be set in the fetch stage, but as we alredy
                            # have it, we might as well save a request
                            obj = HarvestObject(guid=gemini_guid,
                                                job=harvest_job,
                                                content=gemini_string,
                                                harvest_source_reference=url)
                            # NB: Gemini uses WAF URL for the
                            # harvest_source_reference whereas INSPIRE
                            # specifies the Unique Resource Identifier
                            obj.save()

                            ids.append(obj.id)


                    except Exception, e:
                        msg = 'Could not get GUID for source %s: %r' % (url,e)
                        self._save_gather_error(msg,harvest_job)
                        continue
        except Exception,e:
            msg = 'Error extracting URLs from %s' % url
            self._save_gather_error(msg,harvest_job)
            return None


        if len(ids) > 0:
            return ids
        else:
            self._save_gather_error('Couldn\'t find any links to metadata files. (N.B. A common error is for the WAF to contain links that containing path information. Links are discarded if they contain slashes. For example, it should be href="rivers.xml" rather than href="/metadata/rivers.xml".)',
                                     harvest_job)
            return None

    def fetch_stage(self,harvest_object):
        # The fetching was already done in the previous stage
        return True

    @classmethod
    def _extract_urls(cls, content, base_url, log):
        '''
        Get the URLs out of a WAF index page
        '''
        try:
            parser = etree.HTMLParser()
            tree = etree.fromstring(content, parser=parser)
        except Exception, inst:
            msg = 'Couldn\'t parse content into a tree: %s: %s' \
                  % (inst, content)
            raise Exception(msg)
        urls = []
        for url in tree.xpath('//a/@href'):
            url = url.strip()
            if not url:
                continue
            if '?' in url:
                log.debug('Ignoring link in WAF because it has "?": %s', url)
                continue
            if '/' in url:
                log.debug('Ignoring link in WAF because it has "/": %s', url)
                continue
            if '#' in url:
                log.debug('Ignoring link in WAF because it has "#": %s', url)
                continue
            if 'mailto:' in url:
                log.debug('Ignoring link in WAF because it has "mailto:": %s', url)
                continue
            log.debug('WAF contains file: %s', url)
            urls.append(url)
        base_url = base_url.rstrip('/').split('/')
        if 'index' in base_url[-1]:
            base_url.pop()
        base_url = '/'.join(base_url)
        base_url += '/'
        log.debug('WAF base URL: %s', base_url)
        return [base_url + i for i in urls]


