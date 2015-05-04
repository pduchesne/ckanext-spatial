# TODO: Move these to ckanext-geoviews

import mimetypes, urlparse, os
from logging import getLogger

from ckan import plugins as p
import ckan.lib.helpers as h


log = getLogger(__name__)


class DataViewBase(p.SingletonPlugin):
    '''This base class is for view extensions. '''
    if p.toolkit.check_ckan_version(min_version='2.3'):
        p.implements(p.IResourceView, inherit=True)
    else:
        p.implements(p.IResourcePreview, inherit=True)
    p.implements(p.IConfigurer, inherit=True)
    p.implements(p.IConfigurable, inherit=True)

    proxy_is_enabled = False
    same_domain = False

    def update_config(self, config):
        p.toolkit.add_public_directory(config, 'public')
        p.toolkit.add_template_directory(config, 'templates')
        p.toolkit.add_resource('public', 'ckanext-spatial')

        config['ckan.resource_proxy_enabled'] = p.plugin_loaded('resource_proxy')

    def configure(self, config):
        enabled = config.get('ckan.resource_proxy_enabled', False)
        self.proxy_is_enabled = enabled

    def setup_template_variables(self, context, data_dict):
        import ckanext.resourceproxy.plugin as proxy
        self.same_domain = data_dict['resource'].get('on_same_domain')
        if self.proxy_is_enabled and not self.same_domain:
            data_dict['resource']['original_url'] = data_dict['resource']['url']
            data_dict['resource']['url'] = proxy.get_proxified_resource_url(data_dict)



class WMSView(DataViewBase):
    WMS = ['wms']

    # IResourceView (CKAN >=2.3)
    def info(self):
        return {'name': 'wms_view',
                'title': 'WMS',
                'icon': 'map-marker',
                'iframed': True,
                'default_title': p.toolkit._('WMS'),
                }

    def can_view(self, data_dict):
        resource = data_dict['resource']
        format_lower = resource['format'].lower()

        if format_lower in self.WMS:
            return self.same_domain or self.proxy_is_enabled
        return False

    def view_template(self, context, data_dict):
        return 'dataviewer/wms.html'

    # IResourcePreview (CKAN < 2.3)

    def can_preview(self, data_dict):
        format_lower = data_dict['resource']['format'].lower()

        correct_format = format_lower in self.WMS
        can_preview_from_domain = self.proxy_is_enabled or data_dict['resource']['on_same_domain']
        quality = 2

        if p.toolkit.check_ckan_version('2.1'):
            if correct_format:
                if can_preview_from_domain:
                    return {'can_preview': True, 'quality': quality}
                else:
                    return {'can_preview': False,
                            'fixable': 'Enable resource_proxy',
                            'quality': quality}
            else:
                return {'can_preview': False, 'quality': quality}

        return correct_format and can_preview_from_domain

    def preview_template(self, context, data_dict):
        return 'dataviewer/wms.html'

    def setup_template_variables(self, context, data_dict):
        import ckanext.resourceproxy.plugin as proxy
        self.same_domain = data_dict['resource'].get('on_same_domain')
        if self.proxy_is_enabled and not self.same_domain:
            data_dict['resource']['proxy_url'] = proxy.get_proxified_resource_url(data_dict)

        else:
            data_dict['resource']['proxy_url'] = data_dict['resource']['url']


class WMSPreview(WMSView):
    pass


class GeoJSONView(DataViewBase):
    p.implements(p.ITemplateHelpers, inherit=True)

    GeoJSON = ['gjson', 'geojson']

    def update_config(self, config):
        ''' Set up the resource library, public directory and
        template directory for the preview
        '''

        mimetypes.add_type('application/json', '.geojson')

    # IResourceView (CKAN >=2.3)
    def info(self):
        return {'name': 'geojson_view',
                'title': 'GeoJSON',
                'icon': 'map-marker',
                'iframed': True,
                'default_title': p.toolkit._('GeoJSON'),
                }

    def can_view(self, data_dict):
        resource = data_dict['resource']
        format_lower = resource['format'].lower()

        if format_lower in self.GeoJSON:
            return self.same_domain or self.proxy_is_enabled
        return False

    def view_template(self, context, data_dict):
        return 'dataviewer/geojson.html'

    # IResourcePreview (CKAN < 2.3)

    def can_preview(self, data_dict):
        format_lower = data_dict['resource']['format'].lower()

        correct_format = format_lower in self.GeoJSON
        can_preview_from_domain = self.proxy_is_enabled or data_dict['resource']['on_same_domain']
        quality = 2

        if p.toolkit.check_ckan_version('2.1'):
            if correct_format:
                if can_preview_from_domain:
                    return {'can_preview': True, 'quality': quality}
                else:
                    return {'can_preview': False,
                            'fixable': 'Enable resource_proxy',
                            'quality': quality}
            else:
                return {'can_preview': False, 'quality': quality}

        return correct_format and can_preview_from_domain

    def preview_template(self, context, data_dict):
        return 'dataviewer/geojson.html'

    # ITemplateHelpers

    def get_helpers(self):
        from ckanext.spatial import helpers as spatial_helpers

        # CKAN does not allow to define two helpers with the same name
        # As this plugin can be loaded independently of the main spatial one
        # We define a different helper pointing to the same function
        return {
                'get_common_map_config_geojson' : spatial_helpers.get_common_map_config,
                }


def get_proxified_service_url(data_dict):
    '''
    :param data_dict: contains a resource and package dict
    :type data_dict: dictionary
    '''
    url = h.url_for(
        action='proxy_service',
        controller='ckanext.spatial.controllers.service_proxy:ServiceProxyController',
        id=data_dict['package']['name'],
        resource_id=data_dict['resource']['id'])
    log.info('Proxified url is {0}'.format(url))
    return url

class OpenlayersPreview(p.SingletonPlugin):

    p.implements(p.IConfigurer, inherit=True)
    p.implements(p.IResourcePreview, inherit=True)
    p.implements(p.IRoutes, inherit=True)

    FORMATS = ['kml','geojson','gml','wms','wfs','shp', 'esrigeojson', 'gft', 'arcgis_rest']

    def update_config(self, config):

        p.toolkit.add_public_directory(config, 'public')
        p.toolkit.add_template_directory(config, 'templates')
        p.toolkit.add_resource('public', 'ckanext-spatial')

        self.proxy_enabled = p.toolkit.asbool(config.get('ckan.resource_proxy_enabled', 'False'))

    def setup_template_variables(self, context, data_dict):
        import ckanext.resourceproxy.plugin as proxy

        p.toolkit.c.gapi_key = h.config.get('ckanext.spatial.gapi.key')

        if self.proxy_enabled and not data_dict['resource']['on_same_domain']:
            p.toolkit.c.resource['proxy_url'] = proxy.get_proxified_resource_url(data_dict)
            p.toolkit.c.resource['proxy_service_url'] = get_proxified_service_url(data_dict)
        else:
            p.toolkit.c.resource['proxy_url'] = data_dict['resource']['url']

    def can_preview(self, data_dict):
        format_lower = data_dict['resource']['format'].lower()

        #guess from file extension
        if not format_lower:
            #mimetype = mimetypes.guess_type(data_dict['resource']['url'])
            parsedUrl = urlparse.urlparse(data_dict['resource']['url'])
            format_lower = os.path.splitext(parsedUrl.path)[1][1:].encode('ascii','ignore').lower()

        correct_format = format_lower in self.FORMATS
        can_preview_from_domain = self.proxy_enabled or data_dict['resource']['on_same_domain']
        quality = 2

        if p.toolkit.check_ckan_version('2.1'):
            if correct_format:
                if can_preview_from_domain:
                    return {'can_preview': True, 'quality': quality}
                else:
                    return {'can_preview': False,
                            'fixable': 'Enable resource_proxy',
                            'quality': quality}
            else:
                return {'can_preview': False, 'quality': quality}

        return correct_format and can_preview_from_domain

    def preview_template(self, context, data_dict):
        return 'dataviewer/openlayers2.html'

    def before_map(self, m):
        m.connect('/dataset/{id}/resource/{resource_id}/service_proxy',
                  controller='ckanext.spatial.controllers.service_proxy:ServiceProxyController',
                  action='proxy_service')
        return m

class GeoJSONPreview(GeoJSONView):
    pass