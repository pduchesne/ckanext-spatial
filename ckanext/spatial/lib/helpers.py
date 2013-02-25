# template helpers

from pkg_resources import resource_stream
from lxml import etree

from ckan import model
from ckan.lib.base import json
from ckanext.harvest.model import HarvestObject, HarvestCoupledResource
from ckanext.spatial.lib.coupled_resource import extract_gemini_harvest_source_reference

def get_coupled_packages(pkg):
    res_type = pkg.extras.get('resource-type')
    if res_type in ('dataset', 'series'):
        coupled_resources = pkg.coupled_service
        coupled_packages = \
                  [(couple.service_record.name, couple.service_record.title) \
                   for couple in coupled_resources \
                   if couple.service_record_package_id and \
                   couple.service_record and \
                   couple.service_record.state == 'active']
        return coupled_packages
    
    elif res_type == 'service':
        # Find the dataset records which are pointed to in this service record
        coupled_resources = pkg.coupled_dataset
        coupled_packages = \
                  [(couple.dataset_record.name, couple.dataset_record.title) \
                   for couple in coupled_resources \
                   if couple.dataset_record_package_id and \
                   couple.dataset_record and \
                   couple.dataset_record.state == 'active']
        return coupled_packages

transformer = None
def transform_gemini_to_html(gemini_xml):
    from ckanext.spatial.model.harvested_metadata import GeminiDocument
    
    if True:#not transformer or True: #HACK
        with resource_stream("ckanext.spatial",
                             "templates/ckanext/spatial/gemini2-html-stylesheet.xsl") as style:
            style_xml = etree.parse(style)
            global transformer
            transformer = etree.XSLT(style_xml)
    xml = etree.fromstring(gemini_xml)
    html = transformer(xml)
    body = etree.tostring(html, pretty_print=True)

    gemini_doc = GeminiDocument(xml_tree=xml)
    publishers = gemini_doc.read_value('responsible-organisation')
    publisher = publishers[0].get('organisation-name', '') if publishers else ''
    header = {'title': gemini_doc.read_value('title'),
              'guid': gemini_doc.read_value('guid'),
              'publisher': publisher,
              'language': gemini_doc.read_value('metadata-language'),
              }
    return header, body
              
