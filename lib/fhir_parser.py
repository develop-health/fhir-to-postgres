from dataclasses import dataclass
from lxml import html
import logging
import pickle
from typing import List

import requests

from .graph import Graph, ParsedNode, Row


page_content_cache = {}

try:
  with open('page_cache', 'rb') as cache_file:
    page_content_cache = pickle.load(cache_file)
except FileNotFoundError:
  pass

@dataclass
class FHIRPage:
  path: str

  @property
  def _content(self):
    try:
      return page_content_cache[self.path]
    except KeyError:
      response = requests.get(f'https://hl7.org/fhir/{self.path}')
      response.raise_for_status()
      content = response.content
      page_content_cache[self.path] = content
      return content

  @property
  def tree(self):
    return html.fromstring(self._content)

@dataclass
class ResourceText:
  ttl: List[html.HtmlElement]

  @property
  def _rows(self) -> List[Row]:
    # errant carriage return within references eg https://www.hl7.org/fhir/metadatatypes.html#usagecontext
    ttl = [text_element if text_element != '|\r\n  ' else '|' for text_element in self.ttl]
    contents = ''.join(ttl)  # type: ignore
    return [Row(line) for line in contents.split('\r\n')]
  
  @property
  def nodes(self) -> List[ParsedNode]:
    base_node = ParsedNode()
    nodes: List[ParsedNode] = [ base_node ]
    node_tiers: List[ParsedNode] =  [ base_node ]
    for row in self._rows:
      logging.debug(f'resource row {row}')
      if row.ends_subnode:
        node_tiers.pop()
        # no additional information on this line
        continue

      if not row:
        continue 

      node_tiers[-1].append_row(row)

      if row.starts_subnode:
        node = ParsedNode()
        nodes.append(node)
        node_tiers.append(node)

    return nodes

    
def add_nodes_from_pages(graph: Graph):
  multiresource_paths = {
    'datatypes.html',
    'resource.html',
    'metadatatypes.html',
    'references.html',
    'extensibility.html'
  }

  for path in multiresource_paths:
    page = FHIRPage(path=path)
    ttls_elements = page.tree.xpath('//div[@id="ttl"]')
    for element in ttls_elements:
      ttl = element.xpath('div/pre//text()')
      resource = ResourceText(ttl=ttl)
      for node in resource.nodes:
        graph.add_node(node)

  resources_page = FHIRPage(path='resourcelist.html')
  resource_paths = set(resources_page.tree.xpath('//div[@id="tabs-1"]//li/a[1]//@href'))

  meta_paths = {
    'capabilitystatement.html',
    'structuredefinition.html',
    'implementationguide.html',
    'searchparameter .html',
    'messagedefinition.html',
    'operationdefinition.html',
    'compartmentdefinition.html',
    'structuremap.html',
    'graphdefinition.html',
    'examplescenario.html',
  }

  supplementary_paths = {
    'dosage.html',
    'narrative.html',
    'prodcharacteristic.html',
    'productshelflife.html',
    'marketingstatus.html',
    'backboneelement.html',
    'domainresource.html',
  }

  paths = resource_paths.difference(meta_paths).union(supplementary_paths)

  for path in paths:
    page = FHIRPage(path=path)
    ttl = page.tree.xpath('//div[@id="ttl"]/div/pre//text()')
    resource = ResourceText(ttl=ttl)
    for node in resource.nodes:
      graph.add_node(node)

  with open('page_cache', 'wb') as cache_file:
    pickle.dump(page_content_cache, cache_file)