
from collections import defaultdict
import logging
from pathlib import Path
from typing import List
from sqlalchemy import MetaData, create_mock_engine
import yaml

from .graph import Graph, Node


class QueryBuilder:
    metadata_obj: MetaData
    _query_path: Path

    def __init__(self, query_path: Path):
        self._query_path = query_path
        self._engine = create_mock_engine('postgresql://', executor=self._dump)
        self.metadata_obj = MetaData()

    def _dump(self, sql):
        with open(self._query_path, 'a') as sql_file:
            # Writing data to a file
            sql_file.write(f'{sql.compile(dialect=self._engine.dialect)};\n')

    def write_sql(self, graph: Graph, filter_by_resources: List[str]):
        writable_nodes = graph.get_writable_nodes(filter_by_resources=filter_by_resources)
        for node in writable_nodes:
            logging.debug(f'writing sql for node {node.key}')
            node.build_table(self.metadata_obj)
        for edge in graph.edges.values():
            if edge.originating_node in writable_nodes and edge.destination in writable_nodes:
                logging.debug(f'writing sql for edge {edge.key}')
                edge.add_foreign_key(self.metadata_obj)

        # clear file
        open(self._query_path, 'w').close()
        self.metadata_obj.create_all(self._engine, checkfirst=False)

    
def build_base_schema(node: Node):
    return {
        'table': {
            'schema': 'public',
            'name': node.database_name,
        },
        # public user should be able to read from any table
        'select_permissions': [{
            'role': 'public',
            'permission': {
                'columns': ['_id', *[field.database_name for field in node.base_fields.values()]],
                'filter': {},
                'allow_aggregations': True
            }
        }]
    }

def write_metadata(path: Path, graph: Graph):
    metadatas = { node.database_name: defaultdict(list, build_base_schema(node)) for node in graph.concrete_nodes }
    for edge in graph.edges.values():
        logging.debug(f'writing metadata for edge {edge.key}')
        edge.add_metadata(metadatas)  # type: ignore

    for key, contents in metadatas.items():
        file_name = f'public_{key}.yaml'
        with open(path / file_name, 'w') as config_file:
            yaml.dump(dict(contents), config_file, sort_keys=False)

    with open(path / 'tables.yaml', 'w') as table_parent_file:
        yaml.dump(
            [f'!include public_{table}.yaml' for table in sorted(metadatas.keys())],
            table_parent_file,
            default_style='"')

