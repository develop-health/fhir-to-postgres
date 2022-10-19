import argparse
from pathlib import Path
import logging
import os

from lib.graph import Graph
from lib.file_writers import QueryBuilder, write_metadata
from lib.fhir_parser import add_nodes_from_pages

# Order of operations
# X Fetch all schemas from FHIR site, cache
# X Parse text
# X Assemble graph with nodes and edges
# X Create tables 
# X Fill in foriegn keys
# X Fill in relationships


def dir_path(string):
    if os.path.isdir(string):
        return string
    else:
        raise NotADirectoryError(string)

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-r', '--resources', metavar='resource',
        type=str, nargs='+',
        help='resources to include in generated SQL',
        default=[])
    parser.add_argument(
        '-s', '--sql-file',
        help="File to save SQL output"
    )
    parser.add_argument(
        '-D', '--debug',
        help="Print lots of debugging statements",
        action="store_const", dest="loglevel", const=logging.DEBUG,
        default=logging.WARNING,
    )
    return parser.parse_args()

def main():
    args = parse_args()
    logging.basicConfig(level=args.loglevel)
    
    graph = Graph()

    add_nodes_from_pages(graph)

    graph.build_graph()

    sql_file_path = Path(args.sql_file)
    query_builder = QueryBuilder(query_path=sql_file_path)
    query_builder.write_sql(graph, args.resources)


if __name__ == '__main__':
    main()