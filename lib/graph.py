from abc import ABC, abstractmethod
from collections import defaultdict
from copy import copy
from dataclasses import dataclass
from enum import Enum
from functools import cached_property
import re
import logging
from typing import Dict, List, Optional, Set, Tuple, Union

from sqlalchemy import CheckConstraint, Column, Constraint, ForeignKey, Table, MetaData, UniqueConstraint
from sqlalchemy.sql.sqltypes import Enum as ColumnEnum, Date, DateTime, Time, Boolean, Integer, BigInteger, Numeric, String
from sqlalchemy.dialects.postgresql import UUID, JSONB
import inflect

from .utilities import key_to_database_name, to_snake_case

inflector = inflect.engine()

class Node(ABC):

    def __repr__(self):
        return f"{type(self)}('{self.key}')"

    def __str__(self):
        return self.key
    
    def __eq__(self, other: 'Node') -> bool:
        return self.key == other.key
    
    def __hash__(self) -> int:
        return hash(self.key)

    @property
    @abstractmethod
    def key(self) -> str:
        raise NotImplementedError

    @property
    def base_fields(self) -> Dict[str, 'BaseField']:
        return {}

    @property
    def constraints(self) -> List[CheckConstraint]:
        return []

    @property
    def database_name(self) -> str:
        return key_to_database_name(self.key)

    def build_table(self, metadata: MetaData):
        columns = []
        field_constraints: List[Constraint] = []
        for field in self.base_fields.values():
            column = Column(field.database_name, field.to_db_field(self), **field.column_kwargs)
            columns.append(column)
            if isinstance(field, PrimativeField) and field.unique:
                # Unique constraints must be named for Hasura https://github.com/hasura/graphql-engine/issues/3666#issuecomment-572419941
                constraint = UniqueConstraint(field.database_name, name=f'{self.database_name}_{field.database_name}_key')
                field_constraints.append(constraint)

        return Table(
            self.database_name, metadata,
            Column('_id', Integer, primary_key=True),
            *columns,
            *field_constraints,
            *self.constraints)
    
class Ordinality(Enum):
    ZERO = '0'
    ONE = '1'
    MANY = '*'

@dataclass
class Field(ABC):
    key: str
    ordinality: Tuple[Ordinality, Ordinality]
    originating_node: Union['ParsedNode', 'ElementNode']

    def __str__(self):
        return self.key
    
    def __hash__(self):
        return (self.originating_node.key, self.key,)
    
    def __eq__(self, other: 'Field'):
        return self.originating_node.key == other.originating_node.key and self.key == other.key

    @property
    def database_name(self) -> str:
        return key_to_database_name(self.key)
    
    @property
    def is_many_to_many(self) -> bool:
        return self.ordinality[1] == Ordinality.MANY
    
    @property
    def is_max_one(self) -> bool:
        return self.ordinality[1] == Ordinality.ONE
    

@dataclass
class BaseField(Field, ABC):

    @property
    def column_kwargs(self):
        return {}

    def to_db_field(self, node: Node):
        if self.ordinality[1] == Ordinality.MANY:
            return JSONB
        return self._db_base(node)

    @abstractmethod
    def _db_base(self, node: Node):
        raise NotImplementedError
    

class PrimativeType(Enum):
    DATE = 'date'
    DATE_TIME = 'dateTime'
    INSTANT = 'instant'
    TIME = 'time'
    BOOL = 'boolean'
    INT = 'integer'
    POSITIVE_INT = 'positiveInt'
    UNSIGNED_INT = 'unsignedInt'
    INT64 = 'integer64'
    DECIMAL = 'decimal'
    STRING = 'string'
    URI = 'uri'
    BASE64_BINARY = 'base64Binary'
    URL = 'url'
    MARKDOWN = 'markdown'
    ID = 'id'
    OID = 'oid'
    UUID = 'uuid'

@dataclass
class PrimativeField(BaseField):
    type: PrimativeType

    unique: bool = False
    index: bool = False

    _primative_field_type_to_db_field = {
        PrimativeType.DATE: Date,
        PrimativeType.DATE_TIME: DateTime(timezone=True),
        PrimativeType.INSTANT: DateTime(timezone=True),
        PrimativeType.TIME: Time,
        PrimativeType.BOOL: Boolean,
        PrimativeType.INT: Integer,
        PrimativeType.POSITIVE_INT: Integer,
        PrimativeType.UNSIGNED_INT: Integer,
        PrimativeType.INT64: BigInteger,
        PrimativeType.DECIMAL: Numeric,
        PrimativeType.STRING: String,
        PrimativeType.URI: String,
        PrimativeType.BASE64_BINARY: String,
        PrimativeType.URL: String,
        PrimativeType.MARKDOWN: String,
        PrimativeType.ID: String,
        PrimativeType.OID: String,
        PrimativeType.UUID: UUID,
    }

    @property
    def column_kwargs(self):
        return {
            'index': self.index,
        }

    def _db_base(self, node: Node):
        return self._primative_field_type_to_db_field[self.type]


@dataclass
class CodeField(BaseField):
    type = 'code'
    options: Set[str]

    def _db_base(self, node: Node):
        if not self.options:
            return String
        enum_string = ' '.join(self.options)
        enum_name = key_to_database_name(f'{node.database_name}_{self.database_name}_code')
        enum = Enum(enum_name, enum_string)
        return ColumnEnum(enum)

@dataclass
class XHTMLField(BaseField):
    type = PrimativeType.STRING

    def _db_base(self, node: Node):
        return String

@dataclass
class RelationshipField(Field, ABC):
    @property
    @abstractmethod
    def is_polymorphic(self) -> bool:
        raise NotImplementedError


@dataclass
class ReferenceRelationshipField(RelationshipField):
    foreign_node_keys: List[str]

    @property
    def is_polymorphic(self) -> bool:
        return len(self.foreign_node_keys) > 1


@dataclass
class ExclusiveRelationshipField(RelationshipField):
    foreign_node_key: str

    @property
    def is_polymorphic(self) -> bool:
        return False


@dataclass
class Edge:
    originating_node: Node
    originating_field: Optional[RelationshipField]
    destination: Node
    self_referencing_back_reference: bool = False
    shares_references: bool = False

    def __repr__(self):
        return f"Edge{self.key}"
    
    @property
    def is_back_reference(self):
        if isinstance(self.originating_field, ReferenceRelationshipField):
            return self.destination.key in self.originating_field.foreign_node_keys
        
        if isinstance(self.originating_field, ExclusiveRelationshipField):
            return self.destination.key == self.originating_field.foreign_node_key
        
        # for 'any' case
        if self.originating_field is None:
            return False
        
        raise ValueError

    @property
    def key(self) -> Tuple[str, str, str]:
        return (self.originating_node.key, self.originating_field.key if self.originating_field else '', self.destination.key,)
    
    @property
    def _connects_derived_node(self):
        return isinstance(self.originating_node, DerivedNode) or isinstance(self.destination, DerivedNode)

    @property
    def _base_column_name(self) -> str:
        # for "any" case
        if self.originating_field is None:
            return self.destination.key

        if self.shares_references:
            return f'{self.destination.key}.{self.originating_field.key}'

        if self.self_referencing_back_reference:
            return f'back{self.destination.key}'
        
        if self.originating_field.ordinality[1] == Ordinality.MANY:
            return self.destination.key

        if self.originating_field.is_polymorphic:
            if isinstance(self.originating_node, ParsedNode):
                return self.originating_field.key
            return self.destination.key

        if self.originating_field.ordinality[1] == Ordinality.ONE:
            return self.originating_field.key

        raise ValueError

    @property
    def _object_relationship_name(self) -> str:
        # field name equals destination and origin table name
        # [extension] extension <- [extension]
        if self.originating_field is not None and self.originating_field.database_name == self.destination.database_name == self.originating_node.database_name:
            return f'back{self.destination.key}'

        return self._base_column_name

    @property
    def _array_relationship_name(self) -> str:
        if self.originating_field is None:
            return 'anys'

        if self.originating_field.ordinality[1] == Ordinality.MANY:
            if self.originating_field.originating_node == self.destination:
                return self.originating_field.key
            return inflector.plural(self.originating_node.key)

        if self.originating_field.is_polymorphic:
            return inflector.plural(self.originating_node.key)

        if self.originating_field.ordinality[1] == Ordinality.ONE:
            return f'{self.originating_node.key}.{self.originating_field.key}'

        raise ValueError

    @property
    def column_name(self):
        return key_to_database_name(f'{self._base_column_name}_id')

    def add_foreign_key(self, metadata: MetaData):
        table: Table = metadata.tables[self.originating_node.database_name]
        table.append_column(
            Column(self.column_name, ForeignKey(f'{self.destination.database_name}._id')))
    
    def add_metadata(self, metadatas: dict[str, defaultdict[str, list | dict[str, str]]]):
        object_relationship_name = to_snake_case(self._object_relationship_name)
        array_relationship_name = to_snake_case(self._array_relationship_name)
        metadatas[self.originating_node.database_name]['object_relationships'].append({  # type: ignore
            'name': object_relationship_name,
            'using': {
                'foreign_key_constraint_on': self.column_name
            }
        })

        metadatas[self.originating_node.database_name]['select_permissions'][0]['permission']['columns'].append(self.column_name)  # type: ignore

        metadatas[self.destination.database_name]['array_relationships'].append({  # type: ignore
            'name': array_relationship_name,
            'using': {
                'foreign_key_constraint_on': {
                    'column': self.column_name,
                    'table': {
                        'schema': 'public',
                        'name': self.originating_node.database_name
                    }
                }
            }
        })


@dataclass
class Row:
    line: str
    
    @cached_property
    def _fhir_type(self):
        matched = re.search(r'\[\s(.+?)\s\]', self.line)
        if matched is None:
            raise ValueError
        fhir_type = matched.group(1)
        overrides = {
            'Duration': 'Quantity',
            'Distance': 'Quantity',
            'Age': 'Quantity',
            'Count': 'Quantity',
            'Quantity(SimpleQuantity)': 'Quantity'
        }
        if fhir_type in overrides:
            fhir_type = overrides[fhir_type]
        return fhir_type
    
    def _extract_capture_group(self, pattern: str) -> str:
        match = re.search(pattern, self.line)
        if not match:
            raise ValueError
        return match.group(1)

    def __bool__(self):
        return self.includes_key or self.includes_parent_element

    @property
    def includes_key(self) -> bool:
        try:
            self.key
            return True
        except ValueError:
            return False

    @property
    def key(self) -> str:
        return self._extract_capture_group(r'fhir:([A-Z][A-Za-z\.]+)\.')
    
    @property
    def _field_key(self):
        key = self._extract_capture_group(r'\.([a-z][a-zA-Z0-9]+)\s')
        # https://www.hl7.org/fhir/dosage.html ttl and spec do not align
        if self.key == 'Dosage.doseAndRate' and 'Simple' in key:
            key = key.replace('Simple', '')
        return key

    @property
    def includes_parent_element(self) -> bool:
        try:
            self.parent_element
            return True
        except ValueError:
            return False

    @property
    def parent_element(self) -> str:
        return self._extract_capture_group(r'#\sfrom\s([A-Z][a-zA-Z0-9]+):')

    @property
    def parent_element_fields(self) -> List[str]:
        elements = re.findall(r'(?:\.([a-zA-Z0-9]+)(?:,\s(?:and\s)?)*)', self.line)
        # contained field is ill advised https://www.hl7.org/fhir/domainresource-definitions.html#DomainResource.contained
        elements = [element for element in elements if element != 'contained']
        return elements

    @property
    def _options(self) -> Set[str]:
        try:
            option_text = self._extract_capture_group(r'((?:\s\S+\s\|)+\s\S+)')
        except ValueError:
            return set()
        return set(option_text.strip().split(' | '))

    @property
    def _ordinality(self) -> Tuple[Ordinality, Ordinality]:
        try:
            ordinality_string = self._extract_capture_group(r'#\s([0-9]\.\.[0-9*])')
            string_values = ordinality_string.split('..')
            return (Ordinality(string_values[0]), Ordinality(string_values[1]),)
        except ValueError:
            return (Ordinality.ZERO, Ordinality.ONE,)

    @property
    def _reference_types(self) -> List[str]:
        return self._extract_capture_group(r'Reference\(([a-zA-Z|]+)\)').split('|')

    @property
    def starts_subnode(self) -> bool:
        if '[' in self.line:
            if not ']' in self.line:
                return True
            return self.line.count('[') > self.line.count(']')
        return False
    
    @property
    def ends_subnode(self) -> bool:
        return self.line.strip().startswith(']')
    
    def as_field(self, node: 'ParsedNode') -> Field:
        if self.starts_subnode:
            return ExclusiveRelationshipField(
                key=self._field_key,
                ordinality=self._ordinality,
                foreign_node_key=f'{self.key}.{self._field_key}',
                originating_node=node)
        
        try:
            enum_value = PrimativeType(self._fhir_type)
            return PrimativeField(
                key=self._field_key,
                type=enum_value,
                ordinality=self._ordinality,
                originating_node=node,
                index=self._field_key == 'id',
                unique=self._field_key == 'id')
        except ValueError:
            pass

        if self._fhir_type == 'code':
            return CodeField(
                key=self._field_key,
                options=self._options,
                ordinality=self._ordinality,
                originating_node=node)

        if self._fhir_type.startswith('canonical'):
            return PrimativeField(
                key=self._field_key,
                type=PrimativeType.STRING,
                ordinality=self._ordinality,
                originating_node=node)

        if 'Reference(' in self._fhir_type:
            return ReferenceRelationshipField(
                key=self._field_key,
                ordinality=self._ordinality,
                foreign_node_keys=self._reference_types,
                originating_node=node)

        if self._fhir_type.startswith('See'):
            matched = re.search(r'See\s(.+)', self._fhir_type)
            if matched is None:
                raise ValueError
            related_node_key = matched.group(1)
            return ExclusiveRelationshipField(
                key=self._field_key,
                ordinality=self._ordinality,
                foreign_node_key=related_node_key,
                originating_node=node)

        if self._fhir_type[0].isupper():
            return ExclusiveRelationshipField(
                key=self._field_key,
                ordinality=self._ordinality,
                foreign_node_key=self._fhir_type,
                originating_node=node)
        
        if 'xhtml' in self._fhir_type:
            return XHTMLField(
                key=self._field_key,
                ordinality=self._ordinality,
                originating_node=node)
        
        raise ValueError


class AnyNode(Node):
    key: str = 'Any'


# https://www.hl7.org/fhir/element.html does not have a ttl
class ElementNode(Node):
    key: str = 'Element'

    @property
    def fhir_fields(self) -> Dict[str, Field]:
        return {
            'id': PrimativeField(
                key='id',
                ordinality=(Ordinality.ZERO, Ordinality.ONE,),
                type=PrimativeType.ID,
                originating_node=self,
                index=True,
                unique=True),
            'extension': ExclusiveRelationshipField(
                key='extension',
                ordinality=(Ordinality.ZERO, Ordinality.MANY,),
                foreign_node_key='Extension',
                originating_node=self)
        }
    
    # copied form parsednode
    @property
    def base_fields(self) -> Dict[str, BaseField]:
        fields: Dict[str, BaseField] = {}
        for field in self.fhir_fields.values():
            if isinstance(field, BaseField):
                fields[field.key] = field
        return fields

    @cached_property
    def fhir_fields_by_lower_key(self):
        return { key.lower(): field for key, field in self.fhir_fields.items() }


class DerivedNode(Node):
    def __init__(self, key: str, derived_from: Node):
        self._key = key
        self.derived_from = derived_from

    @property
    def key(self):
        return self._key

    # TODO
    # @property
    # def constraints(self) -> List[CheckConstraint]:
    #     return [CheckConstraint(f'num_nonnulls({", ".join(self._foreign_node_keys)}) = 1')]


class ParsedNode(Node):
    _rows: List[Row]
    _inherited_fields: Dict[str, Field]

    def __init__(self):
        self._rows = []
        self._inherited_fields = {}

    def append_row(self, row: Row):
        self._rows.append(row)

    @cached_property
    def key(self):
        for row in self._rows:
            if row.includes_key:
                return row.key
        raise ValueError('Node must have key')
    
    @property
    def _is_child_element(self) -> bool:
        return '.' in self.key
    
    @cached_property
    def _inherited_field_keys(self) -> Dict[str, List[str]]:
        inherited_fields: Dict[str, List[str]] = {}
        for row in self._rows:
            if not row.includes_parent_element:
                continue
            parent_element = row.parent_element
            if parent_element:
                inherited_fields[parent_element] = row.parent_element_fields
        
        # children defined in a parent context inherit from BackboneElement
        if self._is_child_element:
            inherited_fields['BackboneElement'] = ['extension', 'modifierExtension']

        return inherited_fields

    def add_inherited_fields(self, graph: 'Graph'):
        for parent_resource, parent_fields in self._inherited_field_keys.items():
            parent = graph.get_node(parent_resource)
            if not isinstance(parent, ParsedNode) and not isinstance(parent, ElementNode):
                raise ValueError('Parents should only be parsed or element nodes')
            graph.add_meta_node(parent)
            if isinstance(parent, ParsedNode):
                parent.add_inherited_fields(graph=graph)
            for parent_field in parent_fields:
                inherited_field = parent.fhir_fields_by_lower_key[parent_field.lower()]
                inherited_field_copy = copy(inherited_field)
                inherited_field_copy.originating_node = self
                self._inherited_fields[inherited_field.key] = inherited_field_copy

    @cached_property
    def fhir_fields_by_lower_key(self) -> Dict[str, Field]:
        return { key.lower(): field for key, field in self.fhir_fields.items() }

    @cached_property
    def fhir_fields(self) -> Dict[str, Field]:
        fields: Dict[str, Field] = {}
        for field in self._inherited_fields.values():
            fields[field.key] = field
        for row in self._rows:
            if row.includes_key:
                field = row.as_field(self)
                fields[field.key] = field
        return fields

    @property
    def base_fields(self) -> Dict[str, BaseField]:
        fields: Dict[str, BaseField] = {}
        for field in self.fhir_fields.values():
            if isinstance(field, BaseField):
                fields[field.key] = field
        return fields

    @cached_property
    def relationship_fields(self) -> List['RelationshipField']:
        return [field for field in self.fhir_fields.values() if isinstance(field, RelationshipField)]
    

class Graph:
    _nodes: Dict[str, Node]
    _meta_nodes: Dict[str, Node]
    edges: Dict[Tuple[str, str, str], Edge]

    def __init__(self):
        self._nodes = {}
        self.edges = {}
        self._meta_nodes = {}
    
    def add_meta_node(self, node: Node):
        if node.key == 'Resource':
            return
        self._meta_nodes[node.key] = node

    def add_node(self, node: Node):
        if node.key in self._nodes:
            raise BaseException(f'Node {node.key} exists in graph')
        self._nodes[node.key] = node
    
    def get_node(self, key: str, lower: bool=False):
        if lower:
            return self.nodes_by_lower_key[key]
        return self._nodes[key]
    
    @cached_property
    def nodes_by_lower_key(self):
        return { key.lower(): node for key, node in self._nodes.items() }

    def _add_edge(self, edge: Edge):
        self.edges[edge.key] = edge

    @property
    def _parsed_nodes(self) -> List[ParsedNode]:
        return [node for node in self._nodes.values() if isinstance(node, ParsedNode)]
    
    @property
    def _concrete_parsed_nodes(self) -> List[ParsedNode]:
        return [node for node in self._nodes.values() if isinstance(node, ParsedNode) and node.key not in self._meta_nodes]

    @property
    def concrete_nodes(self) -> List[Node]:
        return [node for node in self._nodes.values() if node.key not in self._meta_nodes]

    @property
    def derived_nodes(self) -> List[DerivedNode]:
        return [node for node in self._nodes.values() if isinstance(node, DerivedNode)]

    def _get_subnodes(self, node: Node) -> Set[Node]:
        if not isinstance(node, ParsedNode):
            return set()
        
        if node in self._writable_nodes:
            return set()
        
        logging.debug(f'Getting subnodes for {node.key}')

        self._writable_nodes.add(node)

        related_fields = node.relationship_fields

        nodes: Set[Node] = set()

        for field in related_fields:
            if isinstance(field, ExclusiveRelationshipField):
                # we only want backbone elements, not reference relationships
                if '.' not in field.foreign_node_key:
                    continue
                subnode = self._nodes[field.foreign_node_key]
                nodes = nodes.union(self._get_subnodes(subnode))
                nodes.add(subnode)
        
        return nodes

        
    def get_writable_nodes(self, filter_by_resources: List[str]) -> List[Node]:
        if not filter_by_resources:
            return self.concrete_nodes
        
        nodes: Set[Node] = set()
        self._writable_nodes: Set[Node] = set()

        for resource in filter_by_resources:
            node = self._nodes[resource]
            subnodes = self._get_subnodes(node)
            nodes = nodes.union(subnodes)
            nodes.add(node)
        
        # add derived nodes that connect two related resources in the filtered set
        for derived_node in self.derived_nodes:
            if derived_node in nodes or derived_node.derived_from.key not in filter_by_resources:
                continue
            
            for edge in self.edges.values():
                if edge.originating_node.key == derived_node.key and edge.destination.key in filter_by_resources:
                    nodes.add(derived_node)

                if edge.destination.key == derived_node.key and edge.originating_node.key in filter_by_resources:
                    nodes.add(derived_node)
                
            
        return list(nodes)

    
    def build_graph(self):
        any_node = AnyNode()
        self._nodes[any_node.key] = any_node

        element_node = ElementNode()
        self._nodes[element_node.key] = element_node

        for node in self._parsed_nodes:
            node.add_inherited_fields(graph=self)

        for node in self._concrete_parsed_nodes:
            edge = Edge(
                originating_node=any_node,
                originating_field=None,
                destination=node)
            self._add_edge(edge)

        # add edges
        for node in self._concrete_parsed_nodes:
            for field in node.fhir_fields.values():
                if not isinstance(field, RelationshipField):
                    continue

                if isinstance(field, ExclusiveRelationshipField):
                    if field.is_max_one:
                        destination_key = field.foreign_node_key
                        edge = Edge(
                            originating_node=node,
                            originating_field=field,
                            destination=self._nodes[destination_key])
                        self._add_edge(edge)
                        continue
                    originating_key = field.foreign_node_key
                    edge = Edge(
                        originating_node=self._nodes[originating_key],
                        originating_field=field,
                        destination=node)
                    self._add_edge(edge)
                    continue

                if not isinstance(field, ReferenceRelationshipField):
                    raise ValueError

                if field.is_max_one and not field.is_polymorphic:
                    destination_key = field.foreign_node_keys[0]
                    edge = Edge(
                        originating_node=node,
                        originating_field=field,
                        destination=self._nodes[destination_key])
                    self._add_edge(edge)
                    continue

                # many to many but not poly
                if field.is_many_to_many and not field.is_polymorphic:
                    destination_key = field.foreign_node_keys[0]
                    edge = Edge(
                        originating_node=self._nodes[destination_key],
                        originating_field=field,
                        destination=node)
                    self._add_edge(edge)
                    continue

                # polymorphic
                # many to many
                derived_node_key = f'{node.key}.{field.key}'
                derived_node = DerivedNode(key=derived_node_key, derived_from=node)
                self._nodes[derived_node.key] = derived_node

                for destination in field.foreign_node_keys:
                    edge = Edge(
                        originating_node=derived_node,
                        originating_field=field,
                        destination=self._nodes[destination])
                    self._add_edge(edge)

                # edge between derived and source
                if field.is_max_one:
                    edge = Edge(
                        originating_node=node,
                        originating_field=field,
                        destination=derived_node)
                    self._add_edge(edge)
                else:
                    edge = Edge(
                        originating_node=derived_node,
                        originating_field=field,
                        destination=node)
                    self._add_edge(edge)
                
        # find self referencing back references
        # cases where the same table is referenced in a derived node
        # only happens for two tables
        # [claim] <- claim_id [claim_related] claimd_id -> [claim]
        self_referencing_triplets: Dict[Tuple[str, str, str], Tuple[str, str, str]] = {}
        for key, edge in self.edges.items():
            (originating_node_key, relation_key, destination_node_key,) = key
            triplet_key = (originating_node_key, edge.column_name, destination_node_key,)
            if triplet_key in self_referencing_triplets:
                other_edge = self.edges[self_referencing_triplets[triplet_key]]
                if edge.is_back_reference and not other_edge.is_back_reference:
                    edge.self_referencing_back_reference = True
                    continue
                if other_edge.is_back_reference and not edge.is_back_reference:
                    other_edge.self_referencing_back_reference = True
                    continue
            self_referencing_triplets[triplet_key] = key

        # find cases where multiple backreferences occur eg Codeable Concept
        # [care_plan] based_on <- [care_plan]
        # [care_plan] part_of <- [care_plan]
        source_destination_pairs: Dict[Tuple[str, str], Tuple[str, str, str]] = {}
        for key, edge in self.edges.items():
            if (edge.originating_field is not None and not edge.originating_field.is_many_to_many) or edge.self_referencing_back_reference:
                continue
            (originating_node_key, relation_key, destination_node_key,) = key
            pair_key = (originating_node_key, destination_node_key,)
            if pair_key in source_destination_pairs:
                edge.shares_references = True
                shared_edge = source_destination_pairs[pair_key]
                self.edges[shared_edge].shares_references = True
                continue
            source_destination_pairs[pair_key] = key

        logging.debug('Graph build complete')

