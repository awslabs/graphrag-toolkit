import boto3
import os
from urllib.parse import urlparse
import json
import logging

from .graphstore import GraphStore

logger = logging.getLogger(__name__)

class NeptuneAnalyticsGraphStore(GraphStore):
    """
    GraphStore for interacting with NeptuneAnalytics Graph
    """

    def __init__(self, graph_identifier, region=None):
        """

        Create a NeptuneAnalytics backed GraphStore. The GraphStore is a wrapper for interacting with the
        Neptune Analytics graph object with that graph_id that corresponds to graph_identifier.

        :param graph_identifier: Str. An existing graph identifier (required)
        :param region: Str AWS region
        """


        if region is None:
            self.__detect_region()
        else:
            self.region = region
        assert self.region is not None, "region needs to be passed in or inferrable from current environment"
        self.session = boto3.Session(region_name=self.region)
        self.neptune_client = self.session.client('neptune-graph', region_name=self.region)
        self.s3_client = self.session.client('s3', region_name=self.region)

        self.neptune_graph_id = self.__attach_existing_neptune_graph(graph_identifier)
        self.node_type_to_property_mapping = {}

    def __detect_region(self):
        easy_checks = [
            os.environ.get('AWS_REGION'),
            os.environ.get('AWS_DEFAULT_REGION'),
            boto3.DEFAULT_SESSION.region_name if boto3.DEFAULT_SESSION else None,
            boto3.Session().region_name,
        ]
        for region in easy_checks:
            if region:
                self.region = region
                return

    def __attach_existing_neptune_graph(self, neptune_graph_id):
        assert neptune_graph_id is not None, "graph_identifier is required"
        response = self.neptune_client.get_graph(graphIdentifier=neptune_graph_id)
        return response['id']

    def read_from_csv(self, csv_file=None, s3_path=None, format='CSV'):
        """
        Upload a CSV file or folder of csv files to s3 and uses Neptune Analytics bulk import to read into the graph.

        Args:
            csv_file (str): Path to the CSV file
            s3_path (str): Path to the S3 path to save the file(s) to
            format(str): Str data format for import into s3. Default is 'CSV' or gremlin. Valid formats are 'NTRIPLES' for an RDF KG or 'CSV' or 'OPEN_CYPHER' for
        property graphs

        Returns:
            dict: The constructed knowledge graph
        """

        assert format in ['NTRIPLES', 'CSV', 'OPEN_CYPHER'], "format must be either 'NTRIPLES' or 'CSV' or 'OPEN_CYPHER'"

        if csv_file is not None:
            assert s3_path is not None, "s3 path should be passed with local csv path for data import"
            self.__upload_to_s3(s3_path, csv_file)

        logger.info(f'Loading data from source : {s3_path} into graph: {self.neptune_graph_id}')

        load_cypher = f'''CALL neptune.load(
                    {{
                        source: '{s3_path}',
                        region: '{self.region}',
                        format: '{format}',
                        failOnError: false,
                        concurrency: 16
                    }}
                )'''
        response = self.execute_query(load_cypher)
        logger.info(response)

    def __upload_to_s3(self, s3_path, local_path):
        path = urlparse(s3_path, allow_fragments=False)
        bucket = path.netloc
        file_path = path.path.lstrip('/').rstrip('/')
        print("Uploading {} to {}".format(local_path, s3_path))
        if os.path.isdir(local_path):
             for root, dirs, files in os.walk(local_path):
                for file in files:
                    self.s3_client.upload_file(os.path.join(local_path, file), bucket, f'{file_path}/{file}')
        else:
            self.s3_client.upload_file(local_path, bucket, f'{file_path}/{os.path.basename(local_path)}')

    def assign_text_repr_prop_for_nodes(self, node_label_to_property_mapping=None, **kwargs):
        """
        Assign a text representation property for each node label or node type

        Args:
            node_label_to_property_mapping (dict): A str to str mapping from each node label to the property name to use as it's text representation
            kwargs: keyword arguments where each key is a node label and each value is a property name
        """
        if node_label_to_property_mapping:
            self.node_type_to_property_mapping.update(node_label_to_property_mapping)
        self.node_type_to_property_mapping.update(kwargs)

    def node_label_has_text_repr_prop(self, node_label):
        """ check whether a node label has text representation property"""
        return node_label in self.node_type_to_property_mapping and self.node_type_to_property_mapping[node_label] is not None

    def get_text_repr_prop(self, node_label):
        """ get text representation property for a node label"""
        return self.node_type_to_property_mapping[node_label]

    def execute_query(self, cypher, parameters={}):
        logger.info("GraphQuery::", cypher)
        response =  self.neptune_client.execute_query(
            graphIdentifier=self.neptune_graph_id,
            queryString=cypher,
            parameters=parameters,
            language='OPEN_CYPHER'
        )
        return json.loads(response['payload'].read())['results']

    def get_schema(self):
        """
        Return the property graph schema

        :return:
        """
        schema_cypher = '''CALL neptune.graph.pg_schema() 
                            YIELD schema
                            RETURN schema
                        '''
        response = self.execute_query(schema_cypher)
        logger.info(response)
        return response

    def nodes(self, node_type=None):
        """
        Return a list of all node_ids or of all node types in the graph.

        :return: List[str] all node_ids in the graph
        """
        if node_type is None:
            query = '''
            MATCH (n)
            '''
        else:
            query = f'''
                    MATCH (n:{node_type})
                    '''
        if self.node_type_to_property_mapping:
            query += '''
                    RETURN properties(n) as properties, ID(n) as node, labels(n) as node_labels
                    '''
        else:
            query += '''
                    RETURN ID(n) AS node
                    '''
        response = self.execute_query(query)

        return [item["properties"][self.get_text_repr_prop(item["node_labels"][0])] if "properties" in item
                and self.node_label_has_text_repr_prop(item["node_labels"][0])
                and self.get_text_repr_prop(item["node_labels"][0]) in item["properties"] else item["node"] for item in response]

    def get_nodes(self, node_ids):
        """
        Return node details for given node ids

        :param node_ids: List[str] node ids
        :return: Dict[node_id:Str, Any] node details
        """
        if self.node_type_to_property_mapping:
            sub_query = " OR ".join([f"n.{prop} in $node_ids" for prop in set(self.node_type_to_property_mapping.values())])
            query = f'''MATCH (n)
                    WHERE {sub_query}
                    OR ID(n) IN $node_ids
                    RETURN properties(n) as properties, ID(n) as node
                    '''
        else:
            query = '''MATCH (n)
            WHERE ID(n) IN $node_ids
            RETURN properties(n) as properties, ID(n) as node
            '''

        response = self.execute_query(query, parameters={'node_ids': node_ids})
        return {item["node"]: item["properties"] for item in response}

    def edges(self):
        """
        Return a list of all edge_ids in the graph.

        :return: List[str] all edge_ids in the graph
        """
        query = '''
        MATCH ()-[e]-()
        RETURN ID(e) as edge
        '''
        response = self.execute_query(query)
        return [item["edge"] for item in response]

    def get_edges(self, edge_ids):
        """
        Return edge details for given edge ids

        :param edge_ids: List[str] edge ids
        :return: Dict[edge_id:Str, Any] edge details
        """

        query = '''
        MATCH ()-[e]-()
        WHERE ID(e) IN $edge_ids
        RETURN properties(e) as properties, ID(e) as edge
        '''
        response = self.execute_query(query, parameters={'edge_ids': edge_ids})
        return {item["edge"]: item["properties"] for item in response}

    def get_one_hop_edges(self, source_node_ids, return_triplets=True):
        """
        Return one hop edges given a set of source node ids.

        :param source_node_ids: List[Str] the node ids to start traversal form
        :param return_triplets: whether to return edge_ids only to return triplets in the form (src_node_id, edge, dst_node_id)
        :return: Dict[node_id:Str, Dict[edge_type:Str, edge_ids:List[Str]]] or Dict[node_id:Str, Dict[edge_type:Str, List[(node_id:Str, edge_type:Str, node_id:Str)]]]
        """
        if self.node_type_to_property_mapping:
            sub_query = " OR ".join([f"n.{prop} in $node_ids" for prop in set(self.node_type_to_property_mapping.values())])
            query = f'''MATCH (n) -[e]->(m)
                    WHERE {sub_query}
                    OR ID(n) IN $node_ids
                    RETURN DISTINCT ID(n) as node, properties(n) as properties, labels(n) as node_labels,
                     ID(e) as edge, type(e) as edge_type, 
                     ID(m) as dst_node, properties(m) as dst_properties, labels(m) as dst_node_labels
                    '''
        else:
            query = '''
                MATCH (n) -[e]->(m)
                WHERE ID(n) IN $node_ids
                RETURN DISTINCT ID(n) as node, ID(e) as edge, type(e) as edge_type, ID(m) as dst_node
                '''

        response = self.execute_query(query, parameters={'node_ids': source_node_ids})
        expanded_edges = {}
        for item in response:
            source_node = item["properties"][self.get_text_repr_prop(item["node_labels"][0])] \
                if "properties" in item and self.node_label_has_text_repr_prop(item["node_labels"][0]) else item["node"]
            if source_node not in expanded_edges:
                expanded_edges[source_node] = {}
            edge_type = item['edge_type']
            if edge_type not in expanded_edges[source_node]:
                expanded_edges[source_node][edge_type] = set()

            dst_node_has_text_properties = "dst_properties" in item and self.node_label_has_text_repr_prop(item["dst_node_labels"][0])
            dst_node = item["dst_properties"][self.get_text_repr_prop(item["dst_node_labels"][0])] if dst_node_has_text_properties else item["dst_node"]

            if return_triplets:
                expanded_edges[source_node][edge_type].add((source_node, item["edge_type"], dst_node))
            else:
                expanded_edges[source_node][edge_type].add((item["edge"]))

        return expanded_edges

    def get_edge_destination_nodes(self, edge_ids):
        pass

    def get_linker_tasks(self):
        return [
            "entity-extraction",
            "path-extraction",
            "draft-answer-generation",
            "opencypher"
        ]