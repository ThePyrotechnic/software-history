import json
import logging
import time
from typing import Dict, List, Set, Tuple
from urllib.error import HTTPError

from neo4j import GraphDatabase
from SPARQLWrapper import SPARQLWrapper, JSON

logger = logging.getLogger(__name__)


class Tasks:
    def __init__(self, server: str, auth: Tuple[str, str]):
        logger.info("Connecting to database . . .")
        self.driver = GraphDatabase.driver(server, auth=auth)
        logger.info("Connection complete")

    def add_genre_to_videogames(self):
        pass

    def get_software_instances(self) -> List[Dict[str, str]]:
        """
        Query wikidata for all instances of the software class at any depth.
        :return: A list of dictionaries [{"software_uri": <software_uri>, ... }, ... ]
        """
        # Note: This query times out often. It may be possible to run this query in "rings"
        # i.e. "get all items related to software with exactly n depth"
        wikidata_software = _sparql_results(
            """SELECT DISTINCT ?item ?itemLabel ?type ?typeLabel WHERE {
                 ?item wdt:P31 ?type.
                 ?type (wdt:P279*) wd:Q7397.
                 SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
               }""")
        return [{'child_uri': software["item"]["value"],
                 'child_label':software["itemLabel"]["value"],
                 'parent_uri':software["type"]["value"],
                 'parent_label':software["typeLabel"]["value"]}
                for software in wikidata_software["results"]["bindings"]]

    def get_software_subclasses(self) -> List[Dict[str, str]]:
        """
        Query wikidata for all subclasses of the software class at any depth.
        :return: A list of dictionaries [{"sub_uri": <sub_uri>, "sub_label": <sub_label>, ... }, ... ]
        """
        wikidata_subclasses = _sparql_results(
            """SELECT DISTINCT ?class ?classLabel ?classParent ?classParentLabel WHERE {
                 ?class wdt:P279* wd:Q7397.
                 ?class wdt:P279 ?classParent .
                 ?classParent wdt:P279* wd:Q7397.
                 SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
               }""")
        return [{'child_uri': subclass["class"]["value"],
                 'child_label':subclass["classLabel"]["value"],
                 'parent_uri':subclass["classParent"]["value"],
                 'parent_label':subclass["classParentLabel"]["value"]}
                for subclass in wikidata_subclasses["results"]["bindings"]]

    def generate_batches(self, data: List, batch_size: int) -> List:
        """
        Yield list of items of length batch_size for all items in data.
        :param data: A list of dicts from SPARQL query return
        :param batch_size: The size of each yielded batch of data elements
        """
        for i in range(0, len(data), batch_size):
            yield data[i:i+batch_size]

    def merge_data(self, data: List[Dict[str, str]], label: str, relationship: str, batch_size=500):
        """
        Merge all data passed in the data argument into the neo4j database.
        :param data: List of dictionaries of all data to be merged
        :param label: Label of data (Software, Class, etc.)
        :param relationship: Type of relationship being added (INSTANCE, SUBCLASS, etc.)
        :param batch_size: Size of batches to send data to neo4j, default 500
        """
        with self.driver.session() as session:
            for i, batch in enumerate(self.generate_batches(data, batch_size)):
                logger.info(f"Merging {relationship} relationship for {str(len(batch))} new {label} entries "
                            f"({len(data) - (i * batch_size)} {label} entries remaining)")
                session.run(f"""UNWIND $batch AS data
                            MERGE(child: {label} {{uri: data.child_uri}})
                                ON CREATE SET child.label = data.child_label, child.created = datetime()
                            MERGE(parent: Class {{uri: data.parent_uri}})
                                ON CREATE SET parent.label = data.parent_label, parent.created = datetime()
                            MERGE(child)-[relation: {relationship}] -> (parent)
                                ON CREATE SET relation.created = datetime()
                            """, batch=batch)
                # Sync statement prevents lazy return of query response, blocks until completion
                session.sync()
            logger.info(f"Completed merge of {len(data)} {label} entries")

    def update_database(self):
        """
        Add software which does not currently exist in the database. Create necessary superclass relations to support
        new software nodes. Do not update existing nodes or relationships
        """
        with self.driver.session() as session:
            logger.info("Fetching current software instance URIs . . .")
            current_software = session.run(
                "MATCH (n:Software) RETURN n.uri AS uri, ID(n)").records()
            current_software = set(software["uri"]
                                   for software in current_software)
            logger.info("Complete")

            logger.info("Fetching current software subclass URIs . . .")
            current_subclasses = session.run(
                "MATCH (n:Class) RETURN n.uri AS uri, ID(n)").records()
            current_subclasses = set(subclass["uri"]
                                     for subclass in current_subclasses)
            logger.info("Complete")

        logger.info(
            "Fetching current list of WikiData software subclasses . . .")
        subclass_nodes = self.get_software_subclasses()
        subclass_nodes = [
            node for node in subclass_nodes if node["child_uri"] not in current_subclasses]
        logger.info("Complete")

        logger.info(
            "Fetching current list of WikiData software . . .")
        software_nodes = self.get_software_instances()
        software_nodes = [
            node for node in software_nodes if node["child_uri"] not in current_software]
        logger.info("Complete")

        self.merge_data(subclass_nodes, "Class", "SUBCLASS")
        self.merge_data(software_nodes, "Software", "INSTANCE")

    def add_new_software(self):
        """
        Add software which does not currently exist in the database. Create necessary superclass relations to support
        new software nodes. Do not update existing nodes or relationships
        """
        with self.driver.session() as session:
            logger.info("Fetching current software URIs . . .")
            current_software = session.run(
                "MATCH (n:Software) RETURN n.uri AS uri, ID(n)").records()
            current_software = set(software["uri"]
                                   for software in current_software)
            logger.info("Complete")

        logger.info("Fetching current list of WikiData software . . .")
        # Note: This query times out often. It may be possible to run this query in "rings"
        # i.e. "get all items related to software with exactly n depth"
        wikidata_software = _sparql_results(
            """SELECT DISTINCT ?item ?itemLabel ?type ?typeLabel WHERE {?item wdt: P31 ?type.
                 ?type(wdt: P279*) wd: Q7397.
                 SERVICE wikibase: label {bd: serviceParam wikibase: language "en".}
               }
            """)
        logger.info("Complete")

        logger.info(
            "Fetching current list of WikiData software subclasses . . .")
        wikidata_subclasses = _sparql_results(
            """SELECT DISTINCT ?class ?classLabel ?classParent ?classParentLabel WHERE {?class wdt: P279 * wd: Q7397.
                 ?class wdt: P279 ?classParent .
                 ?classParent wdt: P279 * wd: Q7397.
                 SERVICE wikibase: label {bd: serviceParam wikibase: language "en".}
               }
            """)
        logger.info("Complete")

        visited = set()
        with self.driver.session() as session:
            # wikidata_software entries look like {key: {"value": value}}
            for software in wikidata_software["results"]["bindings"]:
                if software["item"]["value"] not in current_software:
                    logger.info(f"New: {software['item']['value']} ({software['itemLabel']['value']})")
                    _create_superclass_tree(session,
                                            {
                                                "uri": software["type"]["value"],
                                                "label": software["typeLabel"]["value"]
                                            },
                                            visited)

                    session.run("MATCH (super:Class {uri: $super_uri})"
                                "MERGE (s:Software {uri: $software_uri, label: $software_label})"
                                "   ON CREATE SET s.created = datetime()"
                                "CREATE (s)-[:INSTANCE {created: datetime()}]->(super)",
                                software_uri=software["item"]["value"], software_label=software["itemLabel"]["value"],
                                super_uri=software["type"]["value"], super_label=software["typeLabel"]["value"])


def get_superclasses(class_uri: str) -> List[Dict[str, str]]:
    """
    Return a list of direct superclasses for the given WikiData class URI
    : param class_uri: The class URI to get superclasses for
    : return: A list of dictionaries: [{"uri": < uri > , "label": < label}, ...]
    """
    query = """SELECT DISTINCT ?class ?classLabel WHERE {
      wd: % s wdt: P279 ?class.
      ?class wdt: P279 * wd: Q7397.
      SERVICE wikibase: label {
        bd: serviceParam wikibase: language "en".
        ?class rdfs: label ?classLabel
      }
    }""" % class_uri.split("/")[-1]

    while True:
        try:
            results = _sparql_results(query)
            break
        except HTTPError as e:
            if e.code == 429:
                print(e.reason)
                print("Sleeping . . .")
                time.sleep(10)
            else:
                raise e

    return [{"uri": r["class"]["value"],
             "label": r["classLabel"]["value"]}
            for r in results["results"]["bindings"]]


def add_parents(tx, class_: Dict, parents: List[Dict]):
    """
    Create subclass relationships between a subclass and its superclasses.
    Call this with a session.write_transaction() function
    : param tx: Passed automatically by session.write_transaction()
    : param class_: The subclass URI to draw the relations from
    : param parents: The superclass URIs to draw the relations to
    """
    # TODO: Make this an unrolled query
    for parent in parents:
        tx.run("MERGE (sub:Class {uri: $sub_uri})"
               # Don't re-set the label
               "    ON CREATE SET sub.label = $sub_label, sub.created = datetime()"
               # (TODO pending "update" timestamps)
               " MERGE (super:Class {uri: $super_uri})"
               "    ON CREATE SET super.label = $super_label, super.created = datetime()"
               " MERGE (sub)-[relation:SUBCLASS]->(super)"
               "    ON CREATE SET relation.created = datetime()",
               sub_uri=class_["uri"], sub_label=class_["label"], super_uri=parent["uri"], super_label=parent["label"])


def _create_superclass_tree(session, class_: Dict[str, str], visited: Set):
    """
    Recursively build a tree of subclass -> superclass relations up to the "software" root node
    : param session: An open neo4j session
    : param class_: The class URI to start from
    : param visited: A list of class URIs to consider already visited. Usually an empty set
    """
    logger.info(f"Creating superclass tree for {class_['uri']} ({class_['label']}) . . .")
    parents = get_superclasses(class_["uri"])
    session.write_transaction(add_parents, class_, parents)
    for parent in parents:
        if parent["uri"] not in visited:
            visited.add(parent["uri"])
            _create_superclass_tree(session, parent, visited)
        else:
            logger.info(f"Skipped {parent['uri']} ({parent['label']})")


def _sparql_results(query: str) -> Dict:
    """
    Return the results of a SPARQL query against WikiData
    : param query: The query to run
    : return: The SPARQL query result, as a dict
    """
    sparql = SPARQLWrapper("https://query.wikidata.org/sparql",
                           agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) "
                           "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36")
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    response = sparql.query()
    try:
        return response.convert()
    except json.decoder.JSONDecodeError as e:
        logger.debug(response)
        raise e
