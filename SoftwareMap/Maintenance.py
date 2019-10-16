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

    def add_new_software(self):
        """
        Add software which does not currently exist in the database. Create necessary superclass relations to support
        new software nodes. Do not update existing nodes or relationships
        """
        with self.driver.session() as session:
            logger.info("Fetching current software URIs . . .")
            current_software = session.run("MATCH (n:Software) RETURN n.uri AS uri, ID(n)").records()
            current_software = set(software["uri"] for software in current_software)
            logger.info("Complete")

        logger.info("Fetching current list of WikiData software . . .")
        wikidata_software = _sparql_results(
            """SELECT DISTINCT ?item ?itemLabel ?type ?typeLabel WHERE {
                  ?item wdt:P31 ?type.
                  ?type (wdt:P279*) wd:Q7397.
                  SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
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
    :param class_uri: The class URI to get superclasses for
    :return: A list of dictionaries: [{"uri": <uri>, "label": <label}, ...]
    """
    query = """SELECT DISTINCT ?class ?classLabel WHERE {
      wd:%s wdt:P279 ?class.
      ?class wdt:P279* wd:Q7397.
      SERVICE wikibase:label {
        bd:serviceParam wikibase:language "en".
        ?class rdfs:label ?classLabel
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
    :param tx: Passed automatically by session.write_transaction()
    :param class_: The subclass URI to draw the relations from
    :param parents: The superclass URIs to draw the relations to
    """
    for parent in parents:
        tx.run("MERGE (sub:Class {uri: $sub_uri})"
               "    ON CREATE SET sub.label = $sub_label, sub.created: datetime()"  # Don't re-set the label
               " MERGE (super:Class {uri: $super_uri})"                             # (TODO pending "update" timestamps)
               "    ON CREATE SET super.label = $super_label, super.created = datetime()"
               " MERGE (sub)-[relation:SUBCLASS]->(super)"
               "    ON CREATE SET relation.created = datetime()",
               sub_uri=class_["uri"], sub_label=class_["label"], super_uri=parent["uri"], super_label=parent["label"])


def _create_superclass_tree(session, class_: Dict[str, str], visited: Set):
    """
    Recursively build a tree of subclass->superclass relations up to the "software" root node
    :param session: An open neo4j session
    :param class_: The class URI to start from
    :param visited: A list of class URIs to consider already visited. Usually an empty set
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
    :param query: The query to run
    :return: The SPARQL query result, as a dict
    """
    sparql = SPARQLWrapper("https://query.wikidata.org/sparql",
                           agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) "
                                 "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36")
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    return sparql.query().convert()
