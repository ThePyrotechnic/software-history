import json
import logging
from typing import Dict, List, Tuple

from neo4j import GraphDatabase
from SPARQLWrapper import SPARQLWrapper, JSON

logger = logging.getLogger(__name__)


class Tasks:
    def __init__(self, server: str, auth: Tuple[str, str]):
        logger.info("Connecting to database . . .")
        self.driver = GraphDatabase.driver(server, auth=auth)
        logger.info("Connection complete")

    def add_genre_to_videogames(self):
        logger.info('Fetching current instances of "video game" with genre . . .')
        wikidata_query = _sparql_results("""
            SELECT DISTINCT ?item ?genre ?genreLabel WHERE {
                ?item wdt:P31/wdt:P279* wd:Q7889;
                    wdt:P136 ?genre
                SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
            }
        """)
        wikidata_games = [{
            "game_uri": game["item"]["value"],
            "genre_uri": game["genre"]["value"],
            "genre_label": game["genreLabel"]["value"]
        } for game in wikidata_query["results"]["bindings"]]
        logger.info("Complete")

        logger.info('Adding genre labels to existing games . . .')
        with self.driver.session() as session:
            query = session.run("""
                UNWIND $games as game
                    MATCH (v:Software {uri: game.game_uri})
                    MERGE (g:Genre {uri: game.genre_uri})
                        ON CREATE SET g.label = game.genre_label, g.created = datetime()
                    MERGE (v)-[m:MEMBER]->(g)
                        ON CREATE SET m.created = datetime()
            """, games=wikidata_games)
            result = query.consume()
            nodes_created = result['counters']['nodes_created']
            relationships_created = result['counters']['relationships_created']
            logger.info(f"Complete. Created {nodes_created} nodes, {relationships_created} relationships")

    def update_software_and_classes(self):
        """
        Add software which does not currently exist in the database. Create necessary superclass relations to support
        new software nodes. Do not update existing nodes or relationships
        """
        with self.driver.session() as session:
            logger.info("Fetching current software instance URIs . . .")
            current_software = session.run("MATCH (n:Software) RETURN n.uri AS uri, ID(n)").records()
            current_software = set(software["uri"] for software in current_software)
            logger.info("Complete")

            logger.info("Fetching current software class URIs . . .")
            current_classes = session.run("MATCH (n:Class) RETURN n.uri AS uri, ID(n)").records()
            current_classes = set(subclass["uri"] for subclass in current_classes)
            logger.info("Complete")

        logger.info("Fetching current list of WikiData software classes . . .")
        class_nodes = self._get_software_classes()
        class_nodes = [node for node in class_nodes if node["child_uri"] not in current_classes]
        logger.info("Complete")

        logger.info("Fetching current list of WikiData software . . .")
        software_nodes = self._get_software_instances()
        software_nodes = [node for node in software_nodes if node["child_uri"] not in current_software]
        logger.info("Complete")

        self._merge_data(class_nodes, "Class", "SUBCLASS")
        self._merge_data(software_nodes, "Software", "INSTANCE")

    def _merge_data(self, data: List[Dict[str, str]], db_label: str, relationship: str, batch_size: int = 500):
        """
        Merge all data passed in the data argument into the neo4j database.
        :param data: List of dictionaries of all data to be merged
        :param db_label: Label of data (Software, Class, etc.)
        :param relationship: Type of relationship being added (INSTANCE, SUBCLASS, etc.)
        :param batch_size: Size of batches to send data to neo4j, default 500
        """
        with self.driver.session() as session:
            for i, batch in enumerate(self._generate_batches(data, batch_size)):
                logger.info(
                    f"Merging {relationship} relationship for {str(len(batch))} new {db_label} entries "
                    f"({len(data) - (i * batch_size)} {db_label} entries remaining)"
                )
                session.run(
                    f"""UNWIND $batch AS data
                            MERGE(child: {db_label} {{uri: data.child_uri}})
                                ON CREATE SET child.label = data.child_label, child.created = datetime()
                            MERGE(parent: Class {{uri: data.parent_uri}})
                                ON CREATE SET parent.label = data.parent_label, parent.created = datetime()
                            MERGE(child)-[relation: {relationship}] -> (parent)
                                ON CREATE SET relation.created = datetime()
                            """,
                    batch=batch,
                )
                # Sync statement prevents lazy return of query response, blocks until completion
                session.sync()
            logger.info(f"Completed merge of {len(data)} {db_label} entries")

    @staticmethod
    def _get_software_instances() -> List[Dict[str, str]]:
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
               }"""
        )
        return [
            {
                "child_uri": software["item"]["value"],
                "child_label": software["itemLabel"]["value"],
                "parent_uri": software["type"]["value"],
                "parent_label": software["typeLabel"]["value"],
            }
            for software in wikidata_software["results"]["bindings"]
        ]

    @staticmethod
    def _get_software_classes() -> List[Dict[str, str]]:
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
               }"""
        )
        return [
            {
                "child_uri": subclass["class"]["value"],
                "child_label": subclass["classLabel"]["value"],
                "parent_uri": subclass["classParent"]["value"],
                "parent_label": subclass["classParentLabel"]["value"],
            }
            for subclass in wikidata_subclasses["results"]["bindings"]
        ]

    @staticmethod
    def _generate_batches(data: List, batch_size: int) -> List:
        """
        Yield list of items of length batch_size for all items in data.
        :param data: A list of dicts from SPARQL query return
        :param batch_size: The size of each yielded batch of data elements
        """
        for i in range(0, len(data), batch_size):
            yield data[i: i + batch_size]


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
