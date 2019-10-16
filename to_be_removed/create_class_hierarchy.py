from collections import defaultdict
import json
from pathlib import Path
import time
from urllib.error import HTTPError

from neo4j import GraphDatabase
from SPARQLWrapper import SPARQLWrapper, JSON

driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "123456"))


def sparql_results(query):
    sparql = SPARQLWrapper("https://query.wikidata.org/sparql")
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    return sparql.query().convert()


def get_superclasses(class_uri):
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
            time.sleep(1)
            results = sparql_results(query)
            break
        except HTTPError as e:
            if e.code == 429:
                print(e.reason)
                print("Sleeping . . .")
                time.sleep(6)
            else:
                raise e

    return [{"uri": r["class"]["value"], "label": r["classLabel"]["value"]} for r in results["results"]["bindings"]]


def add_parents(tx, uri, parents):
    for parent in parents:
        tx.run("MERGE (sub:Class {uri: $sub_uri})"
               "MERGE (super:Class {uri: $super_uri})"
               "MERGE (sub)-[:SUBCLASS]->(super)"
               "SET super.label = $super_label",
               sub_uri=uri, super_uri=parent["uri"], super_label=parent["label"])


def create_superclass_tree(class_uri, visited):
    with driver.session() as session:
        _create_superclass_tree(class_uri, session, visited)


def _create_superclass_tree(class_uri, session, visited):
    parents = get_superclasses(class_uri)
    print(parents)
    session.write_transaction(add_parents, class_uri, parents)
    for parent in parents:
        if parent["uri"] not in visited:
            visited.add(parent["uri"])
            _create_superclass_tree(parent["uri"], session, visited)
        else:
            print("skipping", parent)


def main():
    datestamp = "1570924800"
    sparql_root = Path("sparql") / Path(datestamp)
    with open(sparql_root / Path(f"query-{datestamp}.json")) as data_file:
        data = json.load(data_file)

    items_by_type = defaultdict(list)
    for item in data:
        for type_ in item["types"].split("||"):
            items_by_type[type_].append(item["item"])  # NOTE: Some items have multiple categories

    visited = set()
    for base_class in items_by_type.keys():
        print(f"Creating relationships for {base_class}: ", end="")
        create_superclass_tree(base_class, visited)


if __name__ == "__main__":
    main()
