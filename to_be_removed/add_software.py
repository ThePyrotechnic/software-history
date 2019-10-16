import json
from pathlib import Path

from neo4j import GraphDatabase

driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "123456"))


def add_parents(tx, uri, label, parents):
    for parent in parents:
        tx.run("MATCH (super:Class {uri: $super_uri})"
               "MERGE (sw:Software {uri: $sw_uri, label: $sw_label})"
               "CREATE (sw)-[:INSTANCE]->(super)",
               sw_uri=uri, sw_label=label, super_uri=parent)


def main():
    datestamp = "1570924800"
    sparql_root = Path("sparql") / Path(datestamp)
    with open(sparql_root / Path(f"query-{datestamp}.json")) as data_file:
        data = json.load(data_file)

    with driver.session() as session:
        for count, item in enumerate(data):
            print(count, "/", len(data))
            session.write_transaction(add_parents, item["item"], item["itemLabel"], item["types"].split("||"))


if __name__ == "__main__":
    main()
