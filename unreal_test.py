import json

from flask import Flask, jsonify, request
from neo4j import GraphDatabase

app = Flask(__name__)

driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "123456"))


def node_to_dict(node):
    node_dict = {
        "Id": str(node.id),
        "Uri": node["uri"],
        "Label": node["label"],
        "Type": next(iter(node.labels))
    }
    if node.get("release"):
        node_dict["ReleaseYear"] = node["release"].year
        node_dict["ReleaseMonth"] = node["release"].month
        node_dict["ReleaseDay"] = node["release"].day

    return node_dict


def relationship_to_dict(relationship):
    return {
        "Id": str(relationship.id),
        "StartId": str(relationship.start_node.id),
        "EndId": str(relationship.end_node.id),
        "Type": str(relationship.type)
    }


@app.route("/node", methods=["GET"])
def get_node():
    uri = request.args.get("uri", type=str)
    with driver.session() as session:
        graph = session.run("MATCH (n1 {uri: $uri})-[r]->(n2) RETURN n1, r, n2", uri=uri).graph()

    dict_relationships = [relationship_to_dict(r) for r in graph.relationships]
    dict_nodes = [node_to_dict(n) for n in graph.nodes]

    return jsonify({
        "Nodes": dict_nodes,
        "Relationships": dict_relationships
    })


@app.route("/class", methods=["GET"])
def get_class():
    uri = request.args.get("uri", type=str)

    with driver.session() as session:
        graph = session.run("MATCH (s:Software)-[r:INSTANCE]->(c:Class {uri: $uri}) RETURN s,r,c",
                            uri=uri).graph()

        dict_relationships = [relationship_to_dict(r) for r in graph.relationships]
        dict_nodes = [node_to_dict(n) for n in graph.nodes]

    return jsonify({
        "Nodes": dict_nodes,
        "Relationships": dict_relationships
    })


@app.route("/graph", methods=["GET"])
def get_graph():
    try:
        with open("graph.txt") as graph_file:
            response_str = graph_file.read()
    except FileNotFoundError:
        with driver.session() as session:
            graph = session.run("MATCH (n)-[r]->(c:Class) RETURN n, r, c").graph()

        dict_relationships = [relationship_to_dict(r) for r in graph.relationships]
        dict_nodes = [node_to_dict(n) for n in graph.nodes]

        with open("graph.txt", "w") as graph_file:
            response_str = json.dumps({
                "Nodes": dict_nodes,
                "Relationships": dict_relationships
            }, separators=(",", ":"))
            print(response_str, file=graph_file)

    return response_str


def main():
    app.run(port=8080, debug=True)


if __name__ == "__main__":
    main()
