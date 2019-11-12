from http.server import HTTPServer, BaseHTTPRequestHandler
import json
from urllib.parse import urlparse, parse_qs

from neo4j import GraphDatabase

driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "123456"))


def node_to_dict(node):
    return {
        "Id": node.id,
        "Uri": node["uri"],
        "Label": node["label"],
        "Type": next(iter(node.labels))
    }


def relationship_to_dict(relationship):
    return {
        "Id": relationship.id,
        "StartId": relationship.start_node.id,
        "EndId": relationship.end_node.id,
        "Label": relationship.type
    }


class BasicServer(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path.startswith("/node?"):
            query_params = parse_qs(urlparse(self.path).query)
            uri = query_params["uri"][0]

            if query_params.get("include_relationships"):
                with driver.session() as session:
                    graph = session.run("MATCH (n1 {uri: $uri})-[r]->(n2) RETURN n1, r, n2", uri=uri).graph()

                dict_relationships = [relationship_to_dict(r) for r in graph.relationships]
                dict_nodes = [node_to_dict(n) for n in graph.nodes]

                self.send_response(200)

                json_response = bytes(json.dumps({
                    "Nodes": dict_nodes,
                    "Relationships": dict_relationships
                }), encoding="UTF-8")

                self.send_header("Content-Length", str(len(json_response)))
                self.end_headers()

                self.wfile.write(json_response)

            else:
                with driver.session() as session:
                    node = session.run("MATCH (n {uri: $uri}) RETURN n", uri=uri).single().value()

                self.send_response(200)

                json_response = bytes(json.dumps(node_to_dict(node)), encoding="UTF-8")

                self.send_header("Content-Length", str(len(json_response)))
                self.end_headers()

                self.wfile.write(json_response)


def run(server_class=HTTPServer, handler_class=BasicServer):
    port = 8080
    server_address = ('', port)
    httpd = server_class(server_address, handler_class)
    print(f"Server listening at http://localhost:{port}")
    httpd.serve_forever()


def main():
    run()


if __name__ == "__main__":
    main()
