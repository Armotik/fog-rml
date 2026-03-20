from rdflib import Graph

g = Graph()
g.parse("../../check_refactor.nt", format="nt")

query = """
PREFIX schema: <http://schema.org/>

SELECT ?issue ?title ?description
WHERE {
    ?issue a schema:Issue ;
           schema:name ?title ;
           schema:description ?description .
    FILTER(CONTAINS(LCASE(?title), "bug"))
}
"""

results = g.query(query)

for row in results:
    print(f"Issue: {row.issue}, Title: {row.title}, Description: {row.description}")