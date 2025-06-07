import requests
import csv

# Configuration
graphdb_url = "http://localhost:7200"
repo_id = "letstalk"
sparql_endpoint = f"{graphdb_url}/repositories/{repo_id}"
output_file = "drive-embeddings/data/triples.tsv"

# SPARQL query to get all triples
sparql_query = """
SELECT ?s ?p ?o
WHERE {
  ?s ?p ?o .
}
"""

# Headers for the SPARQL request
headers = {
    "Accept": "application/sparql-results+json"
}

# Parameters for the request
params = {
    "query": sparql_query
}

# Make the request
response = requests.get(sparql_endpoint, headers=headers, params=params)
response.raise_for_status()

# Parse JSON results
results = response.json()["results"]["bindings"]

print("Response from the database obtained! Saving to TSV")

# Write to TSV
with open(output_file, "w", newline="", encoding="utf-8") as tsvfile:
    writer = csv.writer(tsvfile, delimiter="\t")
    writer.writerow(["subject", "predicate", "object"])  # Header

    for result in results:
        s = result["s"]["value"]
        p = result["p"]["value"]
        o = result["o"]["value"]
        writer.writerow([s, p, o])

print(f"Triples saved to {output_file}")



