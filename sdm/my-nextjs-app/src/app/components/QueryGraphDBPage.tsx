"use client";

import React, { useState, useEffect } from "react";
import axios from "axios";
import Bubble from "@/app/components/Bubble";
import RefreshButton from "@/app/components/Button";

interface SparqlValue {
  type: string;
  value: string;
}

interface SparqlBinding {
  [key: string]: SparqlValue;
}

interface SparqlResponse {
  results: {
    bindings: SparqlBinding[];
  };
}

interface MetricQuery {
  title: string;
  query: string;
  description: string;
}

interface Metric {
  title: string;
  value: string | number;
  description: string;
}

const QueryGraphDBPage: React.FC = () => {
  const [metrics, setMetrics] = useState<Metric[]>([]);
  const [loading, setLoading] = useState<boolean>(false);
  const [error, setError] = useState<string | null>(null);

  const queries: MetricQuery[] = [
    {
      title: "Total Triples",
      query: `SELECT (COUNT(*) AS ?count) WHERE { ?s ?p ?o }`,
      description: "Total triples stored in GraphDB",
    },
    {
      title: "Classes",
      query: `
        PREFIX sdm: <http://sdm_upc.org/ontology/>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        SELECT (COUNT(DISTINCT ?class) AS ?classCount)
        WHERE { ?class rdf:type rdfs:Class . 
        FILTER(STRSTARTS(STR(?class), STR(sdm:)))
        }
      `,
      description: "Number of unique classes",
    },
    {
      title: "Properties",
      query: `
        PREFIX sdm: <http://sdm_upc.org/ontology/>
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        SELECT (COUNT(DISTINCT ?property) AS ?propertyCount) WHERE{
            {
                ?property a rdf:Property .
                    FILTER(STRSTARTS(STR(?property), STR(sdm:)))
            }
            }
      `,
      description: "Number of distinct properties",
    },
  ];

  const runAllQueries = async () => {
    setLoading(true);
    setError(null);

    try {
      const results: Metric[] = [];

      for (const q of queries) {
        const response = await axios.post<SparqlResponse>("/api/graphdb", {
          query: q.query,
        });

        const binding = response.data.results.bindings?.[0];

        let value = "N/A";
        if (binding) {
          const key = Object.keys(binding)[0]; // dynamically detect key like count, classCount, etc.
          value = binding[key]?.value ?? "N/A";
        }

        results.push({
          title: q.title,
          value: isNaN(Number(value)) ? value : Number(value),
          description: q.description,
        });
      }

      setMetrics(results);
    } catch (err: unknown) {
      let message = "An unknown error occurred.";
      if (axios.isAxiosError(err)) {
        message = err.response?.data?.message || err.message;
        console.error("Error calling API:", err.response?.data || err.message);
      } else if (err instanceof Error) {
        message = err.message;
      }
      setError(message);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    runAllQueries();
  }, []);

  return (
    <div>
      <div className="p-6">
        <div className="relative flex flex-row">
          <h1 className="text-2xl font-semibold mb-6">GraphDB Dashboard</h1>
          <div className="absolute right-0">
            <RefreshButton onClick={runAllQueries} loading={loading} />
          </div>
        </div>

        {error && <p className="text-red-600 mb-4">Error: {error}</p>}

        <div className="grid gap-6 grid-cols-1 sm:grid-cols-2 md:grid-cols-3 lg:grid-cols-4">
          {metrics.map((metric, idx) => (
            <Bubble key={idx} {...metric} />
          ))}
        </div>
      </div>
    </div>
  );
};

export default QueryGraphDBPage;
