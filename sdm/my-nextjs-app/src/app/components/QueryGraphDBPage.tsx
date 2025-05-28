"use client";

import React, { useState, useEffect } from "react";
import axios from "axios";
import Bubble from "@/app/components/Bubble";
import RefreshButton from "@/app/components/Button";
import ClusterPlot from "@/app/components/Graph";

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

interface ClusterQuery {
  id: number;
  query: string;
}

interface Cluster {
  id: number;
  value: string | number;
}
interface ClusterResult {
  user_uri: string;
}

interface ClusterResponse {
  status: string;
  data?: {
    cluster_distribution: Record<number, number>;
    user_clusters: ClusterResult[];
    embeddings_2d: [number, number][];
    cluster_labels: number[];
  };
  message?: string;
}

const QueryGraphDBPage: React.FC = () => {
  const [metrics, setMetrics] = useState<Metric[]>([]);
  const [card, setCard] = useState<Cluster[]>([]);
  const [loading, setLoading] = useState<boolean>(false);
  const [error, setError] = useState<string | null>(null);
  const [loadingCluster, setLoadingCluster] = useState<boolean>(false);
  const [errorCluster, setErrorCluster] = useState<string | null>(null);
  const [clusterData, setClusterData] = useState<ClusterResponse | null>(null);
  const [clusterLoading, setClusterLoading] = useState(false);

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
    {
      title: "Users",
      query: `
        PREFIX sdm: <http://sdm_upc.org/ontology/>
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        SELECT (COUNT(DISTINCT ?users) AS ?userCount) WHERE{
            {
                ?users a sdm:User .
            }
        }
      `,
      description: "Number of distinct users",
    },
  ];

  const queriesCluster: ClusterQuery[] = [
    {
      id: 0,
      query: `SELECT (COUNT(*) AS ?count) WHERE { ?s ?p ?o }`,
    },
    {
      id: 1,
      query: `
        PREFIX sdm: <http://sdm_upc.org/ontology/>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        SELECT (COUNT(DISTINCT ?class) AS ?classCount)
        WHERE { ?class rdf:type rdfs:Class . 
        FILTER(STRSTARTS(STR(?class), STR(sdm:)))
        }
      `,
    },
    {
      id: 2,
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
    },
  ];
  const runclusterQuery = async () => {
    setLoadingCluster(true);
    setErrorCluster(null);

    try {
      const results = await Promise.all(
        queriesCluster.map(async (q) => {
          try {
            const response = await axios.post<SparqlResponse>("/api/graphdb", {
              query: q.query,
            });

            const binding = response.data.results.bindings?.[0];
            let value = "N/A";

            if (binding) {
              const key = Object.keys(binding)[0];
              value = binding[key]?.value ?? "N/A";
            }

            return {
              id: q.id,
              value: isNaN(Number(value)) ? value : Number(value),
            };
          } catch (err) {
            console.error("Error executing query:", err);
            return {
              id: q.id,
              value: "Error",
            };
          }
        })
      );

      setCard(results);
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

  const runAllQueries = async () => {
    setLoading(true);
    setError(null);

    try {
      const results: Metric[] = await Promise.all(
        queries.map(async (q) => {
          try {
            const response = await axios.post<SparqlResponse>("/api/graphdb", {
              query: q.query,
            });

            const binding = response.data.results.bindings?.[0];
            let value = "N/A";

            if (binding) {
              const key = Object.keys(binding)[0];
              value = binding[key]?.value ?? "N/A";
            }

            return {
              title: q.title,
              value: isNaN(Number(value)) ? value : Number(value),
              description: q.description,
            };
          } catch (err) {
            console.error(`Error executing query "${q.title}":`, err);
            return {
              title: q.title,
              value: "Error",
              description: q.description,
            };
          }
        })
      );

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

  const runClustering = async () => {
    setClusterLoading(true);
    try {
      const response = await fetch("http://localhost:8000/cluster-users", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({}),
      });

      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }

      const data: ClusterResponse = await response.json();
      setClusterData(data);
    } catch (err: unknown) {
      const errorMessage =
        err instanceof Error ? err.message : "Unknown error occurred";
      setClusterData({
        status: "error",
        message: errorMessage,
      });
    } finally {
      setClusterLoading(false);
    }
  };

  useEffect(() => {
    runAllQueries();
    runClustering();
    runclusterQuery();
  }, []);

  return (
    <div className="p-6">
      <div className="relative flex flex-row">
        <h1 className="text-2xl font-semibold mb-6">GraphDB Dashboard</h1>
        <div className="absolute right-0">
          <RefreshButton onClick={runClustering} loading={loading} />
        </div>
      </div>

      {error && <p className="text-red-600 mb-4">Error: {error}</p>}

      <div className="grid gap-6 grid-cols-1 sm:grid-cols-2 md:grid-cols-3 lg:grid-cols-4">
        {metrics.map((metric, idx) => (
          <Bubble key={idx} {...metric} />
        ))}
      </div>

      <div className="flex flex-col lg:flex-row items-center mt-8 gap-6">
        {/* Cluster Plot */}
        <div className="flex-1 min-w-0">
          <ClusterPlot />
        </div>

        {/* Stats per cluster */}
        {clusterData?.data?.cluster_distribution && (
          <div className="flex-1 h-110 overflow-x-auto">
            <div
              className="flex gap-6 snap-x h-full snap-mandatory overflow-x-scroll px-2 py-4"
              style={{ scrollSnapType: "x mandatory" }}
            >
              {Object.entries(clusterData.data.cluster_distribution)
                .filter(([clusterId]) => parseInt(clusterId, 10) !== -1)
                .map(([clusterId, count]) => (
                  <div
                    key={clusterId}
                    className="snap-start h-full min-w-[300px] max-w-[300px]  p-6 rounded-xl shadow-xl flex-shrink-0"
                  >
                    <h3 className="font-bold text-xl text-blue-600 mt-2 mb-2">
                      Cluster {clusterId}
                    </h3>
                    <p className="text-gray-700 text-md">
                      <span className="font-semibold">Users:</span> {count}
                    </p>
                    {card.map(
                      (item, index) =>
                        item.id === parseInt(clusterId, 10) && (
                          <p key={index} className="text-gray-700 text-md">
                            <span className="font-semibold">Value:</span>{" "}
                            {item.value}
                          </p>
                        )
                    )}
                  </div>
                ))}
            </div>
          </div>
        )}
      </div>
    </div>
  );
};

export default QueryGraphDBPage;
