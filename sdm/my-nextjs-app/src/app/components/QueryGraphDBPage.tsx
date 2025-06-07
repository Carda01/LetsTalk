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
    parameters?: {
      eps: number;
      min_samples: number;
      max_users?: number;
      total_users_available?: number;
      users_processed?: number;
      sampling_method?: string;
    };
  };
  message?: string;
}

interface ClusterPreference {
  category: string;
  name: string;
}

interface ClusterPreferences {
  [clusterId: number]: ClusterPreference[];
}

const QueryGraphDBPage: React.FC = () => {
  const [metrics, setMetrics] = useState<Metric[]>([]);
  const [loading, setLoading] = useState<boolean>(false);
  const [error, setError] = useState<string | null>(null);
  const [clusterData, setClusterData] = useState<ClusterResponse | null>(null);
  const [clusterLoading, setClusterLoading] = useState(false);
  const [clusterPreferences, setClusterPreferences] = useState<ClusterPreferences>({});
  const [preferencesLoading, setPreferencesLoading] = useState(false);
  const [eps, setEps] = useState<string>("4.0");
  const [minSamples, setMinSamples] = useState<string>("5");
  const [maxUsers, setMaxUsers] = useState<string>("1000");
  const [samplingMethod, setSamplingMethod] = useState<string>("random");

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

  // Function to get cluster preferences query for a specific cluster
  const getClusterPreferencesQuery = (clusterId: number, userUris: string[]) => {
    const userUriValues = userUris.map((uri) => `<${uri}>`).join(" ");

    return `
      PREFIX sdm: <http://sdm_upc.org/ontology/>
      PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
      SELECT DISTINCT ?category ?name WHERE {
        {
          SELECT ?category ?name WHERE {
            VALUES ?a { ${userUriValues} }
            ?a sdm:likes_genre ?g .
            ?g sdm:genre_name ?name .
            BIND("Genre" AS ?category)
          }
          GROUP BY ?category ?name
          ORDER BY DESC(COUNT(?name))
          LIMIT 1
        }
        UNION
        {
          SELECT ?category ?name WHERE {
            VALUES ?a { ${userUriValues} }
            ?a sdm:likes_competition ?l .
            ?l sdm:competition_name ?name .
            BIND("Competition" AS ?category)
          }
          GROUP BY ?category ?name
          ORDER BY DESC(COUNT(?name))
          LIMIT 1
        }
        UNION
        {
          SELECT ?category ?name WHERE {
            VALUES ?a { ${userUriValues} }
            ?a sdm:likes_movie ?m .
            ?m sdm:movie_title ?name .
            BIND("Movie" AS ?category)
          }
          GROUP BY ?category ?name
          ORDER BY DESC(COUNT(?name))
          LIMIT 1
        }
        UNION
        {
          SELECT ?category ?name WHERE {
            VALUES ?a { ${userUriValues} }
            ?a sdm:likes_team ?t .
            ?t sdm:team_name ?name .
            BIND("Team" AS ?category)
          }
          GROUP BY ?category ?name
          ORDER BY DESC(COUNT(?name))
          LIMIT 1
        }
        UNION
        {
          SELECT ?category ?name WHERE {
            VALUES ?a { ${userUriValues} }
            ?a sdm:interested_in ?k .
            ?k sdm:keyword_text ?name .
            BIND("Keyword" AS ?category)
          }
          GROUP BY ?category ?name
          ORDER BY DESC(COUNT(?name))
          LIMIT 1
        }
      }
    `;
  };

  const runClusterPreferencesQueries = async () => {
    if (!clusterData?.data?.user_clusters || !clusterData?.data?.cluster_labels) {
      return;
    }

    setPreferencesLoading(true);

    try {
      // Group users by cluster
      const usersByCluster: Record<number, string[]> = {};

      clusterData.data.user_clusters.forEach((user, index) => {
        const clusterId = clusterData.data!.cluster_labels[index];
        if (clusterId !== -1) {
          // Exclude noise points
          if (!usersByCluster[clusterId]) {
            usersByCluster[clusterId] = [];
          }
          usersByCluster[clusterId].push(user.user_uri);
        }
      });

      // Run preference queries for each cluster
      const preferencesPromises = Object.entries(usersByCluster).map(
        async ([clusterId, userUris]) => {
          const clusterIdNum = parseInt(clusterId, 10);
          const query = getClusterPreferencesQuery(clusterIdNum, userUris);

          try {
            const response = await axios.post<SparqlResponse>("/api/graphdb", {
              query: query,
            });

            const preferences: ClusterPreference[] =
              response.data.results.bindings.map((binding) => ({
                category: binding.category?.value || "Unknown",
                name: binding.name?.value || "Unknown",
              }));

            return { clusterId: clusterIdNum, preferences };
          } catch (err) {
            console.error(`Error executing preferences query for cluster ${clusterId}:`, err);
            return { clusterId: clusterIdNum, preferences: [] };
          }
        }
      );

      const results = await Promise.all(preferencesPromises);

      const newPreferences: ClusterPreferences = {};
      results.forEach(({ clusterId, preferences }) => {
        newPreferences[clusterId] = preferences;
      });

      setClusterPreferences(newPreferences);
    } catch (err) {
      console.error("Error running cluster preferences queries:", err);
    } finally {
      setPreferencesLoading(false);
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
      const epsValue = parseFloat(eps) || 4.0;
      const minSamplesValue = parseInt(minSamples) || 5;
      const maxUsersValue = parseInt(maxUsers) || 1000;

      const response = await fetch("http://localhost:8000/cluster-users", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          eps: epsValue,
          min_samples: minSamplesValue,
          max_users: maxUsersValue,
          sampling_method: samplingMethod,
        }),
      });

      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }

      const data: ClusterResponse = await response.json();
      setClusterData(data);
    } catch (err: unknown) {
      const errorMessage = err instanceof Error ? err.message : "Unknown error occurred";
      setClusterData({
        status: "error",
        message: errorMessage,
      });
    } finally {
      setClusterLoading(false);
    }
  };

  const refreshAll = async () => {
    await Promise.all([runAllQueries(), runClustering()]);
  };

  useEffect(() => {
    runAllQueries();
    runClustering();
  }, []);

  // Run preferences queries when cluster data is available
  useEffect(() => {
    if (clusterData?.data?.user_clusters) {
      runClusterPreferencesQueries();
    }
  }, [clusterData]);

  return (
    <div className="p-6">
      <div className="relative flex flex-row">
        <h1 className="text-2xl font-semibold mb-6">GraphDB Dashboard</h1>
        <div className="absolute right-0">
          <RefreshButton
            onClick={refreshAll}
            loading={loading || clusterLoading || preferencesLoading}
          />
        </div>
      </div>

      <div className="grid gap-6 grid-cols-1 sm:grid-cols-2 md:grid-cols-3 lg:grid-cols-4">
        {metrics.map((metric, idx) => (
          <Bubble key={idx} {...metric} />
        ))}
      </div>

      {/* DBSCAN Parameters */}
      <div className="mb-6 p-4 bg-gray-50 rounded-lg mt-10">
        <h2 className="text-md font-semibold mb-3">DBSCAN Clustering Parameters</h2>
        <div className="grid gap-4 grid-cols-1 md:grid-cols-2 lg:grid-cols-4 items-end">
          <div className="flex flex-col">
            <label htmlFor="eps" className="text-sm font-medium text-gray-700 mb-1">
              EPS (ε)
            </label>
            <input
              id="eps"
              type="number"
              step="0.1"
              min="0.1"
              value={eps}
              onChange={(e) => setEps(e.target.value)}
              className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
            />
          </div>
          <div className="flex flex-col">
            <label htmlFor="minSamples" className="text-sm font-medium text-gray-700 mb-1">
              Min Samples
            </label>
            <input
              id="minSamples"
              type="number"
              min="1"
              value={minSamples}
              onChange={(e) => setMinSamples(e.target.value)}
              className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
            />
          </div>
          <div className="flex flex-col">
            <label htmlFor="maxUsers" className="text-sm font-medium text-gray-700 mb-1">
               User Sample
            </label>
            <input
              id="maxUsers"
              type="number"
              min="10"
              max="1000"
              value={maxUsers}
              onChange={(e) => setMaxUsers(e.target.value)}
              className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
            />
          </div>
          <div className="flex flex-col">
            <label htmlFor="samplingMethod" className="text-sm font-medium text-gray-700 mb-1">
              Sampling Method
            </label>
            <select
              id="samplingMethod"
              value={samplingMethod}
              onChange={(e) => setSamplingMethod(e.target.value)}
              className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
            >
              <option value="random">Random</option>
              <option value="first">First N</option>
            </select>
          </div>
        </div>
        <div className="mt-4">
          <button
            onClick={runClustering}
            disabled={clusterLoading}
            className="px-4 py-2 bg-blue-600 text-white rounded-md hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-blue-500 disabled:bg-gray-400 disabled:cursor-not-allowed"
          >
            {clusterLoading ? "Clustering..." : "Run Clustering"}
          </button>
        </div>
        {clusterData?.data?.parameters && (
          <div className="mt-2 text-sm text-gray-600">
            Last run: EPS = {clusterData.data.parameters.eps}, Min Samples = {clusterData.data.parameters.min_samples}
            {clusterData.data.parameters.max_users && (
              <>
                , Max Users = {clusterData.data.parameters.max_users}
                {clusterData.data.parameters.total_users_available && (
                  <> (from {clusterData.data.parameters.total_users_available} total)</>
                )}
                , Method = {clusterData.data.parameters.sampling_method}
              </>
            )}
          </div>
        )}
      </div>

      {error && <p className="text-red-600 mb-4">Error: {error}</p>}

      <div className="flex flex-col lg:flex-row items-center mt-8 gap-6">
        {/* Cluster Plot */}
        <div className="flex-1 min-w-0">
          <ClusterPlot clusterData={clusterData?.data} />
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
                .map(([clusterId, count]) => {
                  const clusterIdNum = parseInt(clusterId, 10);
                  const preferences = clusterPreferences[clusterIdNum] || [];

                  return (
                    <div
                      key={clusterId}
                      className="snap-start h-full min-w-[300px] max-w-[300px] p-6 rounded-xl shadow-xl flex-shrink-0"
                    >
                      <h3 className="font-bold text-xl text-blue-600 mt-2 mb-2">
                        Cluster {clusterId}
                      </h3>
                      <p className="text-gray-700 text-md mb-2">
                        <span className="font-semibold">Users:</span> {count}
                      </p>

                      {/* Cluster Preferences Section */}
                      <div className="mt-4">
                        <h4 className="font-semibold text-lg text-gray-800 mb-2">
                          Top Preferences:
                        </h4>
                        {preferencesLoading ? (
                          <p className="text-gray-500 text-sm italic">Loading preferences...</p>
                        ) : preferences.length > 0 ? (
                          <div className="space-y-1">
                            {preferences.map((pref, idx) => (
                              <div key={idx} className="text-sm">
                                <span className="font-medium text-blue-600">{pref.category}:</span>{" "}
                                <span className="text-gray-700">{pref.name}</span>
                              </div>
                            ))}
                          </div>
                        ) : (
                          <p className="text-gray-500 text-sm italic">No preferences found</p>
                        )}
                      </div>
                    </div>
                  );
                })}
            </div>
          </div>
        )}
      </div>
    </div>
  );
};

export default QueryGraphDBPage;