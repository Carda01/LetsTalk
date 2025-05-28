'use client'; // Only for Next.js App Router. Remove this line if not using Next.js
import React, { useEffect, useState } from 'react';
import dynamic from 'next/dynamic';
import axios from 'axios';

const Plot = dynamic(() => import('react-plotly.js'), {
  ssr: false, // 👈 Prevent SSR to fix "self is not defined"
});

type ClusterData = {
  user_uri: string;
  cluster: number;
};

type ClusterResponse = {
  embeddings_2d: number[][];
  cluster_labels: number[];
  user_clusters: ClusterData[];
};

interface ClusterPlotProps {
  // Option 1: Pass cluster data as props
  clusterData?: {
    embeddings_2d: number[][];
    cluster_labels: number[];
    user_clusters: ClusterData[];
  } | null;
  // Option 2: Pass a refresh trigger
  refreshTrigger?: number;
  // Option 3: Pass clustering parameters
  eps?: number;
  minSamples?: number;
}

const ClusterPlot: React.FC<ClusterPlotProps> = ({ 
  clusterData, 
  refreshTrigger, 
  eps, 
  minSamples 
}) => {
  const [plotData, setPlotData] = useState<unknown[]>([]);
  const [loaded, setLoaded] = useState(false);
  const [loading, setLoading] = useState(false);

  const fetchClusterData = async () => {
    setLoading(true);
    try {
      const response = await axios.post('http://localhost:8000/cluster-users', {
        eps: eps,
        min_samples: minSamples
      });
      
      if (response.data.status === 'success') {
        const data: ClusterResponse = response.data.data;
        processClusterData(data);
      }
    } catch (error) {
      console.error('Error fetching cluster data:', error);
    } finally {
      setLoading(false);
    }
  };

  const processClusterData = (data: ClusterResponse) => {
    const { embeddings_2d, cluster_labels, user_clusters } = data;
    const pointsByCluster: Record<number, { x: number[], y: number[], text: string[] }> = {};

    embeddings_2d.forEach(([x, y], i) => {
      const label = cluster_labels[i];
      const uri = user_clusters[i]?.user_uri || 'Unknown';

      if (!pointsByCluster[label]) {
        pointsByCluster[label] = { x: [], y: [], text: [] };
      }

      pointsByCluster[label].x.push(x);
      pointsByCluster[label].y.push(y);
      pointsByCluster[label].text.push(uri); // Tooltip text
    });

    const traces = Object.entries(pointsByCluster).map(([label, points]) => ({
      x: points.x,
      y: points.y,
      text: points.text,
      type: 'scatter',
      mode: 'markers',
      marker: { size: 8 },
      name: `Cluster ${label}`
    }));

    setPlotData(traces);
    setLoaded(true);
  };

  useEffect(() => {
    // Option 1: Use passed cluster data if available
    if (clusterData) {
      processClusterData(clusterData);
      return;
    }

    // Option 2 & 3: Fetch data when component mounts or dependencies change
    fetchClusterData();
  }, [clusterData, refreshTrigger, eps, minSamples]);

  return (
    <div>
      {loading ? (
        <p>Loading cluster data...</p>
      ) : loaded ? (
        <Plot
          data={plotData}
          layout={{
            title: { text: 'User Embedding Clusters' },
            xaxis: { title: { text: 'PCA 1' } },
            yaxis: { title: { text: 'PCA 2' } },
            height: 600,
          }}
        />
      ) : (
        <p>Loading...</p>
      )}
    </div>
  );
};

export default ClusterPlot;