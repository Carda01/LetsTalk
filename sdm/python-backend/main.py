# main.py
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import torch
import numpy as np
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import DBSCAN
from sklearn.decomposition import PCA
import io
import base64
from pykeen.datasets import Nations
from pykeen.pipeline import pipeline
from pykeen.triples import TriplesFactory

app = FastAPI()

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Adjust for production
    allow_methods=["*"],
    allow_headers=["*"],
)

def perform_clustering(model_path: str, triples_path: str):
    # Load model and data
    print("Loading model...")
    pykeen_model = torch.load(f"{model_path}/trained_model.pkl",weights_only=False, map_location=torch.device('cpu'))
    
    print("Loading mappings...")
    rel_df = pd.read_csv(f"{model_path}/training_triples/relation_to_id.tsv.gz", 
                        sep='\t', compression='gzip', header=0)
    rel_to_id = dict(zip(rel_df.iloc[:, 1], rel_df.iloc[:, 0]))
    
    ent_df = pd.read_csv(f"{model_path}/training_triples/entity_to_id.tsv.gz",
                        sep='\t', compression='gzip', header=0)
    ent_to_id = dict(zip(ent_df.iloc[:, 1], ent_df.iloc[:, 0]))

    # Get embeddings
    entity_embeddings = pykeen_model.entity_representations[0]().detach().cpu().numpy()
    print("Reading triples...")
    # Load and filter triples
    triples = pd.read_csv(
        triples_path,
        sep='\t',
        header=None,
        names=["head", "relation", "tail"],
        dtype=str
    )
    
    user_entities = set(triples[
        (triples["relation"] == "http://www.w3.org/1999/02/22-rdf-syntax-ns#type") &
        (triples["tail"] == "http://sdm_upc.org/ontology/User")
    ]["head"])

    # Prepare user data
    user_ids = []
    user_labels = []
    for label, idx in ent_to_id.items():
        if label in user_entities:
            user_ids.append(idx)
            user_labels.append(label)

    user_embeddings = entity_embeddings[user_ids]
    
    # Process embeddings
    scaler = StandardScaler()
    user_embeddings_scaled = scaler.fit_transform(user_embeddings)
    
    pca = PCA(n_components=50)
    user_embeddings_reduced = pca.fit_transform(user_embeddings_scaled)
    
    # Cluster
    dbscan = DBSCAN(eps=4, min_samples=5)
    labels = dbscan.fit_predict(user_embeddings_reduced)
    
    # Prepare results
    unique, counts = np.unique(labels, return_counts=True)
    cluster_distribution = dict(zip(unique, counts))
    
    pca_2d = PCA(n_components=2)
    user_embeddings_2d = pca_2d.fit_transform(user_embeddings_reduced)
    
    results = {
        "cluster_distribution": {int(k): int(v) for k, v in cluster_distribution.items()},
        "user_clusters": [
            {"user_uri": uri, "cluster": int(cluster)}
            for uri, cluster in zip(user_labels, labels)
        ],
        "embeddings_2d": user_embeddings_2d.tolist(),
        "cluster_labels": [int(c) for c in labels.tolist()]
    }
    return results

@app.post("/cluster-users")
async def cluster_users():
    try:
        # Update these paths to your actual file locations
        model_path = "./data/kg"
        triples_path = "../data/triples.tsv"
        
        results = perform_clustering(model_path, triples_path)
        return {"status": "success", "data": results}
    except Exception as e:
        return {"status": "error", "message": str(e)}