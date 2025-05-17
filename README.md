# Let's Talk P1
## Description 
Our project consists in a pipeline orchestrated with Airflow, that fetches data from different APIs, and uploads them into a Google Cloud bucket, using Delta Lake format.

## Reproducibility
Clone the repository.

### Only local setup
In case you don't want to use google cloud remove these lines from the Dockerfile
and jump to the the [start airflow](#start-airflow) section
```dockerfile
COPY gcs-connector-hadoop.jar gcs/
COPY gcs.json gcs/
```

### Setting up Google Cloud
Set up Google Cloud, we recommend you to follow this [guide](https://delta.io/blog/delta-lake-gcp/),
which will explain you how to set up a bucket, create a service account and once you have done so you can
download the key in json format rename it to **gcs.json** and move it inside the docker folder. 

Also download the cloud storage connector from
[here](https://cloud.google.com/dataproc/docs/concepts/connectors/cloud-storage),
rename it to **gcs-connector-hadoop.jar** and move it in the docker folder. Or use the following command while inside this folder.
```shell
curl -LfO 'https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar'
mv gcs-connector-hadoop3-latest.jar docker/gcs-connector-hadoop.jar
```
We've downloaded the "Cloud Storage connector for Hadoop 3.x", which is the latest at the moment of writing.

### Start Airflow
Make sure to create a local folder called **data** in case you would want to save the files in your local system.
```shell
mkdir data
```

Create airflow database
```shell
docker compose up airflow-init
```

You can now initialize airflow cluster
```shell
docker compose up
```

Once ready, you can connect to your [local airflow dashboard](http://localhost:8080/), using the credentials for your cluster, in case you did not modify them, you can use the default ones:
- Username: airflow
- Password: airflow

Before unpausing the dags, make sure your connections and variables are ready.

In Admin > Connections create 3 connections of Connection Type **HTTP** named
- [news_api](https://newsapi.org/)
- [tmdb_api](https://developer.themoviedb.org/reference/intro/getting-started)
- [sports_api](https://www.api-football.com/documentation-v3#section/Authentication)

In password you have to insert your api key, discover more how to request them by clicking on the links above

In Admin > Variables create 2 variables:
- is_gcs_enabled: with value True, if you want to upload to Google cloud, and False if you want to work with your local system
- gcs_bucket_path: in case the previous variable was set to True, specify your bucket url. Ex. **gs://letstalk_landing_zone_bdma**

You can now unpause the DAGs and the ingestion will automatically start
