# Let's Talk P1
## Description 
Our project consists in a pipeline orchestrated with Airflow, that fetches data from different APIs, and uploads them into a Google Cloud bucket, using Delta Lake format.

## Reproducibility
Once you've cloned the repository. 

Set up Google Cloud, we recommend you to follow this [guide](https://delta.io/blog/delta-lake-gcp/),
which will explain you how to set up a bucket, create a service account and once you have done so you can
download the key in json format rename it to **gcs.json** and move it inside this folder. 

Also download the cloud storage connector from
[here](https://cloud.google.com/dataproc/docs/concepts/connectors/cloud-storage),
rename it to **gcs-connector-hadoop.jar** and move it in this folder. Or use the following command while inside this folder.
```shell
curl -LfO 'https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar'
mv gcs-connector-hadoop3-latest.jar gcs-connector-hadoop.jar
```
We've downloaded the "Cloud Storage connector for Hadoop 3.x", which is the latest at the moment of writing.

Create airflow database
```shell
docker compose up airflow-init
```

You can now initialize airflow cluster
```shell
docker compose up
```
