Here's a rewritten version of your text with cool Markdown formatting and some minor rephrasing for clarity and engagement:

-----

# Welcome to Our Semantic Project\! 🚀

In this project, we'll embark on a journey to build a robust Knowledge Graph, query it using SPARQL, generate powerful embeddings, cluster our data, and visualize it all in a dashboard. Let's dive in\!

-----

**⚠️ IMPORTANT NOTE:** Ensure you execute all commands from the `sdm` directory.

-----

## 0\. Prerequisites

We need two thing before we start. First download the data and some checkpoints to save time from our dropbox. The link is the following:

https://www.dropbox.com/scl/fi/oei9otbt8sdmv0x1rc7pq/sdm_data.zip?rlkey=230hb7tfknheoycrw60z1icbt&st=fygpjob5&dl=0

Put the folder called data out from this folder,
```
sdm/
data/
```
The rest of the folders are checkpoints to save time, if you follow this document it will be explained when and how can you use them.

Secondly, let's create a conda enviroment to run everything without installing modules on our local machine and activate it, make sure that you are running everything inside the environment.

```bash
conda env create -f environment.yml

```
```bash
conda activate sdm
```

## 1\. Data Ingestion & Triplification 📊

We'll kick things off by fetching data from our project's trusted zone, defining its schema, and converting it into RDF triples. This process can take approximately **15 minutes**.

To initiate, run the following command within your `sdm` folder:

```bash
python create_kg.py 1000 
```

(Feel free to adjust `1000` to your desired number of synthetically generated users.)

**⚡️ Time-Saving Tip:** If you'd like to skip this step and jump straight into exploring the data, we've provided pre-generated triples in a zip file. You'll find it in the .zip folder provided before. Simply place the `abox.ttl` and `tbox.ttl`files found inside the `kg_ttls` folder,  directly into your `docker-import` directory, like so:

```
docker-import/
├── abox.ttl
└── tbox.ttl
```

## 2\. Setting Up Your Knowledge Graph Database (GraphDB) 🐘

Next, let's get our GraphDB database up and running. Open your terminal and execute:

```bash
docker-compose up -d
```

This command will spin up a Docker container running an Ontotext GraphDB image. We'll leverage this to store our triples and harness the inference capabilities of a Knowledge Graph.

Once it's running, access the GraphDB workbench in your browser at: [localhost:7200](https://www.google.com/search?q=http://localhost:7200). 

**Manual Repository Creation:** We'll need to manually create a new repository. Please name it `letstalk` and select `RDFS Optimized` as the Ruleset. While we aimed for full automation, this specific step is best handled manually for now. Moving forward, we'll minimize manual processes wherever possible\!

Now let's run the following command to load your data in the repository.

```bash
python load_kg.py
```
You can play with the data in the GraphDB API!
-----

## 3\. Generating Embeddings 🧠

Now that your data is accessible, let's generate some powerful embeddings\!

First, download your Knowledge Graph data by running:

```bash
python download_kg.py
```

This will generate a TSV file. It is inside sdm/drive-embeddings/data. Please copy that tsv also to the empty data folder that you have inside your main folder sdm/data.

**⏳ Heads Up:** The embedding generation process can be quite lengthy, often taking around **1 hour**.

**⚡️ Time-Saving Tip:** If you used our provided data earlier, you can skip this long step\! Simply copy the folder `kg` of the provided .zip, and skip this step.

Otherwise, follow these steps to leverage GPUs for optimized performance:

1.  Grab the `drive-embeddings` folder.
2.  Drop it into your Google Drive.
3.  Open the notebook within `drive-embeddings` on Google Colab.
4.  Run the notebook, ensuring you utilize the GPU runtime\!

-----
-----

## 4\. Final Setup & Dashboard Launch\! 🚀

Once your `kg` folder is created, move it into the `python-backend/data/` directory.

```
python-backend/
└── data/
    └── kg/
        ├── ... (your kg files)
```

**Congratulations\!** You've completed the setup. Now, let's fire up your cool dashboard\!

You'll need two separate terminal windows for this.

1.  In your first terminal, navigate to the `sdm/my-nextjs-app` directory:

    ```bash
    cd my-nextjs-app
    ```

    Then, run:

    ```bash
    npm run dev
    ```

2.  In your second terminal, go to the `sdm/python-backend` directory:

    ```bash
    cd python-backend
    ```

    And run:

    ```bash
    uvicorn main:app --reload
    ```

Finally, open your web browser and go to [http://localhost:3000](https://www.google.com/search?q=http://localhost:3000) to enjoy your dashboard\! WE RECOMMEND TO USE INCOGNITO MODE\!