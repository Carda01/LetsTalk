from pinecone import Pinecone
from pyspark.sql.functions import coalesce
from pyspark.sql.functions import col, lit


def prepare_data(data, key_col, date_col, text_to_embed_cols):
    data_to_upsert = data.drop(date_col) \
        .withColumnRenamed(key_col, "_id") \
        .withColumn("text_to_embed", coalesce(*text_to_embed_cols, lit(""))) \
        .filter(col("text_to_embed") != "")

    data_registry = data.select(key_col, date_col)

    return data_to_upsert, data_registry

class PineconeManager:

    def __init__(self, index_name, namespace, pinecone_api_key):
        self.index_name = index_name
        self.namespace = namespace
        self.pinecone_api_key = pinecone_api_key
        self.batch_size = 95
        self.pc = Pinecone(api_key=self.pinecone_api_key)
        self.index = self.pc.Index(self.index_name)


    def process_partition(self, partition):
        from pinecone import Pinecone
        pc = Pinecone(api_key=self.pinecone_api_key)
        index = pc.Index(self.index_name)
        buffer = []

        for row in partition:
            data = row.asDict()
            entry = {k: v for k, v in data.items() if v is not None}

            buffer.append(entry)

            if len(buffer) >= self.batch_size:
                index.upsert_records(self.namespace, buffer)
                buffer.clear()

        if buffer:
            index.upsert_records(self.namespace, buffer)


    def reset_index(self):
        self.index.delete(delete_all=True, namespace=self.namespace)


    def print_stats(self):
        print(self.index.describe_index_stats())


    def query(self, text, num_results = 10):
        return self.index.search(namespace=self.namespace,
                     query={
                         "top_k": num_results,
                         "inputs": {
                             'text': text
                         }
                     })

