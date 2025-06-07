from pinecone import Pinecone
from pyspark.sql.functions import coalesce
from pyspark.sql.functions import col, lit
from functools import partial


def prepare_data(data, key_col, date_col, text_to_embed_cols, source):
    data_to_upsert = data \
        .withColumnRenamed(key_col, "_id") \
        .withColumn("text_to_embed", coalesce(*text_to_embed_cols, lit(""))) \
        .filter(col("text_to_embed") != "") \
        .withColumn("source", lit(source)) \
        .select('_id', 'text_to_embed', 'source')

    data_registry = None
    if date_col in data.columns:
        data_registry = data.select(key_col, date_col)

    return data_to_upsert, data_registry


def _process_partition(partition, api_key, index_name, namespace, batch_size):
    from pinecone import Pinecone
    pc = Pinecone(api_key=api_key)
    index = pc.Index(index_name)
    buffer = []

    def safe_upsert(records):
        try:
            index.upsert_records(namespace, records)
        except Exception as e:
            print(f"Failed to upsert batch")
            for rec in records:
                try:
                    index.upsert_records(namespace, [rec])
                except Exception as single_e:
                    print(f" -> Single record failed: record={rec}, error={single_e!r}")

            return


    for row in partition:
        data = row.asDict()
        entry = {k: v for k, v in data.items() if v is not None}

        buffer.append(entry)

        if len(buffer) >= batch_size:
            safe_upsert(buffer)
            buffer.clear()

    if buffer:
        safe_upsert(buffer)


class PineconeManager:

    def __init__(self, index_name, namespace, pinecone_api_key):
        self.index_name = index_name
        self.namespace = namespace
        self.pinecone_api_key = pinecone_api_key
        self.batch_size = 95
        self.pc = Pinecone(api_key=self.pinecone_api_key)
        self.index = self.pc.Index(self.index_name)

    def get_pinecone_loader(self):
        return partial(
            _process_partition,
            api_key=self.pinecone_api_key,
            index_name=self.index_name,
            namespace=self.namespace,
            batch_size=self.batch_size,
        )


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

