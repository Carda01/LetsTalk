from pinecone import Pinecone



class PineconeManager:

    def __init__(self, index_name, namespace, pinecone_api_key):
        self.index_name = index_name
        self.namespace = namespace
        self.pinecone_api_key = pinecone_api_key
        self.batch_size = 95


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


