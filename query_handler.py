
class QueryHandler:

    def __init__(self, mongo_connection):
        self.mongo_connection = mongo_connection
        self.docs_score = {}

    def query(self, keywords, top_k):
        for document in self.mongo_connection.find_all_document_records():



