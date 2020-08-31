from pymongo import MongoClient


class MongoDB:

    def __init__(self, username, password, ip, database):
        self.database = database
        self.client = MongoClient(ip, username=username, password=password, authSource="admin")[database]
        self.crawler_db = self.client.crawler_records
        self.documents_db = self.client.documents
        self.indexer_db = self.client.index

    def add_crawler_record(self, json):
        self.crawler_db.insert_one(json)

    def find_all_crawler_records(self):
        return self.crawler_db.find({})

    def crawler_record_exists(self, title, url):
        return self.crawler_db.find_one({"title": title, "url": url}) is not None

    def build_documents_db(self):
        self.documents_db.insert_many(self.crawler_db.find({}))

    def get_documents_count(self):
        return self.documents_db.count()

    def find_document_record(self, doc_id):
        return self.documents_db.find_one({"_id": doc_id})

    def find_all_document_records(self):
        return self.documents_db.find({})

    def add_length_to_document(self, doc_id, doc_length):
        self.documents_db.update({"_id": doc_id}, {"$set": {"length": doc_length}})

    def add_index_entry(self, json):
        self.indexer_db.insert_one(json)

    def update_index_entry(self, word, new_data):
        entry = self.indexer_db.find_one({"word": word})
        w_freq = entry["w_freq"] + 1
        documents = entry["documents"]
        documents.append(new_data)
        self.indexer_db.update({"word": word},
                               {"$set": {"w_freq": w_freq, "documents": documents}})

    def find_index_entry(self, term):
        return self.indexer_db.find_one({"word": term})

    def find_all_index_entries(self):
        return self.indexer_db.find({})

    def index_entry_exists(self, word):
        return self.indexer_db.find_one({"word": word}) is not None

    def reset_crawler(self):
        self.crawler_db.drop()
        self.crawler_db = self.client.crawler_records

    def reset_index(self):
        self.indexer_db.drop()
        self.documents_db.drop()
        self.indexer_db = self.client.index
        self.documents_db = self.client.documents


