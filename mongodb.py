from pymongo import MongoClient
from dotenv import load_dotenv
import os


class MongoDB:

    def __init__(self, username, password, ip, database):
        self.database = database
        self.client = MongoClient(ip, username=username, password=password, authSource="admin")[database]
        self.crawler_db = self.client.crawler_records
        self.documents_db = self.client.documents
        self.indexer_db = self.client.index
        self.query_documents_db = self.client.query_documents
        self.query_indexer_db = self.client.query_index

    @staticmethod
    def connect_to_db():
        load_dotenv()  # load enviromental variables from .env
        username = os.getenv("MONGO_INITDB_ROOT_USERNAME")
        password = os.getenv("MONGO_INITDB_ROOT_PASSWORD")
        database = os.getenv("MONGO_INITDB_DATABASE")
        ip = os.getenv("MONGO_IP")

        # return MongoDB connection object
        return MongoDB(username=username, password=password, database=database, ip=ip)

    # ------------------------------------ Crawler-related methods ------------------------------------
    def add_crawler_record(self, json):
        self.crawler_db.insert_one(json)

    def find_all_crawler_records(self):
        return self.crawler_db.find({}, no_cursor_timeout=True)

    def crawler_record_exists(self, title, url):
        return self.crawler_db.find_one({"title": title, "url": url}) is not None

    def reset_crawler(self):
        self.crawler_db.drop()
        self.crawler_db = self.client.crawler_records

    # ------------------------------------ Indexer-related methods ------------------------------------
    def build_documents_db(self):
        self.documents_db.insert_many(self.crawler_db.find({}))

    def get_documents_count(self):
        return self.documents_db.count()

    def find_document_record(self, doc_id):
        return self.documents_db.find_one({"_id": doc_id})

    def find_all_document_record_ids(self):
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

    def reset_index(self):
        self.indexer_db.drop()
        self.documents_db.drop()
        self.indexer_db = self.client.index
        self.documents_db = self.client.documents

    # ------------------------------------ Query Handler-related methods ------------------------------------
    def reset_query_handler(self):
        self.query_documents_db.drop()
        self.query_indexer_db.drop()
        self.query_documents_db = self.client.query_documents
        self.query_indexer_db = self.client.query_index

    def build_query_documents_db(self):
        self.query_documents_db.insert_many(self.documents_db.find({}))

    def build_query_indexer_db(self):
        self.query_indexer_db.insert_many(self.indexer_db.find({}))

    def update_query_handler_db(self):
        # Reset query handler-related database collections
        self.reset_query_handler()

        # Copy all records from "documents" db collection to "query_documents" db collection.
        self.build_query_documents_db()

        # Copy all records from "indexer" db collection to "query_indexer" db collection.
        self.build_query_indexer_db()

    # Used to determine the status of the Query Database collections. If the Index has finished initializing
    # for the first time, the status returned is always True, since the Query collections always contain the
    # copy of the last completed Index. If the first index build is in progress, the status returned if False,
    # since the Query collections have not yet been initialized.
    def is_initialized(self):
        collections = self.client.list_collection_names()
        if "query_documents" in collections and "query_index" in collections:
            return True
        else:
            return False

    def get_query_documents_count(self):
        return self.query_documents_db.count()

    def find_query_index_entry(self, term):
        return self.query_indexer_db.find_one({"word": term})

    def find_query_document_record(self, doc_id):
        return self.query_documents_db.find_one({"_id": doc_id})




