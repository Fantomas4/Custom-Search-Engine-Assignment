from pymongo import MongoClient


class ConnectToMongo:

    def __init__(self, username, password, ip, database):
        self.database = database
        self.client = MongoClient(ip, username=username, password=password, authSource="admin")[database]
        self.crawler_db = self.client.crawler_records
        self.indexer_db = self.client.index

    def add_crawler_record(self, json):
        self.crawler_db.insert_one(json)

    def find_all_crawler_records(self):
        return self.crawler_db.find({})

    def crawler_record_exists(self, title, url):
        return self.crawler_db.find_one({"title": title, "url": url}) is not None

    def add_index_entry(self, json):
        self.indexer_db.insert_one(json)

    def update_index_entry(self, word, new_data):
        entry = self.indexer_db.find_one({"word": word})
        w_freq = entry["w_freq"] + 1
        documents = entry["documents"]
        documents.append(new_data)
        self.indexer_db.update({"word": word},
                               {"$set": {"w_freq": w_freq, "documents": documents}})

    def index_entry_exists(self, word):
        return self.indexer_db.find_one({"word": word}) is not None

    def drop(self):
        self.crawler_db.drop()
        self.indexer_db.drop()
        self.crawler_db = self.client.crawler_records
        self.indexer_db = self.client.index


