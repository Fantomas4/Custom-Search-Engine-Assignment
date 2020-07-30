from pymongo import MongoClient


class Connect_to_mongo:

    def __init__(self, username, password, ip, database):
        self.database = database
        self.client = MongoClient(ip, username=username, password=password, authSource="admin")[database]
        self.crawler_db = self.client.crawler_records
        self.indexer_db = self.client.index

    def add_crawler_record(self, json):
        self.crawler_db.insert_one(json)
        # test only!
        self.indexer_db.insert_one(json)

    def find_all_crawler_records(self):
        return self.crawler_db.find({})

    def drop(self):
        self.crawler_db.drop()
        self.indexer_db.drop()
        self.crawler_db = self.client.crawler_records
        self.indexer_db = self.client.index

    def crawler_record_exists(self, title, url):
        return self.crawler_db.find_one({"title": title, "url": url}) != None
