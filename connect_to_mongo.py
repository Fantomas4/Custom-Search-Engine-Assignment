from pymongo import MongoClient


class Connect_to_mongo:

    def __init__(self, username, password, ip, database):
        self.database = database
        self.client = MongoClient(ip, username=username, password=password, authSource="admin")[database]
        self.db = self.client.records

    def add(self, json):
        self.db.insert_one(json)

    def find_all(self):
        return self.db.find({})

    def drop(self):
        self.db.drop()
        self.db = self.client.records

    def exists(self, title, url):
        return self.db.find_one({"title": title, "url": url}) != None
