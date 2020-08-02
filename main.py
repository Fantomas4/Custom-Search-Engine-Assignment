#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys

from dotenv import load_dotenv

from connect_to_mongo import ConnectToMongo
from crawler import Crawler
from indexer import Indexer

if __name__ == "__main__":
    load_dotenv()  # load enviromental variables from .env
    username = os.getenv("MONGO_INITDB_ROOT_USERNAME")
    password = os.getenv("MONGO_INITDB_ROOT_PASSWORD")
    database = os.getenv("MONGO_INITDB_DATABASE")
    ip = os.getenv("MONGO_IP")
    mongo_connection = ConnectToMongo(username=username, password=password, database=database,
                                      ip=ip)  # connecting to mongo
    startingUrl = str(sys.argv[1])  # get variables from commandline
    size = int(sys.argv[2])
    willAppend = int(sys.argv[3])
    threads = int(sys.argv[4])
    if willAppend == 0:
        crawler = Crawler(startingUrl=startingUrl, mongo_connection=mongo_connection, append=False, size=size,
                          numberOfThreads=threads)
    else:
        crawler = Crawler(startingUrl=startingUrl, mongo_connection=mongo_connection, append=True, size=size,
                          numberOfThreads=threads)
    crawler.crawl()

    # test
    indexer = Indexer(mongo_connection)

