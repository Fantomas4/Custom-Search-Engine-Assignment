#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys

from dotenv import load_dotenv

from mongodb import MongoDB
from crawler import Crawler
from indexer import Indexer
from query_handler import QueryHandler


class SearchEngine:

    def __init__(self):
        load_dotenv()  # load enviromental variables from .env
        username = os.getenv("MONGO_INITDB_ROOT_USERNAME")
        password = os.getenv("MONGO_INITDB_ROOT_PASSWORD")
        database = os.getenv("MONGO_INITDB_DATABASE")
        ip = os.getenv("MONGO_IP")
        self.mongo_connection = MongoDB(username=username, password=password, database=database,
                                   ip=ip)  # connecting to mongo

        starting_url = str(sys.argv[1])  # get variables from commandline
        size = int(sys.argv[2])
        will_append = int(sys.argv[3])
        threads = int(sys.argv[4])
        if will_append == 0:
            self.crawler = Crawler(starting_url=starting_url, append=False, size=size,
                                   threads_num=threads)
        else:
            self.crawler = Crawler(starting_url=starting_url, append=True, size=size,
                                   threads_num=threads)
        self.crawler.crawl()

        self.indexer = Indexer(self.mongo_connection)
        self.query_handler = QueryHandler(self.mongo_connection)
