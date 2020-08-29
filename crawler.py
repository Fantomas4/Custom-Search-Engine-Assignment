import re
import threading
import time
from collections import Counter
from urllib import request
import nltk
from bs4 import BeautifulSoup
from nltk.stem.wordnet import WordNetLemmatizer

from indexer import Indexer
from mongodb import MongoDB

import os
import sys
from dotenv import load_dotenv


class Crawler:

    def connect_to_db(self):
        load_dotenv()  # load enviromental variables from .env
        username = os.getenv("MONGO_INITDB_ROOT_USERNAME")
        password = os.getenv("MONGO_INITDB_ROOT_PASSWORD")
        database = os.getenv("MONGO_INITDB_DATABASE")
        ip = os.getenv("MONGO_IP")

        # return MongoDB connection object
        return MongoDB(username=username, password=password, database=database, ip=ip)

    def __init__(self, starting_url: str, append: bool, size: int, threads_num: int):
        print("> Downloading natural language packages...")
        nltk.download('punkt')
        nltk.download('stopwords')
        nltk.download('wordnet')

        self.stop_words = set(nltk.corpus.stopwords.words("english"))
        self.global_counter = 0
        self.head = [starting_url]
        self.headLocker, self.count_locker = threading.Lock(), threading.Lock()
        self.threads = []
        self.size = size
        self.threads_num = threads_num

        self.mongo_connection = self.connect_to_db()
        if not append:
            self.mongo_connection.reset_crawler()

        self.indexer = Indexer(self.mongo_connection)

    def crawl(self):
        print("> Started crawling...")
        while self.global_counter < self.size:
            while len(self.head) == 0 and sum(
                    [1 for t in self.threads if t.is_alive()]) > 0:  # there are not avaliable references
                time.sleep(1)
                continue
            if len(self.head) == 0:
                break
            next_url = str(self.head.pop(0))
            t = threading.Thread(target=self.parse_url, args=(next_url))
            t.start()
            self.threads.append(t)
            while True:  # it will stay here till a thread is available
                active = 0
                for thread in self.threads:
                    if thread.isAlive():
                        active += 1
                if active < self.threads_num:
                    break
                else:
                    time.sleep(0.5)
        print("> Crawling finished!")

        # Call Indexer to build index
        self.indexer.build_index()

    def parse_url(self, *url_chars):
        url = "".join(url_chars)
        try:  # check if reference is valid
            html = request.urlopen(url).read().decode('utf8')
            raw = BeautifulSoup(html, 'html.parser')
            title = raw.title.string
            if self.mongo_connection.crawler_record_exists(title, url):
                return
        except Exception:
            return
        try:
            new_references = []
            for link in raw.findAll('a'):  # find the references from this page
                new_references.append(link.get('href'))
            with self.headLocker:  # add the references to the head
                self.head += new_references
            # Preprocess the raw text
            rx = re.compile("[^\W\d_]+")  # regex for words
            tokens = nltk.word_tokenize(raw.get_text())
            all_words = [word for word in tokens if word not in '!"#$%&\'()*+,-./:;<=>?@[\\]^_`{|}~']
            all_words = [i[0] for i in [rx.findall(i) for i in list(all_words)] if len(i) > 0]
            all_words = [i for i in list(all_words) if not i.startswith("wg")]
            # remove the stop words in the all_words
            filtered_words = []
            for w in all_words:
                if w not in self.stop_words:
                    filtered_words.append(w)
            lem = WordNetLemmatizer()  # Lemmatization all words to the root word
            lemmed_words = [lem.lemmatize(word) for word in filtered_words]
            with self.count_locker:  # here will be saved to mongo and increase global counter
                if self.global_counter < self.size:
                    self.mongo_connection.add_crawler_record({"url": url, "title": title, "bag": Counter(lemmed_words)})
                    self.global_counter += 1
        except Exception:  # something went wrong during this phase, so we will not have any results
            return


if __name__ == "__main__":
    starting_url = str(sys.argv[1])  # get variables from commandline
    size = int(sys.argv[2])
    will_append = int(sys.argv[3])
    threads = int(sys.argv[4])
    if will_append == 0:
        crawler = Crawler(starting_url=starting_url, append=False, size=size, threads_num=threads)
    else:
        crawler = Crawler(starting_url=starting_url, append=True, size=size, threads_num=threads)

    crawler.crawl()
