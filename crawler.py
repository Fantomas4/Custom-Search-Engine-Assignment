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
import sys


class Crawler:

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

        self.mongo_connection = MongoDB.connect_to_db()
        if not append:
            self.mongo_connection.reset_crawler()

        self.indexer = Indexer(self.threads_num)

    def crawl(self):
        print("> Started crawling...")
        tic = time.perf_counter()

        while self.global_counter < self.size:
            # Wait while there are no new references
            while len(self.head) == 0 and sum([1 for t in self.threads if t.is_alive()]) > 0:
                time.sleep(1)
                continue
            if len(self.head) == 0:
                break
            next_url = str(self.head.pop(0))
            t = threading.Thread(target=self.parse_url, args=(next_url))
            t.start()
            self.threads.append(t)
            while True:  # Wait until a thread is available
                active = 0
                for thread in self.threads:
                    if thread.isAlive():
                        active += 1
                if active < self.threads_num:
                    break
                else:
                    time.sleep(0.5)

        toc = time.perf_counter()
        print("> Crawler execution time: " + "{:.2f}".format(toc - tic) + " secs")
        print("> Crawling finished!")

        # Call Indexer to build index
        self.indexer.build_index()

    def parse_url(self, *url_chars):
        url = "".join(url_chars)
        try:  # check if the reference is valid
            html = request.urlopen(url).read().decode('utf8')
            raw = BeautifulSoup(html, 'html.parser')
            title = raw.title.string
        except Exception:
            return

        try:
            new_references = []
            for link in raw.findAll('a'):  # Find all new references to other pages (urls) from this page
                new_references.append(link.get('href'))
            with self.headLocker:  # Add the references to the head
                self.head += new_references

            # If a record with the same page title and url already exists in MongoDB,
            # skip parsing this page's contents.
            if self.mongo_connection.crawler_record_exists(title, url):
                return

            # Preprocess the raw text
            rx = re.compile("[^\W\d_]+")  # regex for words
            tokens = nltk.word_tokenize(raw.get_text())
            all_words = [word for word in tokens if word not in '!"#$%&\'()*+,-./:;<=>?@[\\]^_`{|}~']
            all_words = [i[0] for i in [rx.findall(i) for i in list(all_words)] if len(i) > 0]
            all_words = [i for i in list(all_words) if not i.startswith("wg")]

            # Remove the stop words from all_words
            filtered_words = []
            for w in all_words:
                if w not in self.stop_words:
                    filtered_words.append(w)
            lem = WordNetLemmatizer()  # Lemmatize all words to their root
            lemmed_words = [lem.lemmatize(word) for word in filtered_words]
            lowercase_words = [word.lower() for word in lemmed_words]  # Convert all words to lowercase

            with self.count_locker:  # Save the page information to the Database as a new document
                if self.global_counter < self.size:
                    self.mongo_connection.add_crawler_record({"url": url, "title": title, "bag": Counter(lowercase_words)})
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
