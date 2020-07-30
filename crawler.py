import re
import threading
import time
from collections import Counter
from urllib import request
import nltk
from bs4 import BeautifulSoup
from nltk.stem.wordnet import WordNetLemmatizer
from connect_to_mongo import Connect_to_mongo


class Crawler:

    def __init__(self, startingUrl: str, mongo_connection: Connect_to_mongo, append: bool, size: int,
                 numberOfThreads: int):
        print("Downloading natural language packages...")
        nltk.download('punkt')
        nltk.download('stopwords')
        nltk.download('wordnet')
        self.stop_words = set(nltk.corpus.stopwords.words("english"))
        self.global_counter = 0
        self.head = [startingUrl]
        self.headLocker, self.count_locker = threading.Lock(), threading.Lock()
        self.threads = []
        self.size = size
        self.numberOfThreads = numberOfThreads
        self.mongo_connection = mongo_connection
        if not append:
            mongo_connection.drop()

    def crawl(self):
        print("Start crawling...")
        while self.global_counter < self.size:
            while len(self.head) == 0 and sum(
                    [1 for t in self.threads if t.is_alive()]) > 0:  # there are not avaliable references
                time.sleep(1)
                continue
            if len(self.head) == 0:
                break
            next_url = str(self.head.pop(0))
            t = threading.Thread(target=self.parseUrl, args=(next_url))
            t.start()
            self.threads.append(t)
            while True:  # it will stay here till a thread is available
                active = 0
                for thread in self.threads:
                    if thread.isAlive():
                        active += 1
                if active < self.numberOfThreads:
                    break
                else:
                    time.sleep(0.5)
        print("Crawling finished!")

    def parseUrl(self, *url_chars):
        url = "".join(url_chars)
        try:  # check if reference is valid
            html = request.urlopen(url).read().decode('utf8')
            raw = BeautifulSoup(html, 'html.parser')
            title = raw.title.string
            if self.mongo_connection.exists(title, url):
                return
        except Exception:
            return
        try:
            newReferences = []
            for link in raw.findAll('a'):  # find the references from this page
                newReferences.append(link.get('href'))
            with self.headLocker:  # add the references to the head
                self.head += newReferences
            # Preprocess the raw text
            rx = re.compile("[^\W\d_]+")  # regex for words
            tokens = nltk.word_tokenize(raw.get_text())
            allWords = [word for word in tokens if word not in '!"#$%&\'()*+,-./:;<=>?@[\\]^_`{|}~']
            allWords = [i[0] for i in [rx.findall(i) for i in list(allWords)] if len(i) > 0]
            allWords = [i for i in list(allWords) if not i.startswith("wg")]
            # remove the stop words in the allWords
            filteredWords = []
            for w in allWords:
                if w not in self.stop_words:
                    filteredWords.append(w)
            lem = WordNetLemmatizer()  # Lemmatization all words to the root word
            lemmedWords = [lem.lemmatize(word) for word in filteredWords]
            with self.count_locker:  # here will be saved to mongo and increase global counter
                if self.global_counter < self.size:
                    self.mongo_connection.add({"url": url, "title": title, "bag": Counter(lemmedWords)})
                    self.global_counter += 1
        except Exception:  # something went wrong during this phase, so we will not have any results
            return
