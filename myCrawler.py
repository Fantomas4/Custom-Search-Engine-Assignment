#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import nltk
from urllib import request
from bs4 import BeautifulSoup
import re
import threading
from nltk.stem.wordnet import WordNetLemmatizer
from collections import Counter
import time
import sys


def crawler(startingUrl, size, numberOfThreads, append=True):
    print("Downloading natural language packages...")
    nltk.download('punkt')
    nltk.download('stopwords')
    nltk.download('wordnet')
    stop_words = set(nltk.corpus.stopwords.words("english"))
    bagPerPage = []
    head = [startingUrl]
    headLocker, bagLocker = threading.Lock(), threading.Lock()
    threads = []
    print("Start crawling...")
    while len(bagPerPage) < size:
        while len(head) == 0 and sum([1 for t in threads if t.is_alive()]) > 0:  # there are not avaliable references
            time.sleep(1)
            continue
        if len(head) == 0:
            break
        t = threading.Thread(target=getUrl, args=(
            head.pop(0), bagPerPage, bagLocker, head, headLocker, stop_words))
        t.start()
        threads.append(t)
        while True:
            active = 0
            for thread in threads:
                if thread.isAlive():
                    active += 1
            if active < numberOfThreads:
                break
            else:
                time.sleep(0.5)

    [t.join() for t in threads]  # wait for all the threads to end

    # This will right the final results to the file
    if not append:
        file = open("crawler.txt", "w")
    else:
        file = open("crawler.txt", "a+")
    for bag in bagPerPage[:size]:
        file.write(bag["url"] + " " + bag["title"] + " ")
        for element in bag["bag"].keys():
            file.write(element + "-" + str(bag["bag"][element]) + ",")
        file.write("\n")
    file.close()
    return bagPerPage


def getUrl(url: str, bagPerPage: list, bagLocker, head: list, headLocker,
           stop_words: list):
    try:  # check if reference is valid
        html = request.urlopen(url).read().decode('utf8')
        raw = BeautifulSoup(html, 'html.parser')
        title = raw.title.string
        for bag in bagPerPage:  # check if already have been visited
            if title == bag["title"] and url == bag["url"]:
                return
    except:
        return
    try:
        # find the references from this page
        newReferences = []
        for link in raw.findAll('a'):
            newReferences.append(link.get('href'))
        # add the references to the head
        while headLocker.locked():
            continue
        headLocker.acquire()
        head += newReferences
        headLocker.release()
        # Preprocess the raw text
        rx = re.compile("[^\W\d_]+")  # regex for words
        tokens = nltk.word_tokenize(raw.get_text())
        allWords = [word for word in tokens if word not in '!"#$%&\'()*+,-./:;<=>?@[\\]^_`{|}~']
        allWords = [i[0] for i in [rx.findall(i) for i in list(allWords)] if len(i) > 0]
        allWords = [i for i in list(allWords) if not i.startswith("wg")]
        # remove the stop words in the allWords
        filteredWords = []
        for w in allWords:
            if w not in stop_words:
                filteredWords.append(w)
        # Lemmatization all words to the root word
        lem = WordNetLemmatizer()
        lemmedWords = [lem.lemmatize(word) for word in filteredWords]

        # adding findings to the bagPerPage
        while bagLocker.locked():
            continue
        bagLocker.acquire()
        bagPerPage.append({"url": url, "title": title, "bag": Counter(lemmedWords)})
        bagLocker.release()

    except:  # something went wrong during this phase, so we will not have any results
        return


if __name__ == "__main__":
    startingUrl = str(sys.argv[1])
    size = int(sys.argv[2])
    willAppend = int(sys.argv[3])
    threads = int(sys.argv[4])
    if willAppend == 0:
        crawler(startingUrl, size, threads, False)
    else:
        crawler(startingUrl, size, threads)

# url = "https://en.wikipedia.org/wiki/Apage"
# print(crawler(url, 2, 8, False))
