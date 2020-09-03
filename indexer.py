import math
import threading
import time

from mongodb import MongoDB


class Indexer:

    def __init__(self, mongo_connection: MongoDB, threads_num=4):
        self.docs_count = 0
        self.doc_lengths = {}

        self.mongo_connection = mongo_connection
        self.threads_num = threads_num

        self.thread_pool = []

    def build_index(self):
        print("> Building index...")

        tic = time.perf_counter()

        # Reset index related database collections
        self.mongo_connection.reset_index()

        # Copy all records from "Crawler Records" db collection to "Documents" db collection.
        self.mongo_connection.build_documents_db()

        # Get the documents total count
        self.docs_count = self.mongo_connection.get_documents_count()
        print("DIAG: docs_count: ", self.docs_count)

        for document in self.mongo_connection.find_all_document_records():
            bag = document["bag"]
            for word in bag:
                while True:  # it will stay here till a thread is available
                    active = 0
                    for thread in self.thread_pool:
                        if thread.isAlive():
                            active += 1
                    if active < self.threads_num:
                        break
                    else:
                        time.sleep(0.5)

                new_task = threading.Thread(target=self.process_word, args=(document, word, ))
                new_task.start()
                self.thread_pool.append(new_task)

        # Wait until all threads in the thread poll have finished
        for thread in self.thread_pool:
            while thread.isAlive():
                time.sleep(0.5)

        toc = time.perf_counter()
        print("> Index builder execution time: " + "{:.2f}".format(toc - tic) + " secs")
        print("> Index Building complete!")

    def process_word(self, document, word):
        doc_id = document["_id"]
        url = document["url"]
        title = document["title"]
        bag = document["bag"]

        # Check if the word already exists in the inverted index
        if self.mongo_connection.index_entry_exists(word):
            # If the word exists in the index, update the word entry's data
            self.mongo_connection.update_index_entry(word, {"_id": doc_id,
                                                            "title": title,
                                                            "url": url,
                                                            "w_d_freq": bag[word]
                                                            })
        else:
            # If the word does not exist in the index, create a new entry for it
            # ATTENTION: word must be converted to LOWERCASE for the purposes of querying.
            self.mongo_connection.add_index_entry({"word": word.lower(),
                                                   "w_freq": 1,
                                                   "documents": [{"_id": doc_id,
                                                                  "title": title,
                                                                  "url": url,
                                                                  "w_d_freq": bag[word]}]
                                                   })

