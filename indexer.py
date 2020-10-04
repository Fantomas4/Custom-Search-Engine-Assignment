import math
import threading
import time

from mongodb import MongoDB


class Indexer:

    def __init__(self, threads_num=4):
        self.docs_count = 0
        self.doc_lengths = {}
        self.document_ids = []
        self.index_ids = []

        self.mongo_connection = MongoDB.connect_to_db()
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

        # Get all document IDs from the database
        self.document_ids = self.mongo_connection.find_all_document_record_ids()

        doc_counter = 0  # Keeps count of the amount of documents processed.

        for document_id in self.document_ids:
            doc_counter += 1
            print("> Index Builder: Processing document {counter} of {total}...".format(counter=doc_counter,
                                                                                        total=self.docs_count))

            document = self.mongo_connection.find_document_record(document_id)
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
        print("> Index building complete!")

        # Calculate the document lengths of the given document collection, and store them as a new property
        # of each document record.
        print("> Calculating document lengths...")
        tic = time.perf_counter()
        self.calculate_doc_lengths()

        # Wait until all threads in the thread poll have finished
        for thread in self.thread_pool:
            while thread.isAlive():
                time.sleep(0.5)

        toc = time.perf_counter()
        print("> Document lengths calculation execution time: " + "{:.2f}".format(toc - tic) + " secs")
        print("> Document lengths calculation complete!")

        # Update Query Handler's DB collections with the new index and document data.
        print("> Updating Query Handler's DB collections using Index DB collections...")
        self.mongo_connection.update_query_handler_db()
        print("> Query Handler's DB collections update complete!")

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

    def calculate_doc_lengths(self):
        # Reset thread pool
        self.thread_pool = []

        # Get all index entry IDs
        self.index_ids = self.mongo_connection.find_all_index_entry_ids()

        doc_counter = 0  # Keeps count of the amount of documents processed.

        for document_id in self.document_ids:
            doc_counter += 1
            print("> Document lengths calculation: Processing document {counter} of {total}...".format(
                counter=doc_counter, total=self.docs_count))

            document = self.mongo_connection.find_document_record(document_id)
            # Wait until thread pool has an available thread
            while True:
                active = 0
                for thread in self.thread_pool:
                    if thread.isAlive():
                        active += 1
                if active < self.threads_num:
                    break
                else:
                    time.sleep(0.5)

            new_task = threading.Thread(target=self.calculate_doc_length, args=(document,))
            new_task.start()
            self.thread_pool.append(new_task)

    # Given a document id, searches for the word-document frequency on a given term's document list.
    def search_w_d_freq(self, doc_id, doc_list):
        for document in doc_list:
            if document["_id"] == doc_id:
                return document["w_d_freq"]
        # If no w_d_freq data was found, the document is not present in the target term's
        # document list, so we return 0.
        return 0

    def calculate_doc_length(self, document):
        doc_id = document["_id"]

        # Initialize the score accumulator for the current document to 0
        squared_weights_sum = 0

        # Find maximum word-document frequency value to be used in normalization
        max_w_d_freq = 1
        for index_id in self.index_ids:
            term_record = self.mongo_connection.find_index_entry(index_id)
            w_d_freq = self.search_w_d_freq(doc_id, term_record["documents"])
            if w_d_freq > max_w_d_freq:
                max_w_d_freq = w_d_freq

        for index_id in self.index_ids:
            term_record = self.mongo_connection.find_index_entry(index_id)
            w_d_freq = self.search_w_d_freq(doc_id, term_record["documents"])
            w_freq = term_record["w_freq"]

            norm_w_d_freq = w_d_freq / max_w_d_freq
            if self.docs_count > 1:
                norm_inv_d_freq = math.log(self.docs_count / w_freq) / math.log(self.docs_count)
            else:
                # If the document collections consists of just 1 document, set inverted document frequency
                # variable to 1 (thus ignoring inverted document frequency scoring)
                norm_inv_d_freq = 1

            squared_weights_sum += math.pow(norm_w_d_freq * norm_inv_d_freq, 2)

        # Calculate current document's length and add the document:length pair to the doc_lengths dictionary.
        doc_len = math.sqrt(squared_weights_sum)

        self.mongo_connection.add_length_to_document(doc_id, doc_len)
