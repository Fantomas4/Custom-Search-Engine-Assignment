import math
import threading
import time

from mongodb import MongoDB


class QueryHandler:

    def __init__(self, threads_num):
        self.threads_num = threads_num
        self.thread_pool = []
        self.mongo_connection = MongoDB.connect_to_db()

        self.docs_count = 0
        self.docs_score = {}
        self.docs_score_locker = threading.Lock()

    def query(self, query_keywords, top_k):
        print("> Executing query...")
        tic = time.perf_counter()

        top_k = int(top_k)

        # Reset query handler-related database collections
        self.mongo_connection.reset_query_handler()

        # Copy all records from "documents" db collection to "query_documents" db collection.
        self.mongo_connection.build_query_documents_db()

        # Copy all records from "indexer" db collection to "query_indexer" db collection.
        self.mongo_connection.build_query_indexer_db()

        # At the start of a new query, clear the documents score dictionary
        self.docs_score.clear()
        self.docs_count = self.mongo_connection.get_query_documents_count()

        # Calculate the document lengths of the given document collection, and store them as a new property
        # of each document record.
        self.calculate_doc_lengths()

        # Convert all query keywords to lowercase
        query_keywords = [keyword.lower() for keyword in query_keywords]

        for word in query_keywords:
            retrieved_word = self.mongo_connection.find_query_index_entry(word)
            if retrieved_word is not None:
                w_freq = retrieved_word["w_freq"]

                for document in self.mongo_connection.find_query_index_entry(word)["documents"]:
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

                    new_task = threading.Thread(target=self.calculate_doc_score, args=(document, w_freq, ))
                    new_task.start()
                    self.thread_pool.append(new_task)

        # Wait until all threads in the thread poll have finished
        for thread in self.thread_pool:
            while thread.isAlive():
                time.sleep(0.5)

        # Normalize the document scores
        for doc_id in self.docs_score.keys():
            retrieved_doc = self.mongo_connection.find_query_document_record(doc_id)
            if retrieved_doc is not None:
                doc_length = retrieved_doc["length"]
                self.docs_score[doc_id] = self.docs_score[doc_id] / doc_length

        # Sort the final document scores in descending order
        docs_score = {k: v for k, v in sorted(self.docs_score.items(), key=lambda x: x[1], reverse=True)}

        # Generate the result documents list
        query_results = []
        k = 0
        for doc_id in docs_score.keys():
            k += 1

            document = self.mongo_connection.find_query_document_record(doc_id)
            query_results.append({"title": document["title"], "url": document["url"]})

            if k == top_k:
                break

        toc = time.perf_counter()
        print("> Query execution time: " + "{:.2f}".format(toc - tic) + " secs")
        print("> Query execution finished!")
        return query_results

    def calculate_doc_score(self, document, w_freq):
        # Check if a score accumulator exists in docs_score dictionary for the current document
        doc_id = document["_id"]
        if doc_id not in self.docs_score.keys():
            # If a score accumulator for the current document does not exist, initialize one.
            with self.docs_score_locker:
                self.docs_score[doc_id] = 0

        term_freq = 1 + math.log(document["w_d_freq"])
        inv_term_d_freq = math.log(1 + (self.docs_count / w_freq))

        with self.docs_score_locker:
            self.docs_score[doc_id] = self.docs_score[doc_id] + term_freq * inv_term_d_freq

    # Given a document id, searches for the word-document frequency on a given term's document list.
    def search_w_d_freq(self, doc_id, doc_list):
        for document in doc_list:
            if document["_id"] == doc_id:
                return document["w_d_freq"]
        # If no w_d_freq data was found, the document is not present in the target term's
        # document list, so we return 0.
        return 0

    def calculate_doc_lengths(self):
        # Reset thread pool
        self.thread_pool = []

        for document in self.mongo_connection.find_all_query_document_records():
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

    def calculate_doc_length(self, document):
        doc_id = document["_id"]

        # Initialize the score accumulator for the current document to 0
        squared_weights_sum = 0

        # Find maximum word-document frequency value to be used in normalization
        max_w_d_freq = 1
        for term_record in self.mongo_connection.find_all_query_index_entries():
            w_d_freq = self.search_w_d_freq(doc_id, term_record["documents"])
            if w_d_freq > max_w_d_freq:
                max_w_d_freq = w_d_freq

        for term_record in self.mongo_connection.find_all_query_index_entries():
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

        self.mongo_connection.add_length_to_query_document(doc_id, doc_len)

