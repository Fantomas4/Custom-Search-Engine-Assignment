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

        # At the start of a new query, clear the documents score dictionary
        self.docs_score.clear()
        self.docs_count = self.mongo_connection.get_query_documents_count()

        # Convert all query keywords to lowercase
        query_keywords = [keyword.lower() for keyword in query_keywords]

        for word in query_keywords:
            retrieved_word = self.mongo_connection.find_query_index_entry(word)
            if retrieved_word is not None:
                w_freq = retrieved_word["w_freq"]

                for document in retrieved_word["documents"]:
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




