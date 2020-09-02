import math
import os
import threading
import time

from dotenv import load_dotenv
from mongodb import MongoDB


class QueryHandler:

    def connect_to_db(self):
        load_dotenv()  # load enviromental variables from .env
        username = os.getenv("MONGO_INITDB_ROOT_USERNAME")
        password = os.getenv("MONGO_INITDB_ROOT_PASSWORD")
        database = os.getenv("MONGO_INITDB_DATABASE")
        ip = os.getenv("MONGO_IP")

        # return MongoDB connection object
        return MongoDB(username=username, password=password, database=database, ip=ip)

    def __init__(self, threads_num):
        self.threads_num = threads_num
        self.thread_pool = []
        self.mongo_connection = self.connect_to_db()

        self.docs_score = {}
        self.docs_score_locker = threading.Lock()

    def query(self, query_keywords, top_k):
        print("> Executing query...")
        tic = time.perf_counter()

        top_k = int(top_k)

        # At the start of a new query, clear the documents score dictionary
        self.docs_score.clear()
        docs_count = self.mongo_connection.get_documents_count()

        # Convert all query keywords to lowercase
        query_keywords = [keyword.lower() for keyword in query_keywords]

        print("--- QUERY DIAG ---START")
        print("query_keywords: ", query_keywords)
        print("top_k: ", top_k)
        print("--- QUERY DIAG ---END")

        for word in query_keywords:
            retrieved_word = self.mongo_connection.find_index_entry(word)
            if retrieved_word is not None:
                w_freq = retrieved_word["w_freq"]
                inv_term_d_freq = math.log(1 + (docs_count / w_freq))

                for document in self.mongo_connection.find_index_entry(word)["documents"]:
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

                    new_task = threading.Thread(target=self.calculate_doc_score, args=(document, inv_term_d_freq, ))
                    new_task.start()
                    self.thread_pool.append(new_task)

        # Wait until all threads in the thread poll have finished
        for thread in self.thread_pool:
            while thread.isAlive():
                time.sleep(0.5)

        for doc_id in self.docs_score.keys():
            doc_length = self.mongo_connection.find_document_record(doc_id)["length"]
            self.docs_score[doc_id] = self.docs_score[doc_id] / doc_length

        # Sort the final document scores in descending order
        docs_score = {k: v for k, v in sorted(self.docs_score.items(), key=lambda x: x[1], reverse=True)}

        # Generate the result documents list
        query_results = []
        k = 0
        for doc_id in docs_score.keys():
            k += 1

            document = self.mongo_connection.find_document_record(doc_id)
            query_results.append({"title": document["title"], "url": document["url"]})

            if k == top_k:
                break

        # test only!
        print("*** DOCS SCORE *** \n")
        for res in docs_score.keys():
            print(str(res) + " : " + str(docs_score[res]), "\n")

        print("\n*** Query Results for top-k: ***\n")
        for res in query_results:
            print(res, "\n")

        toc = time.perf_counter()
        print("> Query execution time: " + "{:.2f}".format(toc - tic) + " secs")
        print("> Query execution finished!")
        return query_results

    def calculate_doc_score(self, document, inv_term_d_freq):
        # Check if a score accumulator exists in docs_score dictionary for the current document
        doc_id = document["_id"]
        if doc_id not in self.docs_score.keys():
            # If a score accumulator for the current document does not exist, initialize one.
            with self.docs_score_locker:
                self.docs_score[doc_id] = 0

        term_freq = 1 + math.log(document["w_d_freq"])

        with self.docs_score_locker:
            self.docs_score[doc_id] = self.docs_score[doc_id] + term_freq * inv_term_d_freq
