import math
import time

from connect_to_mongo import ConnectToMongo


class Indexer:

    def __init__(self, mongo_connection: ConnectToMongo):
        self.docs_count = 0
        self.doc_lengths = {}

        self.mongo_connection = mongo_connection

        self.build_index()

    def build_index(self):
        tic = time.perf_counter()

        # Reset index related database collections
        self.mongo_connection.reset_index()

        # Copy all records from "Crawler Records" db collection to "Documents" db collection.
        self.mongo_connection.build_documents_db()

        # Get the documents total count
        self.docs_count = self.mongo_connection.get_documents_count()
        print("DIAG: docs_count: ", self.docs_count)

        for document in self.mongo_connection.find_all_document_records():
            doc_id = document["_id"]
            url = document["url"]
            title = document["title"]
            bag = document["bag"]

            for word in bag:
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
                    self.mongo_connection.add_index_entry({"word": word,
                                                           "w_freq": 1,
                                                           "documents": [{"_id": doc_id,
                                                                          "title": title,
                                                                          "url": url,
                                                                          "w_d_freq": bag[word]}]
                                                           })

        # Calculate the document lengths of the given document collection, and store them as a new property
        # of each document record.
        self.calculate_doc_lengths()

        toc = time.perf_counter()
        print("Index builder execution time: " + "{:.2f}".format(toc - tic) + " secs")

    # Given a document id, searches for the word-document frequency on a given term's document list.
    def search_w_d_freq(self, doc_id, doc_list):
        for document in doc_list:
            if document["_id"] == doc_id:
                return document["w_d_freq"]
        # If no w_d_freq data was found, the document is not present in the target term's
        # document list, so we return 0.
        return 0

    def calculate_doc_lengths(self):
        for document in self.mongo_connection.find_all_document_records():
            doc_id = document["_id"]

            # Initialize the score accumulator for the current document to 0
            squared_weights_sum = 0

            # Find maximum word-document frequency value to be used in normalization
            max_w_d_freq = 1
            for term_record in self.mongo_connection.find_all_index_entries():
                w_d_freq = self.search_w_d_freq(doc_id, term_record["documents"])
                if w_d_freq > max_w_d_freq:
                    max_w_d_freq = w_d_freq

            print("DIAG: max_w_d_freq is: ", max_w_d_freq)

            for term_record in self.mongo_connection.find_all_index_entries():
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
            print("DIAG: doc_len is: ", doc_len)
            self.doc_lengths[doc_id] = doc_len

        # Add calculated lengths to the corresponding document records.
        self.mongo_connection.add_lengths_to_document_db(self.doc_lengths)


