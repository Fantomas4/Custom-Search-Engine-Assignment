import time

from connect_to_mongo import ConnectToMongo


class Indexer:

    def __init__(self, mongo_connection: ConnectToMongo):
        self.doc_lengths = {}

        self.mongo_connection = mongo_connection
        self.build_index()
        self.calculate_doc_length()

    def build_index(self):
        tic = time.perf_counter()
        for document in self.mongo_connection.find_all_crawler_records():
            doc_id = document["_id"]
            url = document["url"]
            title = document["title"]
            bag = document["bag"]

            # For every document, add an empty entry to "doc_length" dictionary, initializing length with -1
            self.doc_lengths[doc_id] = -1

            for word in bag:
                # print(word + " " + str(bag[word]))

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
        toc = time.perf_counter()
        print("Index builder execution time: " + "{:.2f}".format(toc - tic) + " secs")

    # Given a document id, searches for the word-document frequency on a given term's document list.
    def search_w_d_freq(self, doc_id, doc_list):
        for document in doc_list:
            if document["_id"] == doc_id:
                return document["w_d_freq"]
        # If no w_d_freq data was found, the document is not present in the target term's
        # document list, so we return None.
        return None

    def calculate_doc_length(self):
        # print(self.mongo_connection.find_all_index_entries())
        for doc_id in self.doc_lengths.keys():
            for term_record in self.mongo_connection.find_all_index_entries():
                w_d_freq = self.search_w_d_freq(doc_id, term_record["documents"])
                w_freq = term_record["w_freq"]