from connect_to_mongo import ConnectToMongo


class Indexer:

    def __init__(self, mongo_connection: ConnectToMongo):
        self.mongo_connection = mongo_connection

        # test
        self.add_document()

    def add_document(self):
        # test
        for document in self.mongo_connection.find_all_crawler_records():
            url = document["url"]
            title = document["title"]
            bag = document["bag"]

            for word in bag:
                # print(word + " " + str(bag[word]))

                # Check if the word already exists in the inverted index
                if self.mongo_connection.index_entry_exists(word):
                    # If the word exists in the index, update the word entry's data
                    self.mongo_connection.update_index_entry(word, {"title": title, "url": url, "w_d_freq": bag[word]})
                else:
                    # If the word does not exist in the index, create a new entry for it
                    self.mongo_connection.add_index_entry({"word": word,
                                                           "w_freq": 1,
                                                           "documents": [{"title": title,
                                                                         "url": url,
                                                                         "w_d_freq": bag[word]}]
                                                           })
