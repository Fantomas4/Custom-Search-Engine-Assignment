import math


class QueryHandler:

    def __init__(self, mongo_connection):
        self.mongo_connection = mongo_connection
        # self.query(["Apage", "Wikipedia", "From"], 3)

    def query(self, query_keywords, top_k):
        print("> Executing query...")
        self.docs_score = {}
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
                    # Check if a score accumulator exists in docs_score dictionary for the current document
                    doc_id = document["_id"]
                    if doc_id not in self.docs_score.keys():
                        # If a score accumulator for the current document does not exist, initialize one.
                        self.docs_score[doc_id] = 0

                    term_freq = 1 + math.log(document["w_d_freq"])
                    self.docs_score[doc_id] = self.docs_score[doc_id] + term_freq * inv_term_d_freq

        for doc_id in self.docs_score.keys():
            doc_length = self.mongo_connection.find_document_record(doc_id)["length"]
            self.docs_score[doc_id] = self.docs_score[doc_id] / doc_length

        # Sort the final document scores in descending order
        self.docs_score = {k: v for k, v in sorted(self.docs_score.items(), key=lambda x: x[1], reverse=True)}

        # Generate the result documents list
        query_results = []
        k = 0
        for doc_id in self.docs_score.keys():
            k += 1

            document = self.mongo_connection.find_document_record(doc_id)
            query_results.append({"title": document["title"], "url": document["url"]})

            if k == top_k:
                break

        # test only!
        print("*** DOCS SCORE *** \n")
        for res in self.docs_score.keys():
            print(str(res) + " : " + str(self.docs_score[res]), "\n")

        print("\n*** Query Results for top-k: ***\n")
        for res in query_results:
            print(res, "\n")

        print("> Query execution finished!")
        return query_results

