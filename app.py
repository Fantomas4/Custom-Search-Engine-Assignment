import sys

from flask import Flask, render_template, request

from mongodb import MongoDB
from query_handler import QueryHandler

flask = Flask(__name__)
mongo_connection = MongoDB.connect_to_db()


@flask.route("/", methods=['POST', 'GET'])
def index():
    # Check whether the Index has finished initializing for the first time.
    # After the initial Index build, all future queries can be handled in parallel with potential
    # Index rebuilds.
    index_initialized = mongo_connection.is_initialized()

    if index_initialized:
        if request.method == 'POST':
            query_string = request.form['query']
            top_k = request.form['top-k']

            # Split query string into individual words using whitespace as the delimiter
            query_keywords = query_string.split()

            # Execute the query using the Search Engine's Query Handler
            query_results = query_handler.query(query_keywords, top_k)

            return render_template("index.html", submitted=True, webpages=query_results)
        else:
            return render_template("index.html", submitted=False)
    else:
        return render_template("loading.html")

if __name__ == "__main__":
    print("> Initializing Query Handler...")
    query_handler = QueryHandler(int(sys.argv[1]))

    print("> Launching Server...")
    flask.run(debug=True, use_reloader=False)
