import sys

from flask import Flask, render_template, request, redirect
from query_handler import QueryHandler

flask = Flask(__name__)


@flask.route("/", methods=['POST', 'GET'])
def index():
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


if __name__ == "__main__":
    print("> Initializing Query Handler...")
    query_handler = QueryHandler(int(sys.argv[1]))

    print("> Launching Server...")
    flask.run(debug=True, use_reloader=False)
