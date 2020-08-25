from flask import Flask, render_template, request, redirect
from search_engine import SearchEngine
flask = Flask(__name__)


@flask.route("/", methods=['POST', 'GET'])
def index():
    if request.method == 'POST':
        query = request.form['query']
        top_k = request.form['top-k']
        query_results = search_engine.query_handler.query(query, top_k)

        #transform query
        #make the question to inverse catalog.
        # webpage = {"title": "This is a huge title", "url": "https://www.facebook.com"}
        return render_template("index.html", webpages=query_results)
    else:
        return render_template("index.html")


if __name__ == "__main__":
    search_engine = SearchEngine()

    flask.run(debug=True, use_reloader=False)
