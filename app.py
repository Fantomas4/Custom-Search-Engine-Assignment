from flask import Flask, render_template, request, redirect
app = Flask(__name__)


# Class used to represent the query results we want printed.
class Webpage:
    def __init__(self, title, url, similarity):
        self.title = title
        self.url = url
        self.similarity = similarity


@app.route("/", methods=['POST', 'GET'])
def index():
    if request.method == 'POST':
        query = request.form['query']
        #transform query
        #make the question to inverse catalog
        webpage = Webpage(title="This is a huge title", url="https://www.facebook.com", similarity=0.7)
        return render_template("index.html", webpages=[webpage])
    else:
        return render_template("index.html")


if __name__ == "__main__":
    app.run(debug=True)
