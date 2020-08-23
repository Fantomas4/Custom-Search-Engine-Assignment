from flask import Flask, render_template, request, redirect
app = Flask(__name__)


@app.route("/", methods=['POST', 'GET'])
def index():
    if request.method == 'POST':
        query = request.form['query']
        #transform query
        #make the question to inverse catalog.
        webpage = {"title": "This is a huge title", "url": "https://www.facebook.com"}
        return render_template("index.html", webpages=[webpage])
    else:
        return render_template("index.html")


if __name__ == "__main__":
    app.run(debug=True)
