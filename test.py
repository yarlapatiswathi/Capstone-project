from flask import Flask

app = Flask(__name__)

@app.route('/')
def index():
    return "<h3>Hello world</h3>"

if __name__=="__main__":
    app.run(debug=True)