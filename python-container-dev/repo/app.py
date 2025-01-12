# app.py docker run -it --rm -v $(pwd):/app python-container-dev_python-dev /bin/bash
#docker run -it --rm -v $(pwd):/app -v /Users/veer/.aws:/root/.aws python-container-dev_python-dev /bin/bash

from flask import Flask

app = Flask(__name__)

@app.route("/")
def home():
    return "Hello from Flask in Docker!"

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
