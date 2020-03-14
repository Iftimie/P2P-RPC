from flask import Flask, request
app = Flask(__name__)

@app.route("/", methods=["POST"])
def hello():
    print("gets hereeeee")

    # print(request.files)

    with open('posted.dat', 'wb') as f:
        data = request.stream.read(1024)
        while data:
            # print(data)
            f.write(data)
            data = request.stream.read(1024)
        return "Success!"


if __name__ == '__main__':
    app.run()