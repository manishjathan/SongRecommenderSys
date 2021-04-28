from flask import Flask
app = Flask(__name__)

@app.route('/<int:number>/')
def incrementer(number):
    return "Incremented number is " + str(number+1)
@app.route('/<string:name>/')
def hello(name):
    return "Hello " + name
    
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=105)
