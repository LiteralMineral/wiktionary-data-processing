# other stuff
from functools import reduce

# website building tools

from flask import Flask, render_template
import settings
import psycopg2

# data processing stuff
import DataProcessing.Steps
import configparser

# pyspark stuff
from pyspark.sql import SparkSession
import pyspark.sql.functions as funcs

settings.init()


app = Flask(__name__)

@app.route('/')
def hello_world():
    return 'Hello World'
@app.route('/about')
def about():
    return render_template()

if __name__ == '__main__':
    app.run()



