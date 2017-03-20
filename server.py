from flask import Flask
from flask import jsonify
from pyspark.sql import SparkSession

from recommendationEngine import RecommendationEngine

app = Flask(__name__)


@app.route('/')
def index():
    return jsonify(engine.songsbySingerFast())


@app.route('/people/<age>')
def people(age):
    return jsonify(engine.people(age))


@app.route('/songbysinger/<singer_name>')
def song(singer_name):
    return jsonify(engine.songsbySinger(singer_name))


def init_spark_context():
    spark = SparkSession \
        .builder \
        .appName("PythonWordCount") \
        .getOrCreate()
    return spark


if __name__ == '__main__':
    spark = init_spark_context()
    engine = RecommendationEngine(spark)
    app.run()
