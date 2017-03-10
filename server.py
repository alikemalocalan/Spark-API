from flask import Flask
from pyspark.sql import SparkSession

from recommendationEngine import RecommendationEngine

basePath = "/home/alikemal/oyunlar/bitirme/"
song_genre_txt = basePath + "msd_tagtraum_cd2.cls"
artist_txt = basePath + "unique_artists.txt"
tracks_txt = basePath + "unique_tracks.txt"
user_stat_txt = basePath + "train_triplets.txt"
peoples_txt = basePath + "people.txt"

app = Flask(__name__)


@app.route('/')
def hello_world():
    return 'Hello World!'


@app.route('/people')
def merhaba():
    return engine.people(peoples_txt)


@app.route('/songbysinger/<singer_name>')
def song(singer_name):
    return engine.songsbySinger(singer_name, tracks_txt)


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
