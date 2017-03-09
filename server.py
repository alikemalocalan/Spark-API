from flask import Flask
from flask import json
from pyspark import Row
from pyspark.sql import SparkSession

basePath = "/home/alikemal/oyunlar/bitirme/"
song_genre = basePath + "msd_tagtraum_cd2.cls"
artist = basePath + "unique_artists.txt"
tracks = basePath + "unique_tracks.txt"
user_stat = basePath + "train_triplets.txt"
peoplestxt = basePath + "people.txt"


# f = h5py.File('myfile.hdf5', 'r')

# spark-submit --master spark://alikemal-300E5C:7077 --total-executor-cores 14 --executor-memory 6g server.py

class RecommendationEngine:
    def __init__(self, spark, ):
        """Init the recommendation engine given a Spark context and a dataset path
        """
        self.spark = spark

    def songsbySinger(self, singer_name):
        sc = self.spark.sparkContext

        # Load a text file and convert each line to a Row.
        lines = sc.textFile(tracks)
        parts = lines.map(lambda l: l.split("<SEP>"))
        songs = lines.map(lambda p: Row(trackID=p[0], songID=(p[1]), artistName=(p[2]), songTitle=(p[3])))

        # Infer the schema, and register the DataFrame as a table.
        schemaSongs = spark.createDataFrame(songs)
        schemaSongs.createOrReplaceTempView("songs")

        # SQL can be run over DataFrames that have been registered as a table.
        teenagers = spark.sql("SELECT songTitle FROM songs LIMIT 20" % singer_name)

        # The results of SQL queries are Dataframe objects.
        # rdd returns the content as an :class:`pyspark.RDD` of :class:`Row`.
        teenNames = teenagers.rdd.map(lambda p: "Song: " + p.songTitle).collect()
        return json.dumps(teenNames)

    def people(self):
        sc = self.spark.sparkContext

        # Load a text file and convert each line to a Row.
        lines = sc.textFile(peoplestxt)
        parts = lines.map(lambda l: l.split(","))
        people = parts.map(lambda p: Row(name=p[0], age=int(p[1])))

        # Infer the schema, and register the DataFrame as a table.
        schemaPeople = spark.createDataFrame(people)
        schemaPeople.createOrReplaceTempView("people")

        # SQL can be run over DataFrames that have been registered as a table.
        teenagers = spark.sql("SELECT name FROM people")
        # The results of SQL queries are Dataframe objects.
        # rdd returns the content as an :class:`pyspark.RDD` of :class:`Row`.
        teenNames = teenagers.rdd.map(lambda p: "Name: " + p.name).collect()
        return json.dumps(teenNames)


app = Flask(__name__)


@app.route('/')
def hello_world():
    return 'Hello World!'


@app.route('/people')
def merhaba():
    return engine.people()


@app.route('/songbysinger/<singer_name>')
def song(singer_name):
    return engine.songsbySinger(singer_name)


def init_spark_context():
    spark = SparkSession \
        .builder \
        .appName("PythonWordCount") \
        .getOrCreate()
    return spark


if __name__ == '__main__':
    spark = init_spark_context()
    sc = spark.sparkContext

    engine = RecommendationEngine(spark)
    app.run()
