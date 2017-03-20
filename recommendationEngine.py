from pyspark import Row
from pyspark import StorageLevel

basePath = "/home/alikemal/oyunlar/bitirme/"
song_genre_txt = basePath + "msd_tagtraum_cd2.cls"

tracks_txt = "dataset/unique_tracks.txt"
peoples_txt = "dataset/people.txt"


class RecommendationEngine:
    def __init__(self, spark):
        """Init the recommendation engine given a Spark context and a dataset path
        """

        self.spark = spark
        sc = self.spark.sparkContext

        # Song table create
        track_line = sc.textFile(tracks_txt)
        songs = track_line.map(lambda l: l.split(",")) \
            .map(lambda p: Row(trackID=p[0], songID=p[1], artistName=p[2], songTitle=p[3])) \
            .persist(StorageLevel.MEMORY_ONLY) \
            .cache()

        # Infer the schema, and register the DataFrame as a table.
        self.schemaSongs = self.spark.createDataFrame(songs)
        self.schemaSongs.createOrReplaceTempView("song")

        # People table create
        people_lines = sc.textFile(peoples_txt)
        people_parts = people_lines.map(lambda l: l.split(","))
        people_parts.cache()
        people = people_parts.map(lambda p: Row(name=p[0], age=int(p[1])))

        # Infer the schema, and register the DataFrame as a table.
        schemaPeople = self.spark.createDataFrame(people)
        schemaPeople.createOrReplaceTempView("people")

    def songsbySinger(self, singer_name):
        result = self.schemaSongs \
            .filter(self.schemaSongs.artistName == singer_name) \
            .limit(20) \
            .collect()
        #  Alternative
        # result = self.spark.sql("SELECT * FROM song WHERE artistName LIKE '%s' LIMIT 20" % singer_name).collect()
        return result

    def people(self, age):
        # SQL can be run over DataFrames that have been registered as a table.
        result = self.spark.sql("SELECT * FROM people WHERE age LIKE %s" % int(age)).collect()
        return result