from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

conf = (SparkConf().set("total-executor-cores", "20").set("executor-memory", "5g"))
sc = SparkContext(master="local[*]", appName="Spark-API", conf=conf)


# master="spark://alikemal-300E5C:7077"

class InitSpark:
    def __init__(self):
        self.spark = SparkSession(sc) \
            .builder \
            .getOrCreate()

        self.base_txt = "dataset/"
        self.tag_txt = self.base_txt + "msd-MAGD-genreAssignment-new.cls"
        self.jamStat_txt = self.base_txt + "jam_to_msd-new.tsv"
        self.tracks_txt = self.base_txt + "unique_tracks.tsv"
        self.usersJson_txt = self.base_txt + "users.json"
        self.songJson_txt = self.base_txt + "songs.json"
        self.tagJson_txt = self.base_txt + "tags.json"
        self.ratingJson_txt = self.base_txt + "rating.json"

        self.tag_parqPATH = self.base_txt + "tagDF.parquet"
        self.taste_parqPATH = self.base_txt + "tasteDF.parquet"
        self.song_parqPATH = self.base_txt + "songDF.parquet"

        userJson = self.spark.read.json(self.usersJson_txt).persist().cache() \
            .createOrReplaceTempView("userjson")
        songJson = self.spark.read.json(self.songJson_txt).persist().cache() \
            .createOrReplaceTempView("songjson")
        tagJson = self.spark.read.json(self.tagJson_txt).persist().cache() \
            .createOrReplaceTempView("tagjson")
        ratingJson = self.spark.read.json(self.ratingJson_txt).persist().cache() \
            .createOrReplaceTempView("ratingjson")

        tagParq = self.spark.read.parquet(self.tag_parqPATH).persist().cache() \
            .createOrReplaceTempView("tag")
        tasteParq = self.spark.read.parquet(self.taste_parqPATH).persist().cache() \
            .createOrReplaceTempView("tastestats")
        songParq = self.spark.read.parquet(self.song_parqPATH).persist().cache() \
            .createOrReplaceTempView("song")

    def songsbySinger(self, singer_name):
        # result = self.schemaSongs \
        #     .filter(self.schemaSongs.artistName == singer_name) \
        #     .limit(20) \
        #     .collect()
        # #  Alternative
        result = self.spark.sql("SELECT * FROM song WHERE artistName LIKE '%s' LIMIT 20" % singer_name).collect()
        return result
