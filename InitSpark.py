from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

conf = (SparkConf()
        .set("spark.executor.cores", "2")
        .set("spark.executor.memory", "3g")
        .set("spark.eventLog.compress", "false")
        .setAppName("Spark-API")
        .setMaster("local[*]"))

# master="spark://alikemal-300E5C:7077"



class InitSpark:
    def __init__(self):
        self.sc = SparkContext(conf=conf)
        self.sc.setLogLevel("INFO")
        self.spark = SparkSession(self.sc) \
            .builder \
            .getOrCreate()

        self.base_txt = "dataset/"
        self.tag_txt = self.base_txt + "msd-MAGD-genreAssignment-new.cls"
        self.tracks_txt = self.base_txt + "unique_tracks.tsv"
        self.usersJson_txt = self.base_txt + "users.json"
        self.songJson_txt = self.base_txt + "songs.json"
        self.tagJson_txt = self.base_txt + "tags.json"
        self.ratingJson_txt = self.base_txt + "rating.json"

        self.tag_parqPATH = self.base_txt + "tagDF.parquet"
        self.taste_parqPATH = self.base_txt + "tasteDF.parquet"
        self.song_parqPATH = self.base_txt + "songDF.parquet"
        self.songNew_parqPATH = self.base_txt + "songNewDF.parquet"

        # userJson = self.spark.read.json(self.usersJson_txt).persist(StorageLevel.MEMORY_ONLY).cache() \
        #     .createOrReplaceTempView("userjson")
        # songJson = self.spark.read.json(self.songJson_txt).persist(StorageLevel.MEMORY_ONLY).cache() \
        #     .createOrReplaceTempView("songjson")
        # tagJson = self.spark.read.json(self.tagJson_txt).persist(StorageLevel.MEMORY_ONLY).cache() \
        #     .createOrReplaceTempView("tagjson")
        #
        # tagParq = self.spark.read.parquet(self.tag_parqPATH).persist(StorageLevel.MEMORY_ONLY).cache() \
        #     .createOrReplaceTempView("tag")
        # # tasteParq = self.spark.read.parquet(self.taste_parqPATH).persist(StorageLevel.MEMORY_ONLY).cache() \
        # #     .createOrReplaceTempView("tastestats")
        # songParq = self.spark.read.parquet(self.song_parqPATH).persist(StorageLevel.MEMORY_ONLY).cache() \
        #    .createOrReplaceTempView("song")
        # songNewParq = self.spark.read.parquet(self.songNew_parqPATH).persist(StorageLevel.MEMORY_ONLY)\
        #     .createOrReplaceTempView("song")
        # self.mongo = MongoAccess()

    def generateParquet(self):
        # track_line = sc.textFile(self.tracks_txt)
        # songs = track_line.map(lambda l: l.split(",")) \
        #     .map(lambda p: Row(trackID=p[0], songID=p[1], artistName=p[2], songTitle=p[3]))
        #
        # tag_line = sc.textFile(self.tag_txt)
        # tags = tag_line.map(lambda l: l.split(",")) \
        #     .map(lambda p: Row(trackID=p[0], tagID=p[1]))
        #
        # schemaSongs = self.spark.createDataFrame(songs)
        # schemaSongs.write.parquet(self.song_parqPATH)
        #
        # schemaTags = self.spark.createDataFrame(tags)
        # schemaTags.write.parquet(self.tag_parqPATH)

        # track_line = sc.textFile(self.base_txt + "userid-timestamp-artid-artname-traid-traname.tsv")
        # songs = track_line.map(lambda l: l.split("\t")) \
        #     .map(lambda p : Row(user=str(p[0]), timestamp=str(p[1]), art=str(p[2]), artname=str(p[3]), track=str(p[4]),
        #                         traname=str(p[5]))) \
        #     .take(1000)
        #
        # # Infer the schema, and register the DataFrame as a table.
        # schemaSongs = self.spark.createDataFrame(songs)
        # schemaSongs.filter(schemaSongs['art'].isNotNull).show()
        # schemaSongs.createOrReplaceTempView("song")
        # # self.mongo.writeToMOngo(schemaSongs)
        # schemaSongs.show()
        # self.schemaSongs.createOrReplaceTempView("train")
        return 200
