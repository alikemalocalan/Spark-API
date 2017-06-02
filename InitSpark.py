from pyspark import SparkConf, SparkContext, StorageLevel, Row
from pyspark.sql import SparkSession

conf = (SparkConf()
        .set("spark.executor.cores", "2")
        .set("spark.executor.memory", "3g")
        .set("spark.eventLog.compress", "false")
        .setAppName("Spark-API")
        .setMaster("local[*]"))

# master="spark://alikemal-300E5C:7077"

# custom_schema = StructType({
#     StructField("userid", IntegerType(), False),
#     StructField("songid", IntegerType(), False),
#     StructField("rating", LongType(), False)})


class InitSpark:
    def __init__(self):
        self.sc = SparkContext(conf=conf)
        locale = self.sc._jvm.java.util.Locale
        locale.setDefault(locale.forLanguageTag("tr-TR"))
        self.sc.setLogLevel("INFO")
        self.spark = SparkSession(self.sc) \
            .builder \
            .config("spark.mongodb.input.uri",
                    "mongodb://alikemal:123456alikemal@alikemal.org:27017/admin.recommend?authMechanism=SCRAM-SHA-1") \
            .config("spark.mongodb.output.uri",
                    "mongodb://alikemal:123456alikemal@alikemal.org:27017/admin.recommend?authMechanism=SCRAM-SHA-1") \
            .config("packages", "org.mongodb.spark:mongo-spark-connector_2.11:2.0.0") \
            .getOrCreate()

        self.base_txt = "dataset/"
        self.tag_txt = self.base_txt + "msd-MAGD-genreAssignment-new.cls"
        self.tracks_txt = self.base_txt + "unique_tracks.tsv"
        self.usersJson_txt = self.base_txt + "users.json"
        self.songJson_txt = self.base_txt + "songs.json"
        self.tagJson_txt = self.base_txt + "tags.json"
        self.train_txt = self.base_txt + "train_triplets-new.tsv"

        self.ratingParq = self.base_txt + "rating.json"
        self.tag_parqPATH = self.base_txt + "tagDF.parquet"
        self.taste_parqPATH = self.base_txt + "tasteDF.parquet"
        self.song_parqPATH = self.base_txt + "songs.parquet"
        # self.songNew_parqPATH = self.base_txt + "songNewDF.parquet"

        # userJson = self.spark.read.json(self.usersJson_txt).persist(StorageLevel.MEMORY_ONLY).cache() \
        #     .createOrReplaceTempView("userjson")

        # tagJson = self.spark.read.json(self.tagJson_txt).persist(StorageLevel.MEMORY_ONLY).cache() \
        #     .createOrReplaceTempView("tagjson")
        #
        # tagParq = self.spark.read.parquet(self.tag_parqPATH).persist(StorageLevel.MEMORY_ONLY).cache() \
        #     .createOrReplaceTempView("tag")
        self.spark.read.load(self.ratingParq, format='json').persist(StorageLevel.MEMORY_ONLY).cache() \
            .createOrReplaceTempView("rating")
        self.spark.read.parquet(self.song_parqPATH).persist(StorageLevel.MEMORY_ONLY) \
            .createOrReplaceTempView("song")

    def generateParquet(self):
        # track_line = self.sc.textFile(self.train_txt)
        # songs = track_line.map(lambda l: l.split(",")) \
        #     .map(lambda p: Row(trackID=p[0], songID=p[1], artistName=p[2], songTitle=p[3]))
        #
        # tag_line = self.sc.textFile(self.tag_txt)
        # tags = tag_line.map(lambda l: l.split(",")) \
        #     .map(lambda p: Row(trackID=p[0], tagID=p[1]))
        #
        # schemaSongs = self.spark.createDataFrame(songs)
        # schemaSongs.write.parquet(self.song_parqPATH)
        track_line = self.sc.textFile(self.train_txt)
        songs = track_line.map(lambda l: l.split(",")) \
            .map(lambda p: Row(tuserID=p[0], songID=p[1], rating=float(p[2]))).take(1000000)

        schemaSongs = self.spark.createDataFrame(songs)
        schemaSongs.createOrReplaceTempView("train")

        return self.spark.sql("SELECT "
                              "row_number() OVER ( ORDER BY t.tuserID) as userid,s.sarkiId as songid,t.rating,s.genreID as genreID "
                              " FROM song as s "
                              "   INNER JOIN train as t ON "
                              "s.songID=t.songID").write.save(self.base_txt + "rating.json", format='json')

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

        # schemaSongs.createOrReplaceTempView("song")
        # # self.mongo.writeToMOngo(schemaSongs)
        # schemaSongs.show()
        # self.schemaSongs.createOrReplaceTempView("train")
