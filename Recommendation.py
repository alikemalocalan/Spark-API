from pyspark.mllib.recommendation import ALS
from pyspark.sql import DataFrame

from InitSpark import InitSpark


class Recommendation:
    def __init__(self):
        self.init = InitSpark()
        self.spark = self.init.spark
        self.sc = self.init.sc

    def generateParquet(self) -> str:
        return self.init.generateParquet()

    def listRating(self) -> DataFrame:
        result = self.spark.sql("SELECT * FROM rating limit 50")
        return result

    def ratingbyUserID(self, userid):
        ratingsRDD = self.listRating().rdd \
            .map(lambda l: (int(l[3]), int(l[2]), float(l[1])))

        ratings = self.spark.createDataFrame(ratingsRDD)

        (training, test) = ratings.randomSplit([0.8, 0.2])

        model = ALS.train(training, rank=5, iterations=5)

        # Generate top 10 movie recommendations for each user
        # usermodel = model.recommendProducts(userid,10)
        # Generate top 10 user recommendations for each movie
        # movieRecs = model.recommendUsersForProducts(10)

        return self.writeToMOngo(model.recommendProducts(userid, 10), userid, 0)

    def writeToMOngo(self, recommendations: list, userid, typeid) -> str:
        rmd = []
        for rate in recommendations:
            rmd.append({"userid": userid, "product": rate.product, "type": typeid})
        df = self.spark.createDataFrame(rmd)
        df.show()
        df.write.format("com.mongodb.spark.sql.DefaultSource") \
            .mode("append").save()
        return "ok"


















        # ("SELECT sj.songID as sarkiId"
        #  ",s.songTitle as sarkiismi"
        #  ",s.artistName as sanatciIsmi"
        #  ",tj.id as genreId   " +
        #  "FROM song s" +
        #  "   INNER JOIN songjson sj ON" +
        #  "   sj.songID= s.songID" +
        #  "   INNER JOIN tag t ON" +
        #  "   s.trackID= t.trackID " +
        #  "   INNER JOIN tagjson tj ON" +
        #  "   t.tagID= tj.tagID"+
        #  " order by s.songTitle limit 20")

        # "SELECT songID, row_number() OVER ( ORDER BY songID) as sarkiId"
        # ",songTitle as sarkiismi"
        # ",trackID"
        # ",artistName as sanatciIsmi"
        # ",genreId as genreId   " +
        # "FROM song "

        # ("SELECT " +
        #  "s.trackID as trackID" +
        #  ",sj.id as ID" +
        #  ",sj.songID as songID" +
        #  ",s.songTitle as songTitle" +
        #  ",s.artistName as artistName" +
        #  ",tj.id as genreId   " +
        #  "FROM song s" +
        #  "   INNER JOIN songjson sj ON" +
        #  "   sj.songID= s.songID" +
        #  "   INNER JOIN tag t ON" +
        #  "   s.trackID= t.trackID " +
        #  "   INNER JOIN tagjson tj ON" +
        #  "   t.tagID= tj.tagID ")
