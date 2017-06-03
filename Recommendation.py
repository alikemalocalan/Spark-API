import numpy as np
from pyspark.mllib.recommendation import ALS
from pyspark.sql import DataFrame

from InitSpark import InitSpark


def parse(x):
    try:
        return int(x)
    except ValueError:
        return np.nan

class Recommendation:
    def __init__(self):
        self.init = InitSpark()
        self.spark = self.init.spark
        self.sc = self.init.sc

    def generateParquet(self) -> str:
        return self.init.generateParquet()

    def listRating(self) -> DataFrame:
        result = self.spark.sql("SELECT * FROM rating")
        return result

    def listPopulerSong(self) -> DataFrame:
        result = self.spark.sql("SELECT r.songid as product,0 as type FROM rating as r ORDER BY r.rating DESC LIMIT 20")
        result.show()
        result.write.format("com.mongodb.spark.sql.DefaultSource") \
            .mode("append").save()
        return "ok"

    def listPopulerGenre(self, userid) -> str:
        ratingsRDD = self.listRating().rdd \
            .map(lambda l: (int(l[3]), int(l[0]), float(l[1])))
        ratings = self.spark.createDataFrame(ratingsRDD)
        model = ALS.train(ratings, rank=5, iterations=5)
        recommend = model.recommendProducts(userid, 3)
        listRC = []
        for rate in recommend:
            listRC.append(int(rate[1]))
        df1 = self.spark.sql(
            "SELECT * FROM (SELECT r.genreID as genreID, r.songid as product ,2 as type,'%s' as userid,rank() OVER (PARTITION BY r.genreID ORDER BY r.rating DESC) as rank FROM rating as r) s WHERE rank < 4" % userid)

        result = df1.filter(
            (df1['genreID'] == listRC[0]) | (df1['genreID'] == listRC[1]) | (df1['genreID'] == listRC[2])).select(
            'product', 'type', 'userid')
        result.show()
        result.write.format("com.mongodb.spark.sql.DefaultSource") \
            .mode("append").save()
        return "ok"

    def ratingbyUserID(self, userid):
        ratingsRDD = self.listRating().rdd \
            .map(lambda l: (int(l[3]), int(l[2]), float(l[1])))
        ratings = self.spark.createDataFrame(ratingsRDD)

        model = ALS.train(ratings, rank=5, iterations=5)
        recommend = model.recommendProducts(userid, 10)
        for rate in recommend:
            print(rate)
        rmd = []
        for rate in recommend:
            rmd.append({"userid": userid, "product": rate.product, "type": 1})
        df = self.spark.createDataFrame(rmd)
        df.write.format("com.mongodb.spark.sql.DefaultSource") \
            .mode("append").save()
        return "ok"





        # "SELECT songID, row_number() OVER ( ORDER BY songID) as sarkiId"
        # ",songTitle as sarkiismi"
        # ",trackID"
        # ",artistName as sanatciIsmi"
        # ",genreId as genreId   " +
        # "FROM song "

        #  "   t.tagID= tj.tagID ")
