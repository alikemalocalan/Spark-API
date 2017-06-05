import json

from pyspark.mllib.recommendation import ALS

from InitSpark import InitSpark


class Recommendation:
    def __init__(self):
        self.init = InitSpark()
        self.spark = self.init.spark
        self.sc = self.init.sc

    def generateParquet(self) -> str:
        return self.init.generateParquet()

    def listPopulerSong(self) -> str:
        result = self.spark.sql("SELECT r.songid as product,0 as type FROM rating as r ORDER BY r.rating DESC LIMIT 20")

        return result.toJSON().collect().__str__()

    def listPopulerGenre(self, userid) -> str:
        ratingsRDD = self.spark.sql("SELECT * FROM rating").rdd \
            .map(lambda l: (int(l[3]), int(l[0]), float(l[1])))
        ratings = self.spark.createDataFrame(ratingsRDD)
        model = ALS.train(ratings, rank=5, iterations=5)
        recommend = model.recommendProducts(userid, 3)
        listRC = []
        for rate in recommend:
            listRC.append(int(rate[1]))
        df1 = self.spark.sql(
            "SELECT * FROM (SELECT r.genreID as genreID, r.songid as product ,2 as type,{0} as userid,rank() OVER (PARTITION BY r.genreID ORDER BY r.rating DESC) as rank FROM rating as r) s WHERE rank < 4".format(
                userid))

        result = df1.filter(
            (df1['genreID'] == listRC[0]) | (df1['genreID'] == listRC[1]) | (df1['genreID'] == listRC[2])).select(
            'product', 'type', 'userid')
        result.write.format("com.mongodb.spark.sql.DefaultSource") \
            .mode("append").save()
        return "ok"

    def ratingbyUserID(self, userid):
        ratingsRDD = self.spark.sql("SELECT * FROM rating").rdd \
            .map(lambda l: (int(l[3]), int(l[2]), float(l[1])))
        ratings = self.spark.createDataFrame(ratingsRDD)

        model = ALS.train(ratings, rank=5, iterations=5)
        recommend = model.recommendProducts(userid, 10)
        rmd = []
        for rate in recommend:
            rmd.append({"userid": userid, "product": rate.product, "type": 1})
        self.spark.createDataFrame(rmd).write.format("com.mongodb.spark.sql.DefaultSource") \
            .mode("append").save()
        return "ok"


        # "SELECT songID, row_number() OVER ( ORDER BY songID) as sarkiId"
        # ",songTitle as sarkiismi"
        # ",trackID"
        # ",artistName as sanatciIsmi"
        # ",genreId as genreId   " +
        # "FROM song "
