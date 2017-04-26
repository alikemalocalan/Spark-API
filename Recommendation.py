from pyspark.mllib.recommendation import Rating, ALS


class Recommendation:
    def __init__(self, initSpark):
        self.spark = initSpark.spark
        rates = self.rating().rdd
        self.ratings = rates.map(lambda l: Rating(int(l[3]), int(l[2]), float(l[0])))
        # self.ratingRDD = rates.map(lambda p: Row(
        #    userID=int(p[3]), songID=int(p[1]), rating=int(p[0]), tagID=int(p[2])))

        # ratings = self.spark.createDataFrame(self.ratings)

    def songsbySinger(self, singer_name):
        # result = self.schemaSongs \
        #     .filter(self.schemaSongs.artistName == singer_name) \
        #     .limit(20) \
        #     .collect()
        # #  Alternative
        result = self.spark.sql("SELECT * FROM song WHERE artistName LIKE '%s' LIMIT 20" % singer_name).collect()
        return result

    def getSongs(self):
        return self.spark.sql("SELECT * FROM song limit 10").collect()

    def getSongbyTrackID(self, trackID):
        result = self.spark.sql("SELECT * FROM song limit 100 WHERE trackID like '%s'", trackID).collect()
        return result

    def liststatnumber(self):
        return self.spark.sql(
            "SELECT songID, row_number() OVER ( ORDER BY songID) as id" +
            " FROM song group by songID ")

    def rating(self):
        return self.spark.sql("SELECT * FROM ratingjson order by userID limit 200")

    def getRecommend(self):
        rank = 10
        iterations = 10
        blocks = -1

        model = ALS.trainImplicit(self.ratings, rank, iterations)
        result = self.spark.createDataFrame(model.productFeatures().persist().cache()).collect()
        return result
