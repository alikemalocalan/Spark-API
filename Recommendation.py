from pyspark.sql import DataFrame

from InitSpark import InitSpark


class Recommendation:
    def __init__(self):
        self.init = InitSpark()
        self.spark = self.init.spark
        self.sc = self.init.sc
        # self.trainrating = self.spark.read.json(self.init.ratingJson_txt).rdd \
        #     .map(lambda l: Rating(int(l.userID), int(l.tagID), float(l.rating)))
        # self.model = ALS.train(self.trainrating, rank=5)

    # trackID=p[0], songID=p[1], artistName=p[2], songTitle=p[3]
    def songsbySongName(self, q) -> list:
        result = self.spark.sql("SELECT * FROM song WHERE songTitle LIKE '%s' LIMIT 20" % (q + '%'))
        return result.collect()

    def listSong(self) -> DataFrame:
        # return self.spark.sql(
        #     "SELECT songID, row_number() OVER ( ORDER BY songID) as id" +
        #     " FROM song group by songID ").collect()
        return self.spark.sql("SELECT ID as sarkiId"
                              ",songTitle as sarkiismi"
                              ",artistName as sanatciIsmi"
                              ",genreId as genreId   " +
                              "FROM song " +
                              " limit 20").cache()

    def ratingbyUserID(self, userid) -> list:
        # self.trainRating.filter(self.train.user == 1)
        recommendations = self.model.recommendProducts(userid, 10)
        for rating in recommendations:
            print(rating)
        return recommendations

    def getRecommend(self, userid) -> DataFrame:
        ratinglist = self.sc.parallelize(self.ratingbyUserID(userid)).toDF(('user', 'product', 'rating'))
        result = self.listSong().join(ratinglist, ratinglist.product == self.listSong().sarkiId, 'inner').select(
            self.listSong().sarkiID, self.listSong().sarkiismi)
        return result


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
