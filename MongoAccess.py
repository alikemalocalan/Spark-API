from pyspark.sql import DataFrame
from pyspark.sql import SparkSession


# client = MongoClient("mongodb://alikemal.org:27017/ali_bitirme")
#
# db = client['ali_bitirme']
#
# cursor = db['test'].find()
#
# for document in cursor:
#     print(document)
#
# db['test'].insert_one(Rating(1,1,1).__dict__)
class MongoAccess:
    def __init__(self):
        self.spark = SparkSession \
            .builder \
            .appName("MongoAccess") \
            .config("spark.mongodb.input.uri", "mongodb://alikemal.org:27017/ali_bitirme.test") \
            .config("spark.mongodb.output.uri", "mongodb://alikemal.org:27017/ali_bitirme.test") \
            .getOrCreate()

        # df = self.spark.read.format("com.mongodb.spark.sql.DefaultSource") \
        #     .option("spark.mongodb.input.uri", "mongodb://alikemal.org:27017/ali_bitirme.test") \
        #     .load()
        # df.printSchema()
        # self.spark.stop

    def writeToMOngo(self, df: DataFrame):
        df.write.format("com.mongodb.spark.sql.DefaultSource") \
            .option("spark.mongodb.input.uri", "mongodb://alikemal.org:27017/ali_bitirme.test") \
            .mode("append").save()
