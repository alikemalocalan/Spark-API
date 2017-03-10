from flask import json
from pyspark import Row


class RecommendationEngine:
    def __init__(self, spark):
        """Init the recommendation engine given a Spark context and a dataset path
        """
        self.spark = spark

    def songsbySinger(self, singer_name, tracks):
        sc = self.spark.sparkContext

        # Load a text file and convert each line to a Row.
        lines = sc.textFile(tracks)
        parts = lines.map(lambda l: l.split(","))
        songs = lines.map(lambda p: Row(trackID=p, songID=p, artistName=p, songTitle=p))
        schemaSongs = self.spark.createDataFrame(songs)
        schemaSongs.createOrReplaceTempView("song")

        # result = schemaSongs \
        #     .filter("artistName LIKE 'A%' ") \
        #     .select("*") \
        #     .limit(20) \
        #     .collect()
        result = self.spark.sql("SELECT * FROM song WHERE artistName LIKE '%' LIMIT 20").collect()
        return json.dumps(result)

    def people(self, peoplestxt):
        sc = self.spark.sparkContext

        # Load a text file and convert each line to a Row.
        lines = sc.textFile(peoplestxt)
        parts = lines.map(lambda l: l.split(","))
        people = parts.map(lambda p: Row(name=p[0], age=int(p[1])))

        # Infer the schema, and register the DataFrame as a table.
        schemaPeople = self.spark.createDataFrame(people)
        schemaPeople.createOrReplaceTempView("people")

        # SQL can be run over DataFrames that have been registered as a table.
        teenagers = self.spark.sql("SELECT * FROM people WHERE name LIKE 'Michael'")
        # The results of SQL queries are Dataframe objects.
        # rdd returns the content as an :class:`pyspark.RDD` of :class:`Row`.
        teenNames = teenagers.rdd.map(lambda p: "Name: " + p.name).collect()
        return json.dumps(teenNames)
